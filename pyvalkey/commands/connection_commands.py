import time
from dataclasses import field
from enum import Enum
from hashlib import sha256

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import (
    keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.router import command
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.enums import UnblockMessage
from pyvalkey.resp import RESP_OK, RespError, RespProtocolVersion, ValueType


@command(b"auth", {b"fast", b"connection"})
class Authorize(Command):
    acl: ACL = dependency()
    configurations: Configurations = dependency()
    client_context: ClientContext = dependency()

    username: bytes | None = positional_parameter(default=None)
    password: bytes = positional_parameter()

    def execute(self) -> ValueType:
        password_hash = sha256(self.password).hexdigest().encode()
        if self.username is not None:
            if self.username not in self.acl:
                raise ServerError(b"WRONGPASS invalid username-password pair or user is disabled.")
            if self.username == b"default" and password_hash == self.configurations.requirepass:
                return RESP_OK
            acl_user = self.acl[self.username]
            if not acl_user.is_no_password_user and password_hash not in acl_user.passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            self.client_context.current_user = acl_user
            return RESP_OK

        if self.configurations.requirepass and password_hash == self.configurations.requirepass:
            return RESP_OK
        raise ServerError(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )


@command(b"list", {b"admin", b"slow", b"dangerous", b"connection"}, b"client")
class ClientList(Command):
    client_context: ClientContext = dependency()
    client_type: bytes | None = keyword_parameter(flag=b"TYPE", default=None)

    def execute(self) -> ValueType:
        if self.client_type:
            return self.client_context.server_context.clients.filter_(client_type=self.client_type).info
        return self.client_context.server_context.clients.info


@command(b"id", {b"slow", b"connection"}, b"client")
class ClientId(Command):
    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.client_id


@command(b"setname", {b"slow", b"connection"}, b"client")
class ClientSetName(Command):
    client_context: ClientContext = dependency()
    name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_client.name = self.name
        return RESP_OK


@command(b"getname", {b"slow", b"connection"}, b"client")
class ClientGetName(Command):
    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.name or None


@command(b"kill", {b"admin", b"slow", b"dangerous", b"connection"}, b"client")
class ClientKill(Command):
    server_context: ServerContext = dependency()
    old_format_address: bytes | None = positional_parameter(default=None)
    client_id: int = keyword_parameter(flag=b"ID", default=None)
    address: bytes = keyword_parameter(flag=b"ADDR", default=None)

    def execute(self) -> ValueType:
        if self.old_format_address:
            clients = self.server_context.clients.filter_(address=self.old_format_address).values()
            if not clients:
                return RespError(b"ERR No such client")
            (client,) = clients
            client.is_killed = True
            return RESP_OK

        clients = self.server_context.clients.filter_(client_id=self.client_id, address=self.address).values()
        for client in clients:
            client.is_killed = True
        return len(clients)


@command(b"pause", {b"admin", b"slow", b"dangerous", b"connection"}, b"client")
class ClientPause(Command):
    server_context: ServerContext = dependency()
    timeout_seconds: int = positional_parameter()
    pause_all: bool = keyword_parameter(flag=b"ALL", default=False)
    pause_write: bool = keyword_parameter(flag=b"WRITE", default=False)

    def execute(self) -> ValueType:
        if self.pause_write and self.pause_all:
            raise ServerError(b"ERR Syntax error")
        elif not self.pause_write:
            self.pause_all = True

        self.server_context.pause_timeout = time.time() + self.timeout_seconds
        if self.pause_write:
            self.server_context.is_paused_for_write = True
        else:
            self.server_context.is_paused = True
        return RESP_OK


@command(b"unpause", {b"admin", b"slow", b"dangerous", b"connection"}, b"client")
class ClientUnpause(Command):
    server_context: ServerContext = dependency()
    timeout_seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        self.server_context.is_paused = False
        self.server_context.is_paused_for_write = False
        return RESP_OK


class ReplyMode(Enum):
    ON = b"ON"
    OFF = b"OFF"
    SKIP = b"SKIP"


@command(b"reply", {b"slow", b"connection"}, b"client")
class ClientReply(Command):
    mode: ReplyMode = positional_parameter()

    def execute(self) -> ValueType:
        if self.mode == ReplyMode.ON:
            return RESP_OK
        return None


class UnblockOption(Enum):
    timeout = b"TIMEOUT"
    error = b"ERROR"


@command(b"unblock", {b"slow", b"connection"}, b"client")
class ClientUnblock(Command):
    server_context: ServerContext = dependency()

    client_id: int = positional_parameter()
    unblock_option: UnblockOption = positional_parameter(default=UnblockOption.timeout)

    _unblocked: int = field(init=False, default=0)

    async def before(self, in_multi: bool = False) -> None:
        if self.client_id not in self.server_context.clients:
            return

        client = self.server_context.clients[self.client_id]
        if client.blocking_context is None:
            return

        await client.blocking_context.queue.put(
            UnblockMessage.ERROR if self.unblock_option == UnblockOption.error else UnblockMessage.TIMEOUT
        )

        self._unblocked = 1

    def execute(self) -> ValueType:
        return self._unblocked


@command(b"setinfo", {b"slow", b"connection"}, b"client")
class ClientSetInformation(Command):
    client_context: ClientContext = dependency()
    library_name: bytes | None = keyword_parameter(flag=b"LIB-NAME", default=None)
    library_version: bytes | None = keyword_parameter(flag=b"LIB-VER", default=None)

    def execute(self) -> ValueType:
        if self.library_name:
            self.client_context.current_client.library_name = self.library_name
        if self.library_version:
            self.client_context.current_client.library_version = self.library_version
        return RESP_OK


@command(b"echo", {b"fast", b"connection"})
class Echo(Command):
    message: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.message


@command(b"hello", {b"connection", b"fast"})
class Hello(Command):
    client_context: ClientContext = dependency()
    protocol_version: RespProtocolVersion | None = positional_parameter(
        default=None, parse_error=b"NOPROTO unsupported protocol version"
    )

    def execute(self) -> ValueType:
        if self.protocol_version is not None:
            self.client_context.protocol = self.protocol_version

        response = {
            b"server": b"valkey",
            b"version": self.client_context.server_context.information.server_version,
            b"proto": self.client_context.protocol,
            b"id": self.client_context.current_client.client_id,
            b"mode": b"standalone",
            b"role": b"master",
            b"modules": [],
        }

        if self.client_context.server_context.configurations.availability_zone != b"":
            response[b"availability_zone"] = self.client_context.server_context.configurations.availability_zone

        return response


@command(b"ping", {b"fast", b"connection"})
class Ping(Command):
    message: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.message:
            return self.message
        return b"PONG"


@command(b"select", {b"connection", b"fast"})
class SelectDatabase(Command):
    client_context: ClientContext = dependency()
    index: int = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_database = self.index
        return RESP_OK
