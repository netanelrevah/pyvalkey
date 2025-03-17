import time
from enum import Enum
from hashlib import sha256

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import (
    keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, RespError, RespProtocolVersion, ValueType


@ServerCommandsRouter.command(b"auth", [b"fast", b"connection"])
class Authorize(Command):
    acl: ACL = server_command_dependency()
    configurations: Configurations = server_command_dependency()
    client_context: ClientContext = server_command_dependency()

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


@ServerCommandsRouter.command(b"list", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientList(Command):
    client_context: ClientContext = server_command_dependency()
    client_type: bytes | None = keyword_parameter(flag=b"TYPE", default=None)

    def execute(self) -> ValueType:
        if self.client_type:
            return self.client_context.server_context.clients.filter_(client_type=self.client_type).info
        return self.client_context.server_context.clients.info


@ServerCommandsRouter.command(b"id", [b"slow", b"connection"], b"client")
class ClientId(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.client_id


@ServerCommandsRouter.command(b"setname", [b"slow", b"connection"], b"client")
class ClientSetName(Command):
    client_context: ClientContext = server_command_dependency()
    name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_client.name = self.name
        return RESP_OK


@ServerCommandsRouter.command(b"getname", [b"slow", b"connection"], b"client")
class ClientGetName(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.name or None


@ServerCommandsRouter.command(b"kill", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientKill(Command):
    client_context: ClientContext = server_command_dependency()
    old_format_address: bytes | None = positional_parameter(default=None)
    client_id: int = keyword_parameter(flag=b"ID", default=None)
    address: bytes = keyword_parameter(flag=b"ADDR", default=None)

    def execute(self) -> ValueType:
        if self.old_format_address:
            clients = self.client_context.server_context.clients.filter_(address=self.old_format_address).values()
            if not clients:
                return RespError(b"ERR No such client")
            (client,) = clients
            client.is_killed = True
            return RESP_OK

        clients = self.client_context.server_context.clients.filter_(
            client_id=self.client_id, address=self.address
        ).values()
        for client in clients:
            client.is_killed = True
        return len(clients)


@ServerCommandsRouter.command(b"pause", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientPause(Command):
    client_context: ClientContext = server_command_dependency()
    timeout_seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.server_context.pause_timeout = time.time() + self.timeout_seconds
        self.client_context.server_context.is_paused = True
        return RESP_OK


@ServerCommandsRouter.command(b"unpause", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientUnpause(Command):
    client_context: ClientContext = server_command_dependency()
    timeout_seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.server_context.is_paused = False
        return RESP_OK


class ReplyMode(Enum):
    ON = b"ON"
    OFF = b"OFF"
    SKIP = b"SKIP"


@ServerCommandsRouter.command(b"reply", [b"slow", b"connection"], b"client")
class ClientReply(Command):
    client_context: ClientContext = server_command_dependency()
    mode: ReplyMode = positional_parameter()

    def execute(self) -> ValueType:
        if self.mode == ReplyMode.ON:
            return RESP_OK
        return None


@ServerCommandsRouter.command(b"setinfo", [b"slow", b"connection"], b"client")
class ClientSetInformation(Command):
    client_context: ClientContext = server_command_dependency()
    library_name: bytes | None = keyword_parameter(flag=b"LIB-NAME", default=None)
    library_version: bytes | None = keyword_parameter(flag=b"LIB-VER", default=None)

    def execute(self) -> ValueType:
        if self.library_name:
            self.client_context.current_client.library_name = self.library_name
        if self.library_version:
            self.client_context.current_client.library_version = self.library_version
        return RESP_OK


@ServerCommandsRouter.command(b"echo", [b"fast", b"connection"])
class Echo(Command):
    message: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.message


@ServerCommandsRouter.command(b"hello", [b"connection", b"fast"])
class Hello(Command):
    client_context: ClientContext = server_command_dependency()
    protocol_version: RespProtocolVersion | None = positional_parameter(
        default=None, parse_error=b"NOPROTO unsupported protocol version"
    )

    def execute(self) -> ValueType:
        if self.protocol_version is not None:
            self.client_context.protocol = self.protocol_version

        response = {
            b"server": b"redis",
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


@ServerCommandsRouter.command(b"ping", [b"fast", b"connection"])
class Ping(Command):
    message: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.message:
            return self.message
        return b"PONG"


@ServerCommandsRouter.command(b"select", [b"connection", b"fast"])
class SelectDatabase(Command):
    client_context: ClientContext = server_command_dependency()
    index: int = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_database = self.index
        return RESP_OK
