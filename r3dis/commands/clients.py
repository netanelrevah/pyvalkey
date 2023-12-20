import time
from dataclasses import dataclass
from enum import Enum

from r3dis.commands.context import ClientContext
from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import (
    redis_keyword_parameter,
    redis_positional_parameter,
)
from r3dis.commands.router import RedisCommandsRouter
from r3dis.resp import RESP_OK, RespError


@dataclass
class ClientCommand(Command):
    client_context: ClientContext = redis_command_dependency()

    def execute(self):
        raise NotImplementedError()


@RedisCommandsRouter.command(b"select", [b"connection", b"fast"])
class SelectDatabase(ClientCommand):
    index: int = redis_positional_parameter()

    def execute(self):
        self.client_context.current_database = self.index
        return RESP_OK


@RedisCommandsRouter.command(b"list", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientList(ClientCommand):
    client_type: bytes | None = redis_keyword_parameter(flag=b"TYPE", default=None)

    def execute(self):
        if self.client_type:
            return self.client_context.server_context.clients.filter_(client_type=self.client_type).info
        return self.client_context.server_context.clients.info


@RedisCommandsRouter.command(b"id", [b"slow", b"connection"], b"client")
class ClientId(ClientCommand):
    def execute(self):
        return self.client_context.current_client.client_id


@RedisCommandsRouter.command(b"setname", [b"slow", b"connection"], b"client")
class ClientSetName(ClientCommand):
    name: bytes = redis_positional_parameter()

    def execute(self):
        self.client_context.current_client.name = self.name
        return RESP_OK


@RedisCommandsRouter.command(b"getname", [b"slow", b"connection"], b"client")
class ClientGetName(ClientCommand):
    def execute(self):
        return self.client_context.current_client.name or None


@RedisCommandsRouter.command(b"kill", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientKill(ClientCommand):
    old_format_address: bytes | None = redis_positional_parameter(default=None)
    client_id: int = redis_keyword_parameter(flag=b"ID", default=None)
    address: bytes = redis_keyword_parameter(flag=b"ADDR", default=None)

    def execute(self):
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


@RedisCommandsRouter.command(b"pause", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientPause(ClientCommand):
    timeout_seconds: int = redis_positional_parameter()

    def execute(self):
        self.client_context.server_context.pause_timeout = time.time() + self.timeout_seconds
        self.client_context.server_context.is_paused = True
        return RESP_OK


@RedisCommandsRouter.command(b"unpause", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientUnpause(ClientCommand):
    timeout_seconds: int = redis_positional_parameter()

    def execute(self):
        self.client_context.server_context.is_paused = False
        return RESP_OK


class ReplyMode(Enum):
    ON = b"ON"
    OFF = b"OFF"
    SKIP = b"SKIP"


@RedisCommandsRouter.command(b"reply", [b"slow", b"connection"], b"client")
class ClientReply(ClientCommand):
    mode: ReplyMode = redis_positional_parameter()

    def execute(self):
        if self.mode == ReplyMode.ON:
            return RESP_OK


@RedisCommandsRouter.command(b"setinfo", [b"slow", b"connection"], b"client")
class ClientSetInformation(ClientCommand):
    library_name: bytes | None = redis_keyword_parameter(flag=b"LIB-NAME", default=None)
    library_version: bytes | None = redis_keyword_parameter(flag=b"LIB-VER", default=None)

    def execute(self):
        if self.library_name:
            self.client_context.current_client.library_name = self.library_name
        if self.library_version:
            self.client_context.current_client.library_version = self.library_version
        return RESP_OK
