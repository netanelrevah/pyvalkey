import time
from dataclasses import dataclass
from enum import Enum

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import (
    server_keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import RESP_OK, RespError


@dataclass
class ClientCommand(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self):
        raise NotImplementedError()


@ServerCommandsRouter.command(b"select", [b"connection", b"fast"])
class SelectDatabase(ClientCommand):
    index: int = positional_parameter()

    def execute(self):
        self.client_context.current_database = self.index
        return RESP_OK


@ServerCommandsRouter.command(b"list", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientList(ClientCommand):
    client_type: bytes | None = server_keyword_parameter(flag=b"TYPE", default=None)

    def execute(self):
        if self.client_type:
            return self.client_context.server_context.clients.filter_(client_type=self.client_type).info
        return self.client_context.server_context.clients.info


@ServerCommandsRouter.command(b"id", [b"slow", b"connection"], b"client")
class ClientId(ClientCommand):
    def execute(self):
        return self.client_context.current_client.client_id


@ServerCommandsRouter.command(b"setname", [b"slow", b"connection"], b"client")
class ClientSetName(ClientCommand):
    name: bytes = positional_parameter()

    def execute(self):
        self.client_context.current_client.name = self.name
        return RESP_OK


@ServerCommandsRouter.command(b"getname", [b"slow", b"connection"], b"client")
class ClientGetName(ClientCommand):
    def execute(self):
        return self.client_context.current_client.name or None


@ServerCommandsRouter.command(b"kill", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientKill(ClientCommand):
    old_format_address: bytes | None = positional_parameter(default=None)
    client_id: int = server_keyword_parameter(flag=b"ID", default=None)
    address: bytes = server_keyword_parameter(flag=b"ADDR", default=None)

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


@ServerCommandsRouter.command(b"pause", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientPause(ClientCommand):
    timeout_seconds: int = positional_parameter()

    def execute(self):
        self.client_context.server_context.pause_timeout = time.time() + self.timeout_seconds
        self.client_context.server_context.is_paused = True
        return RESP_OK


@ServerCommandsRouter.command(b"unpause", [b"admin", b"slow", b"dangerous", b"connection"], b"client")
class ClientUnpause(ClientCommand):
    timeout_seconds: int = positional_parameter()

    def execute(self):
        self.client_context.server_context.is_paused = False
        return RESP_OK


class ReplyMode(Enum):
    ON = b"ON"
    OFF = b"OFF"
    SKIP = b"SKIP"


@ServerCommandsRouter.command(b"reply", [b"slow", b"connection"], b"client")
class ClientReply(ClientCommand):
    mode: ReplyMode = positional_parameter()

    def execute(self):
        if self.mode == ReplyMode.ON:
            return RESP_OK


@ServerCommandsRouter.command(b"setinfo", [b"slow", b"connection"], b"client")
class ClientSetInformation(ClientCommand):
    library_name: bytes | None = server_keyword_parameter(flag=b"LIB-NAME", default=None)
    library_version: bytes | None = server_keyword_parameter(flag=b"LIB-VER", default=None)

    def execute(self):
        if self.library_name:
            self.client_context.current_client.library_name = self.library_name
        if self.library_version:
            self.client_context.current_client.library_version = self.library_version
        return RESP_OK
