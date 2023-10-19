import time
from dataclasses import dataclass
from enum import Enum

from r3dis.commands.context import ClientContext
from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.handlers import CommandHandler
from r3dis.commands.parameters import (
    redis_keyword_parameter,
    redis_positional_parameter,
)
from r3dis.commands.router import RedisCommandsRouter
from r3dis.consts import Commands
from r3dis.resp import RESP_OK, RespError

client_commands_router = RedisCommandsRouter()

client_sub_commands_router = client_commands_router.child(Commands.Client)


@dataclass
class ClientCommand(Command):
    client_context: ClientContext = redis_command_dependency()

    def execute(self):
        raise NotImplementedError()


@client_commands_router.command(Commands.Select)
class SelectDatabase(ClientCommand):
    index: int = redis_positional_parameter()

    def execute(self):
        self.client_context.current_database = self.index
        return RESP_OK


@client_commands_router.command(Commands.ClientList)
class ClientList(CommandHandler):
    client_type: bytes | None = redis_keyword_parameter(flag=b"TYPE", default=None)

    def execute(self):
        if self.client_type:
            return self.clients.filter_(client_type=self.client_type).info
        return self.clients.info


@client_sub_commands_router.command(Commands.ClientId)
class ClientId(ClientCommand):
    def execute(self):
        return self.client_context.current_client.client_id


@client_sub_commands_router.command(Commands.ClientSetName)
class ClientSetName(ClientCommand):
    name: bytes = redis_positional_parameter()

    def execute(self):
        self.client_context.current_client.name = self.name
        return RESP_OK


@client_sub_commands_router.command(Commands.ClientGetName)
class ClientGetName(ClientCommand):
    def execute(self):
        return self.client_context.current_client.name or None


@client_sub_commands_router.command(Commands.ClientKill)
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


@client_sub_commands_router.command(Commands.ClientPause)
class ClientPause(ClientCommand):
    timeout_seconds: int = redis_positional_parameter()

    def handle(self):
        self.client_context.server_context.pause_timeout = time.time() + self.timeout_seconds
        self.client_context.server_context.is_paused = True
        return RESP_OK


@client_sub_commands_router.command(Commands.ClientUnpause)
class ClientUnpause(ClientCommand):
    def handle(self, timeout_seconds: int):
        self.client_context.server_context.is_paused = False
        return RESP_OK


class ReplyMode(Enum):
    ON = b"ON"
    OFF = b"OFF"
    SKIP = b"SKIP"


@client_sub_commands_router.command(Commands.ClientReply)
class ClientReply(ClientCommand):
    mode: ReplyMode = redis_positional_parameter()

    def execute(self):
        if self.mode == ReplyMode.ON:
            return RESP_OK


@client_sub_commands_router.command(Commands.ClientSetInformation)
class ClientSetInformation(ClientCommand):
    library_name: bytes | None = redis_keyword_parameter(flag=b"LIB-NAME", default=None)
    library_version: bytes | None = redis_keyword_parameter(flag=b"LIB-VER", default=None)

    def execute(self):
        if self.library_name:
            self.client_context.current_client.library_name = self.library_name
        if self.library_version:
            self.client_context.current_client.library_version = self.library_version
