from collections import defaultdict
from dataclasses import dataclass

from r3dis.acl import ACL
from r3dis.clients import Client, ClientList
from r3dis.databases import Database


@dataclass
class ServerContext:
    databases: defaultdict[int, Database]
    acl: ACL
    clients: ClientList

    is_paused: bool = False
    pause_timeout: float = 0


@dataclass
class ClientContext:
    server_context: ServerContext
    current_client: Client

    current_database: int = 0
    current_user: bytes = None

    @property
    def database(self):
        return self.server_context.databases[self.current_database]


@dataclass
class CommandHandler:
    command_context: ClientContext

    def execute(self, parameters: list[bytes]):
        parsed_parameters = self.parse(parameters)
        return self.handle(*parsed_parameters)

    def handle(self, *args):
        raise NotImplementedError()

    def parse(self, parameters: list[bytes]):
        raise NotImplementedError()

    @property
    def database(self):
        return self.command_context.database

    @property
    def acl(self):
        return self.command_context.server_context.acl

    @property
    def clients(self):
        return self.command_context.server_context.clients

    @property
    def current_client(self):
        return self.command_context.current_client
