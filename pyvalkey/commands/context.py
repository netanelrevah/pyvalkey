import itertools
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Self

from pyvalkey.commands.core import Command
from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.database_objects.clients import Client, ClientList
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.information import Information


@dataclass
class ServerContext:
    databases: defaultdict[int, Database] = field(default_factory=lambda: defaultdict(Database, {0: Database()}))
    acl: ACL = field(default_factory=ACL.create)
    client_ids: Iterable[int] = field(default_factory=lambda: itertools.count(0))
    clients: ClientList = field(default_factory=ClientList)
    configurations: Configurations = field(default_factory=Configurations)
    information: Information = field(default_factory=Information)

    is_paused: bool = False
    pause_timeout: float = 0


@dataclass
class ClientContext:
    server_context: ServerContext
    current_client: Client

    current_database: int = 0
    current_user: ACLUser | None = None

    transaction_commands: list[Command] | None = None

    @property
    def database(self) -> Database:
        return self.server_context.databases[self.current_database]

    @classmethod
    def create(cls, server_context: ServerContext, host: bytes, port: int) -> Self:
        return cls(server_context, server_context.clients.create_client(host, port))
