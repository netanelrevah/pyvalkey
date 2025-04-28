from __future__ import annotations

import itertools
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.database_objects.clients import Client, ClientList
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import BlockingManager, ClientWatchlist, Database
from pyvalkey.database_objects.information import Information
from pyvalkey.resp import RespProtocolVersion

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command


@dataclass
class ServerContext:
    databases: dict[int, Database] = field(default_factory=lambda: {0: Database(0)})
    acl: ACL = field(default_factory=ACL.create)
    client_ids: Iterable[int] = field(default_factory=lambda: itertools.count(0))
    clients: ClientList = field(default_factory=ClientList)
    configurations: Configurations = field(default_factory=Configurations)
    information: Information = field(default_factory=Information)
    notification_manager: BlockingManager = field(default_factory=BlockingManager)

    def __post_init__(self) -> None:
        self.information.server_context = self

    is_paused: bool = False
    pause_timeout: float = 0

    def get_or_create_database(self, index: int) -> Database:
        if index not in self.databases:
            self.databases[index] = Database(index)
        return self.databases[index]

    def reset(self) -> None:
        self.databases.clear()
        self.get_or_create_database(0)
        self.acl = ACL.create()
        self.client_ids = itertools.count(0)
        self.clients = ClientList()
        self.configurations = Configurations()
        self.information = Information()
        self.notification_manager = BlockingManager()


@dataclass
class TransactionContext:
    commands: list[Command] = field(default_factory=list)
    is_aborted: bool = False


@dataclass
class ClientContext:
    server_context: ServerContext
    current_client: Client

    current_database: int = 0
    current_user: ACLUser | None = None

    transaction_context: TransactionContext | None = None
    client_watchlist: ClientWatchlist = field(default_factory=ClientWatchlist)

    protocol: RespProtocolVersion = RespProtocolVersion.RESP2

    @property
    def database(self) -> Database:
        return self.server_context.get_or_create_database(self.current_database)

    @classmethod
    def create(cls, server_context: ServerContext, host: bytes, port: int) -> Self:
        return cls(server_context, server_context.clients.create_client(host, port))
