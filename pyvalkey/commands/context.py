from __future__ import annotations

import itertools
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from pyvalkey.commands.scripting import ScriptingEngine
from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.database_objects.clients import Client, ClientsMap
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import (
    BlockingManager,
    ClientWatchlist,
    Database,
)
from pyvalkey.database_objects.information import Information
from pyvalkey.resp import RespProtocolVersion

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command
    from pyvalkey.commands.router import CommandsRouter


@dataclass
class ServerContext:
    databases: dict[int, Database] = field(default_factory=lambda: {0: Database(0)})
    acl: ACL = field(default_factory=ACL.create)
    client_ids: Iterable[int] = field(default_factory=lambda: itertools.count(0))
    clients: ClientsMap = field(default_factory=ClientsMap)
    configurations: Configurations = field(default_factory=Configurations)
    information: Information = field(default_factory=Information)
    blocking_manager: BlockingManager = field(default_factory=BlockingManager)

    def __post_init__(self) -> None:
        self.information.server_context = self

    is_paused: bool = False
    is_paused_for_write: bool = False

    pause_timeout: float = 0

    def num_of_blocked_clients(self) -> int:
        return sum(1 for client in self.clients.values() if client.blocking_context is not None)

    def num_of_blocked_client_for_no_key(self) -> int:
        return sum(
            1
            for client in self.clients.values()
            if client.blocking_context is not None and client.blocking_context.command in {b"xreadgroup"}
        )

    def get_or_create_database(self, index: int) -> Database:
        if index not in self.databases:
            self.databases[index] = Database(index)
        return self.databases[index]

    def reset(self) -> None:
        self.databases.clear()
        self.get_or_create_database(0)
        self.acl = ACL.create()
        self.client_ids = itertools.count(0)
        self.clients = ClientsMap()
        self.configurations = Configurations()
        self.information = Information()
        self.blocking_manager = BlockingManager()


@dataclass
class TransactionContext:
    commands: list[Command] = field(default_factory=list)
    is_aborted: bool = False


@dataclass
class ClientContext:
    server_context: ServerContext
    current_client: Client

    scripting_manager: ScriptingEngine

    current_database: int = 0
    current_user: ACLUser | None = None

    transaction_context: TransactionContext | None = None
    client_watchlist: ClientWatchlist = field(default_factory=ClientWatchlist)

    protocol: RespProtocolVersion = RespProtocolVersion.RESP2

    propagated_commands: list[Command] = field(default_factory=list)

    @property
    def database(self) -> Database:
        return self.server_context.get_or_create_database(self.current_database)

    @classmethod
    def create(cls, server_context: ServerContext, host: bytes, port: int, router: CommandsRouter) -> Self:
        scripting_manager = ScriptingEngine.create()

        client_context = cls(
            server_context,
            server_context.clients.create_client(host, port),
            scripting_manager=scripting_manager,
        )
        scripting_manager._client_context = client_context
        scripting_manager._commands_router = router
        return client_context
