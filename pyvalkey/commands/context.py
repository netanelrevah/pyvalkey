from __future__ import annotations

import asyncio
import itertools
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from pyvalkey.blocking import BlockingManager
from pyvalkey.commands.scripting import ScriptingEngine
from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.database_objects.clients import Client, ClientsMap
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import (
    ClientWatchlist,
    Database,
)
from pyvalkey.database_objects.information import Information
from pyvalkey.notifications import ClientSubscriptions, NotificationsManager, SubscriptionsManager
from pyvalkey.resp import RespProtocolVersion, ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command
    from pyvalkey.commands.router import CommandsRouter


@dataclass
class ServerContext:
    databases: dict[int, Database] = field(default_factory=dict)
    acl: ACL = field(default_factory=ACL.create)
    client_ids: Iterable[int] = field(default_factory=lambda: itertools.count(0))
    clients: ClientsMap = field(default_factory=ClientsMap)
    configurations: Configurations = field(default_factory=Configurations)
    information: Information = field(default_factory=Information)
    blocking_manager: BlockingManager = field(default_factory=BlockingManager)

    subscriptions_manager: SubscriptionsManager = field(default_factory=SubscriptionsManager)

    def __post_init__(self) -> None:
        self.databases = {0: Database(0, self.configurations, self.create_notification_manager(0))}
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
            self.databases[index] = Database(index, self.configurations, self.create_notification_manager(index))
        return self.databases[index]

    def create_notification_manager(self, database_index: int) -> NotificationsManager:
        return NotificationsManager(self.configurations, self.subscriptions_manager, database_index)

    @property
    def push_message_queues(self) -> set[asyncio.Queue[ValueType]]:
        return {client.push_message_queue for client in self.clients.values()}

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
class PubSubContext:
    queue: asyncio.Queue[ValueType] = field(default_factory=asyncio.Queue)
    subscribed_channels: set[bytes] = field(default_factory=set)
    subscribed_patterns: set[bytes] = field(default_factory=set)

    @property
    def active_subscriptions(self) -> int:
        return len(self.subscribed_channels) + len(self.subscribed_patterns)


@dataclass
class ClientContext:
    server_context: ServerContext
    current_client: Client

    scripting_manager: ScriptingEngine
    subscriptions: ClientSubscriptions

    current_database: int = 0
    current_user: ACLUser | None = None

    transaction_context: TransactionContext | None = None
    client_watchlist: ClientWatchlist = field(default_factory=ClientWatchlist)

    protocol: RespProtocolVersion = RespProtocolVersion.RESP2

    propagated_commands: list[Command] = field(default_factory=list)

    @property
    def database(self) -> Database:
        return self.server_context.get_or_create_database(self.current_database)

    @property
    def notifications_manager(self) -> NotificationsManager:
        return self.server_context.create_notification_manager(self.current_database)

    @classmethod
    def create(cls, server_context: ServerContext, host: bytes, port: int, router: CommandsRouter) -> Self:
        scripting_manager = ScriptingEngine.create()

        client = server_context.clients.create_client(host, port)
        client_context = cls(
            server_context,
            client,
            scripting_manager=scripting_manager,
            subscriptions=ClientSubscriptions(
                client.push_message_queue,
                server_context.subscriptions_manager,
            ),
        )
        scripting_manager._client_context = client_context
        scripting_manager._commands_router = router
        return client_context
