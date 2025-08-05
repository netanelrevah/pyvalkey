from dataclasses import field

from pyvalkey.commands.context import ClientContext, TransactionContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.database_objects.databases import (
    ClientWatchlist,
    Database,
    ListBlockingManager,
    SortedSetBlockingManager,
    StreamBlockingManager,
)
from pyvalkey.resp import RESP_OK, RespError, ValueType


def unwatch(databases: dict[int, Database], client_watchlist: ClientWatchlist) -> None:
    for index, key in client_watchlist.watchlist:
        if index not in databases:
            continue
        watchlist_database: Database = databases[index]
        if key not in watchlist_database.content.watchlist:
            continue
        key_database_watchlist = watchlist_database.content.watchlist[key]
        key_database_watchlist.remove(client_watchlist)
    client_watchlist.watchlist = {}


@command(b"multi", {b"transaction", b"fast"}, flags={b"nomulti"})
class TransactionStart(Command):
    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        self.client_context.transaction_context = TransactionContext()

        return RESP_OK


@command(b"discard", {b"transaction", b"fast"})
class TransactionDiscard(Command):
    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        if self.client_context.transaction_context is None:
            return RespError(b"ERR DISCARD without MULTI")

        unwatch(self.client_context.server_context.databases, self.client_context.client_watchlist)
        self.client_context.transaction_context = None

        return RESP_OK


@command(b"exec", {b"connection", b"fast"})
class TransactionExecute(Command):
    database: Database = dependency()
    client_context: ClientContext = dependency()
    list_blocking_manager: ListBlockingManager = dependency()
    sorted_set_blocking_manager: SortedSetBlockingManager = dependency()
    stream_blocking_manager: StreamBlockingManager = dependency()

    _result: ValueType = field(default=None, init=False)
    _keys_to_notify: set[bytes] = field(default_factory=set, init=False)

    async def before(self, _: bool = False) -> None:
        if (
            self.client_context.transaction_context is not None
            and self.client_context.transaction_context.is_aborted is True
        ):
            self._result = RespError(b"EXECABORT Transaction discarded because of previous errors.")
            return
        if self.client_context.transaction_context is None:
            self._result = RespError(b"ERR EXEC without MULTI")
            return

        if any(self.client_context.client_watchlist.watchlist.values()):
            return

        self._result = []
        for transaction_command in self.client_context.transaction_context.commands:
            await transaction_command.before(in_multi=True)
            self._result.append(transaction_command.execute())
            await transaction_command.after(in_multi=True)

    def execute(self) -> ValueType:
        if self.client_context.transaction_context is not None:
            unwatch(self.client_context.server_context.databases, self.client_context.client_watchlist)
        self.client_context.transaction_context = None

        return self._result

    async def after(self, _: bool = False) -> None:
        await self.list_blocking_manager.notify_lazy(self.database)
        await self.sorted_set_blocking_manager.notify_lazy(self.database)
        await self.stream_blocking_manager.notify_lazy(self.database)


@command(b"watch", {b"transaction", b"fast"}, flags={b"nomulti"})
class TransactionWatch(Command):
    client_context: ClientContext = dependency()

    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        for key in self.keys:
            print(
                f"watch key {key.decode()} "
                f"of client {self.client_context.current_client.client_id} "
                f"in database {self.client_context.current_database}"
            )
            self.client_context.database.add_key_to_watchlist(key, self.client_context.client_watchlist)

        return RESP_OK


@command(b"unwatch", {b"transaction", b"fast"}, flags={b"nomulti"})
class TransactionUnwatch(Command):
    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        unwatch(self.client_context.server_context.databases, self.client_context.client_watchlist)
        return RESP_OK
