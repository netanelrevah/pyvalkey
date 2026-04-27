from dataclasses import field

from pyvalkey.blocking import ListBlockingManager, SortedSetBlockingManager, StreamBlockingManager
from pyvalkey.commands.context import ClientContext, TransactionContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.database_objects.databases import (
    ClientWatchlist,
    Database,
)
from pyvalkey.resp import RESP_OK, BulkArray, DoNotReply, RespError, ValueType
from pyvalkey.utils.dependencies import dependency


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


@command(b"multi", {b"transaction"}, flags={b"allow_busy", b"fast", b"loading", b"noscript", b"no_multi", b"stale"})
class TransactionStart(Command):
    """
    summary: Starts a transaction.
    complexity: O(1)
    since: 1.2.0
    function: multiCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        self.client_context.transaction_context = TransactionContext()

        return RESP_OK


@command(b"discard", {b"transaction"}, flags={b"allow_busy", b"fast", b"loading", b"noscript", b"stale"})
class TransactionDiscard(Command):
    """
    summary: Discards a transaction.
    complexity: O(N), when N is the number of queued commands
    since: 2.0.0
    function: discardCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        if self.client_context.transaction_context is None:
            return RespError(b"ERR DISCARD without MULTI")

        unwatch(self.client_context.server_context.databases, self.client_context.client_watchlist)
        self.client_context.transaction_context = None

        return RESP_OK


@command(b"exec", {b"slow", b"transaction"}, flags={b"loading", b"noscript", b"skip_commandlog", b"stale"})
class TransactionExecute(Command):
    """
    summary: Executes all commands in a transaction.
    complexity: Depends on commands in the transaction
    since: 1.2.0
    function: execCommand
    reply_schema:
      oneOf:
      - description: Each element being the reply to each of the commands in the atomic transaction.
        type: array
      - description: The transaction was aborted because a `WATCH`ed key was touched
        type: 'null'
    """

    database: Database = dependency()
    client_context: ClientContext = dependency()
    list_blocking_manager: ListBlockingManager = dependency()
    sorted_set_blocking_manager: SortedSetBlockingManager = dependency()
    stream_blocking_manager: StreamBlockingManager = dependency()

    _result: ValueType = field(default=None, init=False)
    _keys_to_notify: set[bytes] = field(default_factory=set, init=False)

    async def before(self, in_multi: bool = False) -> None:
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
            result = transaction_command.execute()
            if result is not DoNotReply:
                if isinstance(result, BulkArray):
                    for item in result:
                        self._result.append(item)
                else:
                    self._result.append(result)
            await transaction_command.after(in_multi=True)

    def execute(self) -> ValueType:
        if self.client_context.transaction_context is not None:
            unwatch(self.client_context.server_context.databases, self.client_context.client_watchlist)
        self.client_context.transaction_context = None

        return self._result

    async def after(self, in_multi: bool = False) -> None:
        await self.list_blocking_manager.notify_lazy(self.database)
        await self.sorted_set_blocking_manager.notify_lazy(self.database)
        await self.stream_blocking_manager.notify_lazy(self.database)


@command(b"watch", {b"transaction"}, flags={b"allow_busy", b"fast", b"loading", b"noscript", b"no_multi", b"stale"})
class TransactionWatch(Command):
    """
    summary: Monitors changes to keys to determine the execution of a transaction.
    complexity: O(1) for every key.
    since: 2.2.0
    function: watchCommand
    reply_schema:
      const: OK
    """

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


@command(b"unwatch", {b"transaction"}, flags={b"allow_busy", b"fast", b"loading", b"noscript", b"stale"})
class TransactionUnwatch(Command):
    """
    summary: Forgets about watched keys of a transaction.
    complexity: O(1)
    since: 2.2.0
    function: unwatchCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        unwatch(self.client_context.server_context.databases, self.client_context.client_watchlist)
        return RESP_OK
