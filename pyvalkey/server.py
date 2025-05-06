from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass, field
from io import BytesIO
from traceback import print_exc
from types import FrameType
from typing import cast

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.router import CommandsRouter
from pyvalkey.commands.transactions_commands import (
    TransactionDiscard,
    TransactionExecute,
    TransactionStart,
    TransactionWatch,
)
from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.database_objects.clients import Client, ClientList
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, UnblockMessage
from pyvalkey.database_objects.errors import (
    CommandPermissionError,
    RouterKeyError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ServerWrongTypeError,
)
from pyvalkey.resp import RESP_OK, RespError, RespFatalError, RespParser, RespSyntaxError, ValueType, dump

logger = logging.getLogger(__name__)


@dataclass
class ValkeyClientProtocol(asyncio.Protocol):
    server_context: ServerContext
    router: CommandsRouter

    _transport: asyncio.BaseTransport | None = None
    _client_context: ClientContext | None = None
    data: BytesIO = field(default_factory=BytesIO)

    _resp_query_parser: RespParser = field(default_factory=RespParser)

    parser_task: asyncio.Task | None = None

    @property
    def configurations(self) -> Configurations:
        return self.server_context.configurations

    @property
    def databases(self) -> dict[int, Database]:
        return self.server_context.databases

    @property
    def clients(self) -> ClientList:
        return self.server_context.clients

    @property
    def acl(self) -> ACL:
        return self.server_context.acl

    @property
    def client_context(self) -> ClientContext:
        if self._client_context is None:
            raise Exception("must initialize client first")
        return self._client_context

    @property
    def current_client(self) -> Client:
        return self.client_context.current_client

    @property
    def current_user(self) -> ACLUser | None:
        return self.client_context.current_user

    @property
    def transport(self) -> asyncio.Transport:
        if self._transport is None:
            raise Exception("must initialize client first")
        return cast(asyncio.Transport, self._transport)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport
        host: str
        port: int
        host, port = transport.get_extra_info("peername")
        self._client_context = ClientContext.create(self.server_context, host.encode(), port)

        self.parser_task = asyncio.create_task(self.parse())

    def connection_lost(self, exception: Exception | None) -> None:
        print(f"{self.current_client.client_id} connection lost")
        if self.client_context.current_client.blocking_queue is not None:
            self.client_context.current_client.blocking_queue.put_nowait(UnblockMessage.ERROR)
        del self.clients[self.current_client.client_id]

    def dump(self, value: ValueType) -> None:
        dumped = BytesIO()
        dump(value, dumped, self.client_context.protocol)
        print(self.current_client.client_id, "result", dumped.getvalue()[:100])

        if self.current_client.reply_mode == "skip":
            self.current_client.reply_mode = "on"
            return

        if self.current_client.reply_mode == "off":
            return

        dump(value, self.transport, self.client_context.protocol)

    async def parse(self) -> None:
        try:
            async for query in self._resp_query_parser:
                await self.handle(query)
                await asyncio.sleep(0)
        except RespSyntaxError as e:
            if self.client_context.transaction_context is not None:
                self.client_context.transaction_context.is_aborted = True
            if e.args:
                self.dump(RespError(e.args[0]))
            self.cancel()
        except RespFatalError:
            self.cancel()

    def cancel(self) -> None:
        self.transport.close()
        if self.parser_task:
            self.parser_task.cancel()

    def data_received(self, data: bytes) -> None:
        try:
            self._resp_query_parser.feed(data)
        except RespFatalError:
            self.cancel()

    async def handle(self, command: list[bytes]) -> None:
        if not command:
            return
        if command[0] == b"QUIT":
            self.dump(RESP_OK)
            self.transport.close()
            return

        self.server_context.information.total_commands_processed += 1

        print(self.current_client.client_id, [i[:100] if i and not isinstance(i, int) else i for i in command])

        try:
            routed_command_cls, parameters = self.router.route(command)
            command_statistics = self.server_context.information.get_command_statistics(
                routed_command_cls.full_command_name
            )
        except RouterKeyError:
            if self.client_context.transaction_context is not None:
                self.client_context.transaction_context.is_aborted = True
            self.dump(
                RespError(
                    f"ERR unknown command '{command[0].decode()}', "
                    f"with args beginning with: {command[1].decode() if len(command) > 1 else ''}".encode()
                )
            )
            return
        try:
            routed_command = routed_command_cls.create(parameters, self.client_context)
            if self.configurations.maxmemory > 0:
                if b"denyoom" in routed_command.flags:
                    raise ServerError(b"ERR OOM command not allowed when used memory > 'maxmemory'.")

            if self.client_context.transaction_context is not None:
                if b"nomulti" in routed_command.flags:
                    raise ServerError(b"ERR Command not allowed inside a transaction")
                if not isinstance(
                    routed_command, (TransactionExecute | TransactionDiscard | TransactionStart | TransactionWatch)
                ):
                    self.client_context.transaction_context.commands.append(routed_command)
                    self.dump("QUEUED")
                    return

            if self.current_user:
                self.current_user.check_permissions(routed_command)

            await routed_command.before()
            start = time.time()
            result = routed_command.execute()
            command_statistics.microseconds += int(time.time() - start) * 1000000
            if not isinstance(result, RespError):
                await routed_command.after()
            self.dump(result)
            command_statistics.calls += 1

            self.current_client.last_command = routed_command.full_command_name

            if self.server_context.pause_timeout:
                while self.server_context.is_paused and time.time() < self.server_context.pause_timeout:
                    time.sleep(0.1)
                self.server_context.pause_timeout = 0
        except RouterKeyError:
            if self.client_context.transaction_context is not None:
                self.client_context.transaction_context.is_aborted = True
            self.dump(
                RespError(
                    f"ERR unknown command '{command[0].decode()}', "
                    f"with args beginning with: {command[1].decode() if len(command) > 1 else ''}".encode()
                )
            )
        except ServerWrongNumberOfArgumentsError:
            command_statistics.rejected_calls += 1
            self.dump(RespError(b"ERR wrong number of arguments for '" + command[0].lower() + b"' command"))
        except ServerWrongTypeError:
            command_statistics.failed_calls += 1
            self.dump(RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value"))
        except CommandPermissionError as e:
            command_statistics.rejected_calls += 1
            if not self.current_user:
                raise e
            self.dump(
                RespError(
                    b"NOPERM User "
                    + self.current_user.name
                    + b" has no permissions to run the '"
                    + e.command_name
                    + b"' command"
                )
            )
        except ServerError as e:
            command_statistics.failed_calls += 1
            if self.client_context.transaction_context is not None:
                self.client_context.transaction_context.is_aborted = True
            self.dump(RespError(e.message))
        except Exception as e:
            print_exc()
            self.dump(RespError(b"ERR internal"))
            raise e


HANDLED_SIGNALS: tuple[signal.Signals, ...] = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
if sys.platform == "win32":  # pragma: py-not-win32
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # Windows signal 21. Sent by Ctrl+Break.


@dataclass
class ValkeyServer:
    host: str
    port: int

    context: ServerContext = field(default_factory=ServerContext)
    router: CommandsRouter = field(default_factory=CommandsRouter)

    _captured_signals: list[int] = field(default_factory=list)
    should_exit: bool = False
    force_exit: bool = False
    server: asyncio.Server | None = None

    async def serve(self) -> None:
        with self.capture_signals():
            await self._serve()

    async def _serve(self) -> None:
        await self.startup()
        if self.should_exit:
            return
        await self.main_loop()
        await self._shutdown()

    async def startup(self) -> None:
        loop = asyncio.get_running_loop()

        self.server = await loop.create_server(
            lambda: ValkeyClientProtocol(self.context, self.router), self.host, self.port
        )

    async def main_loop(self) -> None:
        while not self.should_exit:
            await asyncio.sleep(0.1)

    def run(self) -> None:
        return asyncio.run(self.serve())

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        self._captured_signals.append(sig)
        if self.should_exit and sig == signal.SIGINT:
            self.force_exit = True  # pragma: full coverage
        else:
            self.should_exit = True

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None, None, None]:
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        original_handlers = {sig: signal.signal(sig, self.handle_exit) for sig in HANDLED_SIGNALS}
        try:
            yield
        finally:
            for sig, handler in original_handlers.items():
                signal.signal(sig, handler)
        for captured_signal in reversed(self._captured_signals):
            signal.raise_signal(captured_signal)

    async def _shutdown(self) -> None:
        if self.server:
            self.server.close()

    def shutdown(self) -> None:
        self.should_exit = True
