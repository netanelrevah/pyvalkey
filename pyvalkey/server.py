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
from pyvalkey.commands.core import Command
from pyvalkey.commands.executors import CommandExecutor
from pyvalkey.commands.router import CommandsRouter
from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.database_objects.clients import Client, ClientsMap
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import (
    RouterKeyError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
)
from pyvalkey.enums import ReplyMode, UnblockMessage
from pyvalkey.resp import RESP_OK, DoNotReply, RespError, RespFatalError, RespParser, RespSyntaxError, ValueType, dump
from pyvalkey.utils.times import now_f_s

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
    pubsub_task: asyncio.Task | None = None
    command_handling_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def configurations(self) -> Configurations:
        return self.server_context.configurations

    @property
    def databases(self) -> dict[int, Database]:
        return self.server_context.databases

    @property
    def clients(self) -> ClientsMap:
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
        self._client_context = ClientContext.create(self.server_context, host.encode(), port, self.router)

        self.parser_task = asyncio.create_task(self.parse())
        self.pubsub_task = asyncio.create_task(self.pubsub())

    def connection_lost(self, exception: Exception | None) -> None:
        print(f"{self.current_client.client_id} connection lost")
        self.client_context.subscriptions.unsubscribe_all()
        if self.client_context.current_client.blocking_context is not None:
            self.client_context.current_client.blocking_context.queue.put_nowait(UnblockMessage.ERROR)
        del self.clients[self.current_client.client_id]

    async def pubsub(self) -> None:
        try:
            while True:
                message = await self.current_client.push_message_queue.get()
                async with self.command_handling_lock:
                    self.dump(message, push_message=True)
        except asyncio.CancelledError:
            pass

    def dump(self, value: ValueType, push_message: bool = False) -> None:
        dumped = BytesIO()
        dump(value, dumped, self.client_context.protocol)

        print(f"{self.current_client.client_id} reply:{self.current_client.reply_mode} {dumped.getvalue()[:103]!r}")

        if not push_message:
            if self.current_client.reply_mode == ReplyMode.SKIP:
                self.current_client.reply_mode = ReplyMode.ON
                return

            if self.current_client.reply_mode == ReplyMode.OFF:
                return

        dump(value, self.transport, self.client_context.protocol)

    async def parse(self) -> None:
        try:
            async for query in self._resp_query_parser:
                async with self.command_handling_lock:
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

    def _is_paused(self, routed_command_cls: type[Command]) -> bool:
        if self.server_context.is_paused:
            return True
        if self.server_context.is_paused_for_write:
            if b"write" in routed_command_cls.flags:
                return True
        return False

    async def handle(self, command: list[bytes]) -> None:
        if not command:
            return
        if command[0] == b"QUIT":
            self.dump(RESP_OK)
            self.transport.close()
            return

        self.server_context.information.total_commands_processed += 1
        self.current_client.command_time_snapshot = time.time_ns() // 1_000_000

        print(self.current_client.client_id, [i[:300] if i and not isinstance(i, int) else i for i in command])

        try:
            routed_command_cls, parameters = self.router.route(command)

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

        if self.server_context.pause_timeout:
            while self._is_paused(routed_command_cls) or now_f_s() < self.server_context.pause_timeout:
                time.sleep(0.1)
            self.server_context.pause_timeout = 0

        command_statistics = self.server_context.information.get_command_statistics(
            routed_command_cls.full_command_name
        )

        try:
            routed_command = routed_command_cls.create(parameters, self.client_context)
        except ServerWrongNumberOfArgumentsError:
            command_statistics.rejected_calls += 1
            self.dump(
                RespError(
                    b"ERR wrong number of arguments for '" + routed_command_cls.full_command_name.lower() + b"' command"
                )
            )
            self.server_context.information.error_statistics[b"ERR"] += 1
            return
        except ServerError as e:
            command_statistics.failed_calls += 1
            if self.client_context.transaction_context is not None:
                self.client_context.transaction_context.is_aborted = True
            self.dump(RespError(e.message))
            self.server_context.information.error_statistics[e.message.split()[0].upper()] += 1
            return
        except Exception as e:
            print_exc()
            self.dump(RespError(b"ERR internal"))
            self.server_context.information.error_statistics[b"ERR"] += 1
            raise e

        command_executor = CommandExecutor(routed_command, self.client_context)
        try:
            result = await command_executor.execute()
            if result is not DoNotReply:
                self.dump(result, self.client_context.subscriptions.active_subscriptions > 0)

        except Exception as e:
            self.dump(RespError(b"ERR internal"))
            raise e

        while self.client_context.propagated_commands:
            command_executor = CommandExecutor(self.client_context.propagated_commands.pop(0), self.client_context)
            try:
                await command_executor.execute()
            except Exception as e:
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

    _database_cleanup_task: asyncio.Task | None = None

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
        self._database_cleanup_task = asyncio.create_task(self.cleanup_databases())

    async def main_loop(self) -> None:
        while not self.should_exit:
            await asyncio.sleep(0.1)

    def clean_databases(self) -> None:
        for database in self.context.databases.values():
            for key in database.content.key_with_expiration[:10]:
                database.get_or_none(key.key)

    async def cleanup_databases(self) -> None:
        while not self.should_exit:
            self.clean_databases()
            await asyncio.sleep(1.0)

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
        if self._database_cleanup_task:
            self._database_cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._database_cleanup_task
        if self.server:
            self.server.close()

    def shutdown(self) -> None:
        self.should_exit = True
