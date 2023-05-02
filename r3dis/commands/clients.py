import time
from dataclasses import dataclass
from typing import Any

from r3dis.commands.core import CommandHandler
from r3dis.errors import RedisSyntaxError, RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK, RespError


@dataclass
class ClientList(CommandHandler):
    def handle(self, type_: bytes | None = None):
        if type_:
            return self.clients.filter_(client_type=type_).info
        return self.clients.info

    @classmethod
    def parse(cls, parameters: list[bytes]):
        type_ = None
        while parameters:
            match parameters.pop(0):
                case b"TYPE":
                    type_ = parameters.pop(0)
                case _:
                    raise RedisSyntaxError()

        return type_


@dataclass
class ClientId(CommandHandler):
    def handle(self):
        return self.current_client.client_id

    @classmethod
    def parse(cls, parameters: list[bytes]):
        while parameters:
            match parameters.pop(0):
                case _:
                    raise RedisSyntaxError()


@dataclass
class ClientSetName(CommandHandler):
    def handle(self, name: bytes):
        self.current_client.name = name
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            raise RedisWrongNumberOfArguments()
        return parameters.pop(0)


@dataclass
class ClientGetName(CommandHandler):
    def handle(self):
        return self.current_client.name or None

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if parameters:
            raise RedisWrongNumberOfArguments()


@dataclass
class ClientKill(CommandHandler):
    def handle(self, filters: dict[str, Any], old_format: bool = False):
        if old_format:
            clients = self.clients.filter_(**filters).values()
            if not clients:
                return RespError(b"ERR No such client")
            (client,) = clients
            client.is_killed = True
            return RESP_OK

        clients = self.clients.filter_(**filters).values()
        for client in clients:
            client.is_killed = True
        return len(clients)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) == 1:
            return {"address": parameters.pop(0)}, True

        filters = {}
        while parameters:
            match parameters.pop(0):
                case b"ID":
                    filters["client_id"] = int(parameters.pop(0))
                case b"ADDR":
                    filters["address"] = parameters.pop(0)
                case _:
                    raise RedisSyntaxError()

        return filters, False


@dataclass
class ClientPause(CommandHandler):
    def handle(self, timeout_seconds: int):
        self.command_context.server_context.pause_timeout = time.time() + int(timeout_seconds)
        self.command_context.server_context.is_paused = True
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            raise RedisWrongNumberOfArguments()
        timeout_seconds = parameters.pop(0)
        if not timeout_seconds.isdigit():
            return RespError(b"ERR timeout is not an integer or out of range")
        return timeout_seconds


@dataclass
class ClientUnpause(CommandHandler):
    def handle(self, timeout_seconds: int):
        self.command_context.server_context.is_paused = False
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 0:
            raise RedisWrongNumberOfArguments()


@dataclass
class ClientReply(CommandHandler):
    def handle(self, mode: bytes):
        if mode == b"ON":
            return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            raise RedisWrongNumberOfArguments()
        mode = parameters.pop(0)
        if mode not in (b"ON", b"OFF", b"SKIP"):
            raise RedisSyntaxError()
        return mode.lower()
