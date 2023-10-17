from dataclasses import dataclass
from typing import Any

from typing_extensions import Self

from r3dis.commands.context import ClientContext
from r3dis.commands.parameters import redis_positional_parameter


@dataclass
class Command:
    def execute(self):
        raise NotImplementedError()

    @staticmethod
    def parse(parameters: list[bytes]) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def create(cls, parameters: list[bytes], client_context: ClientContext) -> Self:
        raise NotImplementedError()


class CommandParser:
    def parse(self, parameters: list[bytes]) -> Command:
        raise NotImplementedError()


@dataclass
class Echo(Command):
    message: bytes = redis_positional_parameter()

    def execute(self):
        return self.message


@dataclass
class Ping(Command):
    message: bytes = redis_positional_parameter(default=None)

    def execute(self):
        if self.message:
            return self.message
        return b"PONG"
