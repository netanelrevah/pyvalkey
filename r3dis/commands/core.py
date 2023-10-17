from dataclasses import dataclass

from black.linegen import partial
from typing import Any
from typing_extensions import Self

from r3dis.commands.context import ClientContext
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.consts import Commands


@dataclass
class Command:
    def execute(self):
        raise NotImplementedError()

    @staticmethod
    def parse(parameters: list[bytes]) -> dict[str, Any]:
        pass

    @classmethod
    def create(cls, parameters: list[bytes], client_context: ClientContext) -> Self:
        pass


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


def create_smart_command_parser(router, command: Commands, command_cls: type[Command], *args, **kwargs):
    router.routes[command] = SmartCommandParser(command_cls, partial(command_cls, *args, **kwargs))
