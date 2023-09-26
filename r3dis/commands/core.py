from dataclasses import dataclass
from typing import Callable

from r3dis.errors import RedisWrongNumberOfArguments


@dataclass
class Command:
    def execute(self):
        raise NotImplementedError()


class CommandParser:
    def parse(self, parameters: list[bytes]) -> Command:
        raise NotImplementedError()


@dataclass
class BytesParametersParser(CommandParser):
    command_creator: Callable[[bytes, ...], Command]

    number_of_parameters: int = None

    def parse(self, parameters: list[bytes]) -> Command:
        if self.number_of_parameters and len(parameters) != self.number_of_parameters:
            raise RedisWrongNumberOfArguments()
        return self.command_creator(*parameters)
