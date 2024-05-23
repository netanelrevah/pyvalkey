from dataclasses import dataclass
from typing import Any, Self

from pyvalkey.commands.context import ClientContext
from pyvalkey.resp import ValueType


@dataclass
class Command:
    def execute(self) -> ValueType:
        raise NotImplementedError()

    @staticmethod
    def parse(parameters: list[bytes]) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def create(cls, parameters: list[bytes], client_context: ClientContext) -> Self:
        raise NotImplementedError()
