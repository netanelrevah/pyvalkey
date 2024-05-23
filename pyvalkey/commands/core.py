from dataclasses import dataclass
from typing import Any, List

from typing_extensions import Self

from pyvalkey.commands.context import ClientContext
from pyvalkey.resp import ValueType


@dataclass
class Command:
    def execute(self) -> ValueType:
        raise NotImplementedError()

    @staticmethod
    def parse(parameters: List[bytes]) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def create(cls, parameters: List[bytes], client_context: ClientContext) -> Self:
        raise NotImplementedError()
