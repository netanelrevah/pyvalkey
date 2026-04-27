from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Self, dataclass_transform

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.resp import ValueType


@dataclass_transform()
@dataclass
class Command:
    full_command_name: ClassVar[bytes]
    flags: ClassVar[set[bytes]]

    async def before(self, in_multi: bool = False) -> None:
        pass

    def execute(self) -> ValueType:
        raise NotImplementedError()

    async def after(self, in_multi: bool = False) -> None:
        pass

    @staticmethod
    def parse(parameters: list[bytes]) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def create(cls, parameters: list[bytes], client_context: ClientContext) -> Self:
        raise NotImplementedError()

    @classmethod
    def collect_key_arguments(cls, command_arguments: dict[str, Any]) -> list[bytes] | None:
        pass
