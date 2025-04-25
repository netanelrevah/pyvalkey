from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Self, dataclass_transform

from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext


@dataclass_transform()
@dataclass
class Command:
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


@dataclass
class DatabaseCommand(Command):
    database: Database = server_command_dependency()
