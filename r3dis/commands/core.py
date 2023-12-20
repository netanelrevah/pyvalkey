from typing import Any

from typing_extensions import Self

from r3dis.commands.context import ClientContext


class Command:
    def execute(self):
        raise NotImplementedError()

    @staticmethod
    def parse(parameters: list[bytes]) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def create(cls, parameters: list[bytes], client_context: ClientContext) -> Self:
        raise NotImplementedError()
