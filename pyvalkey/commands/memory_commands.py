from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"usage", [b"read", b"slow"], b"memory")
class MemoryUsage(Command):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1
