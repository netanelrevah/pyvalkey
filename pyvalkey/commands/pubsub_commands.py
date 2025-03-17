from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"subscribe", [b"slow", b"connection"])
class Subscribe(Command):
    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return ["genpass"]
