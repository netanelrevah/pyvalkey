from pyvalkey.commands.core import Command
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"add", [b"geo", b"write", b"slow"], b"geo")
class GeoAdd(Command):
    def execute(self) -> ValueType:
        pass
