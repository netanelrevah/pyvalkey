from pyvalkey.commands.core import Command
from pyvalkey.commands.router import command
from pyvalkey.resp import ValueType


@command(b"geoadd", [b"geo", b"write", b"slow"])
class GeoAdd(Command):
    def execute(self) -> ValueType:
        pass
