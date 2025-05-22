from enum import Enum

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType


class ResetMode(Enum):
    HARD = b"HARD"
    SOFT = b"SOFT"


@command(b"reset", {b"slow", b"admin"}, b"cluster", flags={b"no-script"})
class ClusterReset(Command):
    reset_mode: ResetMode = positional_parameter(default=ResetMode.SOFT)

    def execute(self) -> ValueType:
        raise ServerError(b"ERR This instance has cluster support disabled")
