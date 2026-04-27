from enum import Enum

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType


class ResetMode(Enum):
    HARD = b"HARD"
    SOFT = b"SOFT"


@command(b"reset", {b"admin", b"dangerous", b"slow"}, b"cluster", flags={b"admin", b"noscript", b"stale"})
class ClusterReset(Command):
    """
    summary: Resets a node.
    complexity: >-
      O(N) where N is the number of known nodes. The command may execute a FLUSHALL as a side effect.
    since: 3.0.0
    function: clusterCommand
    reply_schema:
      const: OK
    """

    reset_mode: ResetMode = positional_parameter(default=ResetMode.SOFT)

    def execute(self) -> ValueType:
        raise ServerError(b"ERR This instance has cluster support disabled")
