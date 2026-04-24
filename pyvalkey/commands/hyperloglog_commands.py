from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.resp import ValueType


@command(b"pfadd", {b"hyperloglog"}, flags={b"denyoom", b"fast", b"write"})
class HyperLogLogAdd(Command):
    """
    summary: Adds elements to a HyperLogLog key. Creates the key if it doesn't exist.
    complexity: O(1) to add every element.
    since: 2.8.9
    function: pfaddCommand
    reply_schema:
      oneOf:
      - description: If at least 1 HyperLogLog internal register was altered.
        const: 1
      - description: If no HyperLogLog internal register were altered.
        const: 0
    """

    key: bytes = positional_parameter(key_mode=b"RW")
    elements: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfcount", {b"hyperloglog", b"read", b"slow"}, flags={b"may_replicate", b"readonly"})
class HyperLogLogCount(Command):
    """
    summary: >-
      Returns the approximated cardinality of the set(s) observed by the HyperLogLog key(s).
    complexity: >-
      O(1) with a very small average constant time when called with a single key. O(N) with N being the number of
      keys, and much bigger constant times, when called with multiple keys.
    since: 2.8.9
    function: pfcountCommand
    reply_schema:
      description: The approximated number of unique elements observed via PFADD
      type: integer
    """

    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfdebug", {b"admin", b"dangerous", b"hyperloglog", b"slow"}, flags={b"admin", b"denyoom", b"write"})
class HyperLogLogDebug(Command):
    """
    summary: Internal commands for debugging HyperLogLog values.
    complexity: N/A
    since: 2.8.9
    function: pfdebugCommand
    """

    subcommand: bytes = positional_parameter()
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return 1


@command(b"pfmerge", {b"hyperloglog", b"slow"}, flags={b"denyoom", b"write"})
class HyperLogLogMerge(Command):
    """
    summary: Merges one or more HyperLogLog values into a single key.
    complexity: O(N) to merge N HyperLogLogs, but with high constant times.
    since: 2.8.9
    function: pfmergeCommand
    reply_schema:
      const: OK
    """

    destination_key: bytes = positional_parameter()
    source_keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfselftest", {b"admin", b"dangerous", b"hyperloglog", b"slow"}, flags={b"admin"})
class HyperLogLogSelfTest(Command):
    """
    summary: An internal command for testing HyperLogLog values.
    complexity: N/A
    since: 2.8.9
    function: pfselftestCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return 1
