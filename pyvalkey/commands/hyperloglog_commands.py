from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.resp import ValueType


@command(b"pfadd", {b"fast", b"hyperloglog"}, flags={b"write"})
class HyperLogLogAdd(Command):
    key: bytes = positional_parameter()
    elements: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfcount", {b"hyperloglog", b"read", b"slow"}, flags={b"write"})
class HyperLogLogCount(Command):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfdebug", {b"admin", b"dangerous", b"hyperloglog", b"slow"}, flags={b"write"})
class HyperLogLogDebug(Command):
    subcommand: bytes = positional_parameter()
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfmerge", {b"hyperloglog", b"slow"}, flags={b"write"})
class HyperLogLogMerge(Command):
    destination_key: bytes = positional_parameter()
    source_keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfselftest", {b"admin", b"dangerous", b"hyperloglog", b"slow"}, flags={b"write"})
class HyperLogLogSelfTest(Command):
    def execute(self) -> ValueType:
        return 1
