from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.resp import ValueType


@command(b"pfadd", {b"stream", b"write", b"fast"})
class HyperLogLogAdd(Command):
    key: bytes = positional_parameter()
    elements: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfcount", {b"stream", b"write", b"fast"})
class HyperLogLogCount(Command):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfdebug", {b"stream", b"write", b"fast"})
class HyperLogLogDebug(Command):
    subcommand: bytes = positional_parameter()
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfmerge", {b"stream", b"write", b"fast"})
class HyperLogLogMerge(Command):
    destination_key: bytes = positional_parameter()
    source_keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"pfselftest", {b"stream", b"write", b"fast"})
class HyperLogLogSelfTest(Command):
    def execute(self) -> ValueType:
        return 1
