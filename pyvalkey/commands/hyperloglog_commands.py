from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"pfadd", [b"stream", b"write", b"fast"])
class HyperLogLogAdd(Command):
    key: bytes = positional_parameter()
    elements: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"pfcount", [b"stream", b"write", b"fast"])
class HyperLogLogCount(Command):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"pfdebug", [b"stream", b"write", b"fast"])
class HyperLogLogDebug(Command):
    subcommand: bytes = positional_parameter()
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"pfmerge", [b"stream", b"write", b"fast"])
class HyperLogLogMerge(Command):
    destination_key: bytes = positional_parameter()
    source_keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"pfselftest", [b"stream", b"write", b"fast"])
class HyperLogLogSelfTest(Command):
    def execute(self) -> ValueType:
        return 1
