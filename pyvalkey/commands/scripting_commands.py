from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.resp import RESP_OK, ValueType


@command(b"eval", {b"connection", b"fast"})
class Eval(Command):
    script: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"flush", {b"fast", b"connection"}, parent_command=b"function")
class FunctionFLush(Command):
    def execute(self) -> ValueType:
        return True


@command(b"kill", {b"connection", b"fast"}, parent_command=b"script")
class ScriptKill(Command):
    def execute(self) -> ValueType:
        return RESP_OK
