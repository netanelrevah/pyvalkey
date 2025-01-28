from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import RESP_OK, ValueType


@ServerCommandsRouter.command(b"eval", [b"connection", b"fast"])
class Eval(Command):
    script: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK
