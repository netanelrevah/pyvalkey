from dataclasses import dataclass

from pyvalkey.commands.context import ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import keyword_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@dataclass
class ServerCommand(Command):
    server_context: ServerContext

    def execute(self) -> ValueType:
        raise NotImplementedError()


@ServerCommandsRouter.command(b"debug", acl_categories=[b"fast", b"connection"])
class Debug(Command):
    set_active_expire: int = keyword_parameter(flag=b"set-active-expire", default=b"0")

    def execute(self) -> ValueType:
        return True
