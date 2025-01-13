from dataclasses import dataclass

from pyvalkey.commands.context import ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@dataclass
class ServerCommand(Command):
    server_context: ServerContext

    def execute(self) -> ValueType:
        raise NotImplementedError()


@ServerCommandsRouter.command(b"flush", parent_command=b"function", acl_categories=[b"fast", b"connection"])
class FunctionFLush(Command):
    def execute(self) -> ValueType:
        return True
