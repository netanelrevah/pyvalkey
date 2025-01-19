from dataclasses import dataclass

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.resp import RESP_OK, RespError, ValueType


@dataclass
class TransactionStart(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        self.client_context.transaction_commands = []

        return RESP_OK


@dataclass
class TransactionExecute(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        if self.client_context.transaction_commands is None:
            return RespError(b"ERR EXEC without MULTI")
        return [command.execute() for command in self.client_context.transaction_commands]
