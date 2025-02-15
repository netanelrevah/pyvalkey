from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import RESP_OK, RespError, ValueType


@ServerCommandsRouter.command(b"multi", [b"transaction", b"fast"])
class TransactionStart(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        self.client_context.transaction_commands = []

        return RESP_OK


@ServerCommandsRouter.command(b"exec", [b"connection", b"fast"])
class TransactionExecute(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        if self.client_context.transaction_commands is None:
            return RespError(b"ERR EXEC without MULTI")
        results = [command.execute() for command in self.client_context.transaction_commands]
        self.client_context.transaction_commands = None
        return results
