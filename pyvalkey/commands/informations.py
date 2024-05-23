from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.information import Information
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"info", [b"slow", b"dangerous"])
class GetInformation(Command):
    information: Information = server_command_dependency()

    def execute(self) -> ValueType:
        return self.information.all()
