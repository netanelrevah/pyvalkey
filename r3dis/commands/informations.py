from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.router import RedisCommandsRouter
from r3dis.consts import Commands
from r3dis.information import Information

information_commands_router = RedisCommandsRouter()


@information_commands_router.command(Commands.Information)
class GetInformation(Command):
    information: Information = redis_command_dependency()

    def execute(self):
        return self.information.all()
