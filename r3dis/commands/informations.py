from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.information import Information


@RedisCommandsRouter.command(b"info", [b"slow", b"dangerous"])
class GetInformation(Command):
    information: Information = redis_command_dependency()

    def execute(self):
        return self.information.all()
