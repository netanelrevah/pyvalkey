from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.configurations import Configurations
from r3dis.consts import Commands
from r3dis.resp import RESP_OK

config_commands_router = RedisCommandsRouter()


@config_commands_router.command(Commands.ConfigGet)
class ConfigGet(Command):
    configurations: Configurations = redis_command_dependency()
    parameters: list[bytes] = redis_positional_parameter()

    def execute(self):
        names = self.configurations.get_names(*self.parameters)
        return self.configurations.info(names)


@config_commands_router.command(Commands.ConfigSet)
class ConfigSet(Command):
    configurations: Configurations = redis_command_dependency()
    parameters_values: list[tuple[bytes, bytes]] = redis_command_dependency()

    def execute(self):
        for name, value in self.parameters_values:
            self.configurations.set_values(name, value)
        return RESP_OK
