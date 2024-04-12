from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.resp import RESP_OK


@ServerCommandsRouter.command(b"get", [b"admin", b"slow", b"dangerous"], b"config")
class ConfigGet(Command):
    configurations: Configurations = server_command_dependency()
    parameters: list[bytes] = positional_parameter()

    def execute(self):
        names = self.configurations.get_names(*self.parameters)
        return self.configurations.info(names)


@ServerCommandsRouter.command(b"set", [b"admin", b"slow", b"dangerous"], b"config")
class ConfigSet(Command):
    configurations: Configurations = server_command_dependency()
    parameters_values: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self):
        for name, value in self.parameters_values:
            self.configurations.set_values(name, value)
        return RESP_OK
