from r3dis.commands.core import Command, Echo, Ping, create_smart_command_parser
from r3dis.commands.router import RouteParser
from r3dis.consts import Commands

COMMAND_TO_COMMAND_CLS: dict[Commands, type[Command]] = {
    Commands.Ping: Ping,
    Commands.Echo: Echo,
}


def fill_database_string_commands(router: RouteParser):
    for command, command_cls in COMMAND_TO_COMMAND_CLS.items():
        create_smart_command_parser(router, command, command_cls)
