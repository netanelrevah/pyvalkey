from r3dis.commands.client_context.core import (
    ClientCommand,
    Echo,
    Ping,
    SelectDatabase,
    create_smart_command_parser,
)
from r3dis.commands.context import ClientContext
from r3dis.commands.router import RouteParser
from r3dis.consts import Commands

COMMAND_TO_COMMAND_CLS: dict[Commands, type[ClientCommand]] = {
    Commands.Ping: Ping,
    Commands.Echo: Echo,
    Commands.Select: SelectDatabase,
}


def fill_database_string_commands(router: RouteParser, client_context: ClientContext):
    for command, command_cls in COMMAND_TO_COMMAND_CLS.items():
        create_smart_command_parser(router, command, command_cls, client_context)
