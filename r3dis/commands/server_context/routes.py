from r3dis.commands.context import ServerContext
from r3dis.commands.router import RouteParser
from r3dis.commands.server_context.core import (
    Authorize,
    ServerCommand,
    create_smart_command_parser,
)
from r3dis.consts import Commands

COMMAND_TO_COMMAND_CLS: dict[Commands, type[ServerCommand]] = {
    Commands.Authorize: Authorize,
}


def fill_database_string_commands(router: RouteParser, server_context: ServerContext):
    for command, command_cls in COMMAND_TO_COMMAND_CLS.items():
        create_smart_command_parser(router, command, command_cls, server_context)
