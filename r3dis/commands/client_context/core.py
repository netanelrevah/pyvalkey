from dataclasses import dataclass
from functools import partial

from r3dis.commands.context import ClientContext
from r3dis.commands.core import Command
from r3dis.commands.parsers import SmartCommandParser, redis_positional_parameter
from r3dis.consts import Commands
from r3dis.resp import RESP_OK


@dataclass
class ClientCommand(Command):
    client_context: ClientContext

    def execute(self):
        raise NotImplementedError()


@dataclass
class SelectDatabase(ClientCommand):
    index: int = redis_positional_parameter()

    def execute(self):
        self.client_context.current_database = self.index
        return RESP_OK


def create_smart_command_parser(
    router, command: Commands, database_command_cls: type[ClientCommand], client_context: ClientContext, *args, **kwargs
):
    router.routes[command] = SmartCommandParser(
        database_command_cls, partial(database_command_cls, client_context, *args, **kwargs)
    )
