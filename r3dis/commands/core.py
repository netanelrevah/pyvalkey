from collections import defaultdict
from dataclasses import dataclass

from r3dis.acl import ACL
from r3dis.databases import Database


@dataclass
class CommandContext:
    databases: defaultdict[int, Database]
    acl: ACL

    current_database: int = 0
    current_user: bytes = None

    @property
    def database(self):
        return self.databases[self.current_database]


@dataclass
class CommandHandler:
    command_context: CommandContext

    def execute(self, parameters: list[bytes]):
        parsed_parameters = self.parse(parameters)
        return self.handle(*parsed_parameters)

    def handle(self, *args):
        raise NotImplementedError()

    def parse(self, parameters: list[bytes]):
        raise NotImplementedError()

    @property
    def database(self):
        return self.command_context.database

    @property
    def acl(self):
        return self.command_context.acl
