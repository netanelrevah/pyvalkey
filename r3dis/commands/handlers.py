from dataclasses import dataclass

from r3dis.commands.context import ClientContext


@dataclass
class CommandHandler:
    command_context: ClientContext

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
        return self.command_context.server_context.acl

    @property
    def clients(self):
        return self.command_context.server_context.clients

    @property
    def current_client(self):
        return self.command_context.current_client

    @property
    def configurations(self):
        return self.command_context.server_context.configurations

    @property
    def information(self):
        return self.command_context.server_context.information
