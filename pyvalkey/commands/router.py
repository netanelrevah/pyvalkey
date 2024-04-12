from dataclasses import dataclass
from typing import ClassVar

from black.trans import defaultdict

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parsers import server_command
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.errors import RouterKeyError


@dataclass
class ServerCommandsRouter:
    ROUTES: ClassVar[dict[bytes, Command | dict[bytes, Command]]] = defaultdict(dict)

    def internal_route(self, parameters: list[bytes], routes: dict[bytes, Command | dict[bytes, Command]]) -> Command:
        command = parameters.pop(0).lower()

        if command not in routes:
            raise RouterKeyError()

        routed_command = routes[command]

        if isinstance(routed_command, dict):
            return self.internal_route(parameters, routed_command)

        return routed_command

    def route(self, parameters: list[bytes], client_context: ClientContext):
        routed_command: Command = self.internal_route(parameters, self.ROUTES)

        return routed_command.create(parameters, client_context)

    @classmethod
    def command(cls, command: bytes, acl_categories: list[bytes], parent_command: bytes = None):
        def _command_wrapper(command_cls: Command):
            command_cls = server_command(command_cls)

            if not acl_categories:
                raise TypeError("command must have at least one acl_categories")

            for acl_category in acl_categories:
                ACL.CATEGORIES[acl_category].add(command)

            routes = cls.ROUTES
            if parent_command is not None:
                routes = routes[parent_command]
            routes[command] = command_cls

            return command_cls

        return _command_wrapper
