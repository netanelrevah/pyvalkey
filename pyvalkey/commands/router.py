from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from pyvalkey.commands.parsers import server_command
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.errors import RouterKeyError

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.core import Command


@dataclass
class ServerCommandsRouter:
    ROUTES: ClassVar[defaultdict[bytes, Any]] = defaultdict(dict)

    def internal_route(
        self,
        parameters: list[bytes],
        routes: dict[bytes, type[Command]] | dict[bytes, type[Command] | dict[bytes, type[Command]]],
    ) -> type[Command]:
        command = parameters.pop(0).lower()

        if command not in routes:
            raise RouterKeyError()

        routed_command = routes[command]

        if isinstance(routed_command, dict):
            return self.internal_route(parameters, routed_command)

        return routed_command

    def route(self, parameters: list[bytes], client_context: ClientContext) -> Command:
        routed_command: type[Command] = self.internal_route(parameters, self.ROUTES)

        return routed_command.create(parameters, client_context)

    @classmethod
    def command(
        cls, command: bytes, acl_categories: list[bytes], parent_command: bytes | None = None
    ) -> Callable[[type[Command]], type[Command]]:
        def _command_wrapper(command_cls: type[Command]) -> type[Command]:
            command_cls = server_command(command_cls)

            if not acl_categories:
                raise TypeError("command must have at least one acl_categories")

            command_name = parent_command + b"|" + command if parent_command else command

            ACL.COMMAND_CATEGORIES[command_name] = set(acl_categories)
            for acl_category in acl_categories:
                ACL.CATEGORIES[acl_category].add(command)

            ACL.COMMANDS_NAMES[command_cls] = command_name

            if parent_command is not None:
                sub_route = cls.ROUTES[parent_command]
                sub_route[command] = command_cls
            else:
                cls.ROUTES[command] = command_cls

            return command_cls

        return _command_wrapper
