from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from pyvalkey.commands.parsers import CommandMetadata, transform_command
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.errors import RouterKeyError

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command


@dataclass
class CommandsRouter:
    ROUTES: ClassVar[defaultdict[bytes, Any]] = defaultdict(dict)

    def internal_route(
        self,
        parameters: list[bytes],
        routes: dict[bytes, type[Command]] | dict[bytes, type[Command] | dict[bytes, type[Command]]],
    ) -> type[Command]:
        command_name = parameters.pop(0).lower()

        if command_name not in routes:
            raise RouterKeyError()

        routed_command = routes[command_name]

        if isinstance(routed_command, dict):
            return self.internal_route(parameters, routed_command)

        return routed_command

    def route(self, parameters: list[bytes]) -> tuple[type[Command], list[bytes]]:
        parameters = parameters[:]
        routed_command_cls: type[Command] = self.internal_route(parameters, self.ROUTES)
        return routed_command_cls, parameters

    @classmethod
    def command(
        cls,
        command_name: bytes,
        acl_categories: set[bytes],
        parent_command: bytes | None = None,
        flags: set[bytes] | None = None,
        metadata: dict[CommandMetadata, Any] | None = None,
    ) -> Callable[[type[Command]], type[Command]]:
        def _command_wrapper(command_cls: type[Command]) -> type[Command]:
            command_cls = transform_command(command_cls, metadata)

            if not acl_categories:
                raise TypeError("command must have at least one acl_categories")

            for flag in flags or []:
                if flag in [b"write"]:
                    acl_categories.add(b"write")

            setattr(command_cls, "flags", set(flags or []))

            full_command_name = parent_command + b"|" + command_name if parent_command else command_name

            setattr(command_cls, "full_command_name", full_command_name)

            if full_command_name in ACL.COMMANDS_NAMES:
                raise ValueError("redecalration of command")

            ACL.COMMAND_CATEGORIES[full_command_name] = acl_categories
            for acl_category in acl_categories:
                ACL.CATEGORIES[acl_category].add(command_name)

            ACL.COMMANDS_NAMES[command_cls] = full_command_name

            if parent_command is not None:
                sub_route = cls.ROUTES[parent_command]
                sub_route[command_name] = command_cls
            else:
                cls.ROUTES[command_name] = command_cls

            return command_cls

        return _command_wrapper


command = CommandsRouter.command
