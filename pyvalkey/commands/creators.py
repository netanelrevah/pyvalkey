from __future__ import annotations

from collections.abc import Callable
from dataclasses import Field, dataclass, fields
from typing import TYPE_CHECKING, Self, get_type_hints

from pyvalkey.blocking import BlockingManager, ListBlockingManager, SortedSetBlockingManager, StreamBlockingManager
from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.dependencies import DependencyMetadata
from pyvalkey.commands.scripting import ScriptingEngine
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.information import Information
from pyvalkey.notifications import ClientSubscriptions, NotificationsManager, SubscriptionsManager
from pyvalkey.resp import RespProtocolVersion

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command


@dataclass
class CommandCreator:
    command_cls: type[Command]
    command_creator: Callable[..., Command]
    dependencies: list[Field]
    dependencies_types: list[Field]

    def __call__(self, parameters: list[bytes], client_context: ClientContext) -> Command:
        command_kwargs = self.command_cls.parse(parameters)

        for command_dependency, command_dependency_type in zip(self.dependencies, self.dependencies_types):
            if command_dependency_type == Database:
                command_kwargs[command_dependency.name] = client_context.database
            elif command_dependency_type == ACL:
                command_kwargs[command_dependency.name] = client_context.server_context.acl
            elif command_dependency_type == ClientContext:
                command_kwargs[command_dependency.name] = client_context
            elif command_dependency_type == ServerContext:
                command_kwargs[command_dependency.name] = client_context.server_context
            elif command_dependency_type == Information:
                command_kwargs[command_dependency.name] = client_context.server_context.information
            elif command_dependency_type == Configurations:
                command_kwargs[command_dependency.name] = client_context.server_context.configurations
            elif command_dependency_type == RespProtocolVersion:
                command_kwargs[command_dependency.name] = client_context.protocol
            elif command_dependency_type == BlockingManager:
                command_kwargs[command_dependency.name] = client_context.server_context.blocking_manager
            elif command_dependency_type == ListBlockingManager:
                command_kwargs[command_dependency.name] = (
                    client_context.server_context.blocking_manager.list_blocking_manager
                )
            elif command_dependency_type == SortedSetBlockingManager:
                command_kwargs[command_dependency.name] = (
                    client_context.server_context.blocking_manager.sorted_set_blocking_manager
                )
            elif command_dependency_type == StreamBlockingManager:
                command_kwargs[command_dependency.name] = (
                    client_context.server_context.blocking_manager.stream_blocking_manager
                )
            elif command_dependency_type == ScriptingEngine:
                command_kwargs[command_dependency.name] = client_context.scripting_manager
            elif command_dependency_type == NotificationsManager:
                command_kwargs[command_dependency.name] = client_context.notifications_manager
            elif command_dependency_type == ClientSubscriptions:
                command_kwargs[command_dependency.name] = client_context.subscriptions
            elif command_dependency_type == SubscriptionsManager:
                command_kwargs[command_dependency.name] = client_context.server_context.subscriptions_manager
            else:
                raise TypeError()

        return self.command_creator(**command_kwargs)

    @classmethod
    def create(cls, command_cls: type[Command]) -> Self:
        field_types = get_type_hints(command_cls)

        command_dependencies = []
        command_dependencies_types = []
        for command_dependency in fields(command_cls):
            if not command_dependency.metadata.get(DependencyMetadata.DEPENDENCY):
                continue

            command_dependencies.append(command_dependency)
            command_dependencies_types.append(field_types[command_dependency.name])

        return cls(command_cls, command_cls, command_dependencies, command_dependencies_types)
