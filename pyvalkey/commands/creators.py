from dataclasses import Field, dataclass, fields
from enum import Enum, auto
from typing import Callable, Self

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.information import Information


class DependencyMetadata(Enum):
    DEPENDENCY = auto()


@dataclass
class CommandCreator:
    command_cls: type[Command]
    command_creator: Callable[..., Command]
    dependencies: list[Field]

    def __call__(self, parameters: list[bytes], client_context: ClientContext) -> Command:
        command_kwargs = self.command_cls.parse(parameters)

        for command_dependency in self.dependencies:
            if command_dependency.type == Database:
                command_kwargs[command_dependency.name] = client_context.database
            elif command_dependency.type == ACL:
                command_kwargs[command_dependency.name] = client_context.server_context.acl
            elif command_dependency.type == ClientContext:
                command_kwargs[command_dependency.name] = client_context
            elif command_dependency.type == ServerContext:
                command_kwargs[command_dependency.name] = client_context.server_context
            elif command_dependency.type == Information:
                command_kwargs[command_dependency.name] = client_context.server_context.information
            elif command_dependency.type == Configurations:
                command_kwargs[command_dependency.name] = client_context.server_context.configurations
            else:
                raise TypeError()

        return self.command_creator(**command_kwargs)

    @classmethod
    def create(cls, command_cls: type[Command]) -> Self:
        command_dependencies = []

        for command_dependency in fields(command_cls):
            if not command_dependency.metadata.get(DependencyMetadata.DEPENDENCY):
                continue

            command_dependencies.append(command_dependency)

        return cls(command_cls, command_cls, command_dependencies)
