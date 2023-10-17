from dataclasses import Field, dataclass, fields
from enum import Enum, auto
from typing import Callable

from r3dis.acl import ACL
from r3dis.commands.context import ClientContext
from r3dis.commands.core import Command
from r3dis.configurations import Configurations
from r3dis.databases import Database
from r3dis.information import Information


class DependencyMetadata(Enum):
    DEPENDENCY = auto()


@dataclass
class CommandCreator:
    command_cls: type[Command]
    command_creator: Callable[..., Command]
    dependencies: list[Field]

    def __call__(self, parameters: list[bytes], client_context: ClientContext):
        command_kwargs = self.command_cls.parse(parameters)

        for command_dependency in self.dependencies:
            if command_dependency.type == Database:
                command_kwargs[command_dependency.name] = client_context.database
            elif command_dependency.type == ACL:
                command_kwargs[command_dependency.name] = client_context.server_context.acl
            elif command_dependency.type == ClientContext:
                command_kwargs[command_dependency.name] = client_context
            elif command_dependency.type == Information:
                command_kwargs[command_dependency.name] = client_context.server_context.information
            elif command_dependency.type == Configurations:
                command_kwargs[command_dependency.name] = client_context.server_context.configurations
            else:
                raise TypeError()

        return self.command_creator(**command_kwargs)

    @classmethod
    def create(cls, command_cls: type[Command]):
        command_dependencies = []

        for command_dependency in fields(command_cls):
            if not command_dependency.metadata.get(DependencyMetadata.DEPENDENCY):
                continue

            command_dependencies.append(command_dependency)

        return CommandCreator(command_cls, command_cls, command_dependencies)
