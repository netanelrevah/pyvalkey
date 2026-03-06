from __future__ import annotations

from dataclasses import fields
from typing import TYPE_CHECKING, Any, ClassVar, Self, get_type_hints

from pyvalkey.commands.dependencies import DependencyMetadata, dependency_registerer

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.core import Command


@dependency_registerer
class CommandCreator:
    command_cls: type[Command]
    command_creator: Callable[..., Command]
    dependencies: list[Any]  # qa: ANN401
    dependencies_types: list[Any]  # qa: ANN401

    if TYPE_CHECKING:
        RESOLVERS: ClassVar[dict[type, Callable[[ClientContext], Any]]]

    def __call__(self, parameters: list[bytes], client_context: ClientContext) -> Command:
        command_kwargs = self.command_cls.parse(parameters)

        for command_dependency, command_dependency_type in zip(self.dependencies, self.dependencies_types):
            resolver = self.RESOLVERS.get(command_dependency_type)
            if resolver is None:
                raise TypeError(f"Unregistered dependency type: {command_dependency_type}")
            command_kwargs[command_dependency.name] = resolver(client_context)

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
