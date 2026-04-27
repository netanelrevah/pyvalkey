from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self, get_type_hints

from pyvalkey.utils.dependencies import collect_dependencies, dependency_registerer

if TYPE_CHECKING:
    from collections.abc import Callable
    from dataclasses import Field

    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.core import Command


@dependency_registerer
class CommandCreator:
    command_cls: type[Command]
    dependencies: list[Field[Any]]
    dependencies_types: list[Any]

    if TYPE_CHECKING:
        RESOLVERS: ClassVar[dict[type, Callable[[ClientContext], Any]]]

        @classmethod
        def register_dependency(cls, dependency_cls: type, resolver: Callable[..., Any]) -> Callable[[type], type]:
            pass

    def __call__(self, parameters: list[bytes], client_context: ClientContext) -> Command:
        command_kwargs = self.command_cls.parse(parameters)

        for command_dependency, command_dependency_type in zip(self.dependencies, self.dependencies_types):
            resolver = self.RESOLVERS.get(command_dependency_type)
            if resolver is None:
                raise TypeError(f"Unregistered dependency type: {command_dependency_type}")
            command_kwargs[command_dependency.name] = resolver(client_context)

        return self.command_cls(**command_kwargs)

    @classmethod
    def create(cls, command_cls: type[Command]) -> Self:
        command_dependencies, command_dependencies_types = collect_dependencies(
            command_cls, get_type_hints(command_cls, localns={d.__name__: d for d in cls.RESOLVERS.keys()})
        )
        return cls(command_cls, command_dependencies, command_dependencies_types)
