from __future__ import annotations

from collections.abc import Callable
from dataclasses import Field, dataclass, field, fields, is_dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Self, TypeVar, dataclass_transform, get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable


DependencyRegistererType = TypeVar("DependencyRegistererType")


class DependencyRegisterer:
    RESOLVERS: ClassVar[dict[type, Callable[..., Any]]]


class DependencyMetadata(Enum):
    DEPENDENCY = auto()


@dataclass_transform()
def dependency_registerer(cls: type[DependencyRegistererType]) -> type[DependencyRegistererType]:
    if not is_dataclass(cls):
        cls = dataclass(cls)
    setattr(cls, "RESOLVERS", {})

    def register_dependency(_cls: type, dependency_type: type, resolver: Callable[..., Any]) -> Callable[[type], type]:
        getattr(_cls, "RESOLVERS")[dependency_type] = resolver
        return dependency_type

    setattr(cls, "register_dependency", classmethod(register_dependency))
    return cls


def registered_as_dependency_for(
    registry_cls: Any,  # noqa: ANN401
    resolver: Callable[..., Any],
) -> Callable[[type[DependencyRegistererType]], type[DependencyRegistererType]]:
    def decorator(cls: type[DependencyRegistererType]) -> type[DependencyRegistererType]:
        getattr(registry_cls, "RESOLVERS")[cls] = resolver
        return cls

    return decorator


def dependency() -> Any:  # noqa: ANN401
    return field(metadata={DependencyMetadata.DEPENDENCY: True})


T = TypeVar("T")


def collect_dependencies(cls: type[T], field_types: Any) -> tuple[list[Field[Any]], list[Any]]:  # noqa: ANN401
    if not is_dataclass(cls):
        raise TypeError()

    command_dependencies = []
    command_dependencies_types = []
    for command_dependency in fields(cls):
        if not command_dependency.metadata.get(DependencyMetadata.DEPENDENCY):
            continue

        command_dependencies.append(command_dependency)
        command_dependencies_types.append(field_types[command_dependency.name])

    return command_dependencies, command_dependencies_types


@dataclass
class DependenciesInjector(Generic[T]):
    dependencies: list[Field[Any]]
    dependencies_types: list[Any]

    if TYPE_CHECKING:
        RESOLVERS: ClassVar[dict[type, Callable[..., Any]]]

    def __call__(self, kwargs: dict[str, Any]) -> None:
        for command_dependency, command_dependency_type in zip(self.dependencies, self.dependencies_types):
            resolver = self.RESOLVERS.get(command_dependency_type)
            if resolver is None:
                raise TypeError(f"Unregistered dependency type: {command_dependency_type}")
            kwargs[command_dependency.name] = resolver(**kwargs)

    @classmethod
    def create(cls, created_cls: type[T]) -> Self:
        type_hints = get_type_hints(created_cls, localns={d.__name__: d for d in cls.RESOLVERS.keys()})
        command_dependencies, command_dependencies_types = collect_dependencies(created_cls, type_hints)
        return cls(command_dependencies, command_dependencies_types)


def get_type_hints_with_dependencies(created_cls: type[T], creator_cls: type[DependencyRegisterer]) -> dict:
    return get_type_hints(created_cls, localns={d.__name__: d for d in creator_cls.RESOLVERS.keys()})
