from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, TypeVar, dataclass_transform

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext


class DependencyMetadata(Enum):
    DEPENDENCY = auto()


T = TypeVar("T")


@dataclass_transform()
def dependency_registerer(cls: type[T]) -> type[T]:
    cls = dataclass(cls)
    setattr(cls, "RESOLVERS", {})

    def register_dependency(_cls: type, resolver: Callable[["ClientContext"], Any]) -> Callable[[type], type]:
        def decorator(dependency_type: type) -> type:
            getattr(_cls, "RESOLVERS")[dependency_type] = resolver
            return dependency_type

        return decorator

    setattr(cls, "register_dependency", classmethod(register_dependency))
    return cls


def registered_as_dependency_for(
    registry_cls: Any,  # noqa: ANN401
    resolver: Callable[["ClientContext"], Any],
) -> Callable[[type[T]], type[T]]:
    def decorator(cls: type[T]) -> type[T]:
        getattr(registry_cls, "RESOLVERS")[cls] = resolver
        return cls

    return decorator


def dependency() -> Any:  # noqa: ANN401
    return field(metadata={DependencyMetadata.DEPENDENCY: True})
