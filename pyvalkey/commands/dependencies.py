from dataclasses import field
from enum import Enum, auto
from typing import Any


class DependencyMetadata(Enum):
    DEPENDENCY = auto()


def dependency() -> Any:  # noqa: ANN401
    return field(metadata={DependencyMetadata.DEPENDENCY: True})
