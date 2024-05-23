from dataclasses import field
from typing import Any

from pyvalkey.commands.creators import DependencyMetadata


def server_command_dependency() -> Any:  # noqa: ANN401
    return field(metadata={DependencyMetadata.DEPENDENCY: True})
