from dataclasses import field

from pyvalkey.commands.creators import DependencyMetadata


def server_command_dependency():
    return field(metadata={DependencyMetadata.DEPENDENCY: True})
