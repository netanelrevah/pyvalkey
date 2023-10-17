from dataclasses import field

from r3dis.commands.creators import DependencyMetadata


def redis_command_dependency():
    return field(metadata={DependencyMetadata.DEPENDENCY: True})
