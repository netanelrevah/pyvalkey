from __future__ import annotations

from typing import TYPE_CHECKING

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import flag_parameter, positional_parameter
from pyvalkey.commands.router import CommandsRouter, command
from pyvalkey.commands.server_commands import CommandGetKeys
from pyvalkey.database_objects.acl import ACL

if TYPE_CHECKING:
    from pyvalkey.resp import ValueType


def _iter_all_commands() -> list[tuple[bytes, type[Command]]]:
    results: list[tuple[bytes, type[Command]]] = []
    for name, entry in CommandsRouter.ROUTES.items():
        if isinstance(entry, dict):
            for sub_name, sub_cls in entry.items():
                results.append((name + b"|" + sub_name, sub_cls))
        else:
            results.append((name, entry))
    return results


def _command_info(name: bytes, command_cls: type[Command]) -> list[ValueType]:
    flags = list(command_cls.flags) if hasattr(command_cls, "flags") else []
    acl_categories = ACL.COMMAND_CATEGORIES.get(name, set())
    return [
        name,
        -1,
        flags,
        0,
        0,
        0,
        sorted([b"@" + c for c in acl_categories]),
        [],
        [],
        [],
    ]


@command(b"count", {b"connection", b"slow"}, b"command")
class CommandCount(Command):
    def execute(self) -> ValueType:
        return len(_iter_all_commands())


@command(b"list", {b"connection", b"slow"}, b"command")
class CommandList(Command):
    filterby: bool = flag_parameter(token=b"FILTERBY")
    filter_type: bytes | None = positional_parameter(default=None)
    filter_value: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return [name for name, _ in _iter_all_commands()]


@command(b"info", {b"connection", b"slow"}, b"command")
class CommandInfo(Command):
    command_names: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        all_commands = dict(_iter_all_commands())
        if not self.command_names:
            return [_command_info(name, cls) for name, cls in all_commands.items()]
        results: list[ValueType] = []
        for name in self.command_names:
            key = name.lower()
            if key in all_commands:
                results.append(_command_info(key, all_commands[key]))
            else:
                results.append(None)
        return results


@command(b"docs", {b"connection", b"slow"}, b"command")
class CommandDocs(Command):
    command_names: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        all_commands = dict(_iter_all_commands())
        names = [n.lower() for n in self.command_names] if self.command_names else list(all_commands.keys())
        result: list[ValueType] = []
        for name in names:
            if name not in all_commands:
                continue
            result.append(name)
            result.append([b"summary", b"", b"since", b"1.0.0", b"group", b"generic"])
        return result


@command(b"getkeysandflags", {b"connection", b"slow"}, b"command")
class CommandGetKeysAndFlags(Command):
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        keys_result = CommandGetKeys(self.command, self.args).execute()
        if not isinstance(keys_result, list):
            return keys_result
        return [[key, []] for key in keys_result]


@command(b"help", {b"connection", b"slow"}, b"command")
class CommandHelp(Command):
    def execute(self) -> ValueType:
        return [
            b"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"(no subcommand)",
            b"    Return details about all commands.",
            b"COUNT",
            b"    Return the total number of commands in this server.",
            b"DOCS  [<command-name> ...]",
            b"    Return documentary information about commands.",
            b"GETKEYS <full-command>",
            b"    Return the keys from a full command.",
            b"GETKEYSANDFLAGS <full-command>",
            b"    Return the keys and the access flags from a full command.",
            b"INFO [<command-name> ...]",
            b"    Return details about multiple commands.",
            b"LIST [FILTERBY (MODULE <module-name>|ACLCAT <category>|PATTERN <pattern>)]",
            b"    Return a list of all commands in this server.",
            b"HELP",
            b"    Print this help.",
        ]
