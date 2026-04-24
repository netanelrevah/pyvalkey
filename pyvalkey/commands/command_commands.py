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


@command(b"count", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandCount(Command):
    """
    summary: Returns a count of commands.
    complexity: O(1)
    since: 2.8.13
    function: commandCountCommand
    reply_schema:
      description: Number of total commands in this server.
      type: integer
    """

    def execute(self) -> ValueType:
        return len(_iter_all_commands())


@command(b"list", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandList(Command):
    """
    summary: Returns a list of command names.
    complexity: O(N) where N is the total number of commands
    since: 7.0.0
    function: commandListCommand
    reply_schema:
      type: array
      items:
        description: Command name.
        type: string
      uniqueItems: true
    """

    filterby: bool = flag_parameter(token=b"FILTERBY")
    filter_type: bytes | None = positional_parameter(default=None)
    filter_value: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return [name for name, _ in _iter_all_commands()]


@command(b"info", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandInfo(Command):
    """
    summary: Returns information about one, multiple or all commands.
    complexity: O(N) where N is the number of commands to look up
    since: 2.8.13
    function: commandInfoCommand
    reply_schema:
      type: array
      items:
        oneOf:
        - description: Command does not exist.
          type: 'null'
        - description: Command info array output.
          type: array
          minItems: 10
          maxItems: 10
          items:
          - description: Command name.
            type: string
          - description: Command arity.
            type: integer
          - description: Command flags.
            type: array
            items:
              description: Command flag.
              type: string
          - description: Command first key index.
            type: integer
          - description: Command last key index.
            type: integer
          - description: Command key step index.
            type: integer
          - description: Command categories.
            type: array
            items:
              description: Command category.
              type: string
          - description: Command tips.
            type: array
            items:
              description: Command tip.
              type: string
          - description: Command key specs.
            type: array
            items:
              type: object
              additionalProperties: false
              properties:
                notes:
                  type: string
                flags:
                  type: array
                  items:
                    type: string
                begin_search:
                  type: object
                  additionalProperties: false
                  properties:
                    type:
                      type: string
                    spec:
                      anyOf:
                      - description: Unknown type, empty map.
                        type: object
                        additionalProperties: false
                      - description: Index type.
                        type: object
                        additionalProperties: false
                        properties:
                          index:
                            type: integer
                      - description: Keyword type.
                        type: object
                        additionalProperties: false
                        properties:
                          keyword:
                            type: string
                          startfrom:
                            type: integer
                find_keys:
                  type: object
                  additionalProperties: false
                  properties:
                    type:
                      type: string
                    spec:
                      anyOf:
                      - description: Unknown type.
                        type: object
                        additionalProperties: false
                      - description: Range type.
                        type: object
                        additionalProperties: false
                        properties:
                          lastkey:
                            type: integer
                          keystep:
                            type: integer
                          limit:
                            type: integer
                      - description: Keynum type.
                        type: object
                        additionalProperties: false
                        properties:
                          keynumidx:
                            type: integer
                          firstkey:
                            type: integer
                          keystep:
                            type: integer
          - type: array
            description: Subcommands.
    """

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


@command(b"docs", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandDocs(Command):
    """
    summary: Returns documentary information about one, multiple or all commands.
    complexity: O(N) where N is the number of commands to look up
    since: 7.0.0
    function: commandDocsCommand
    reply_schema:
      description: >-
        A map where each key is a command name, and each value is the documentary information
      type: object
      additionalProperties: false
      patternProperties:
        ^.*$:
          type: object
          additionalProperties: false
          properties:
            summary:
              description: Short command description.
              type: string
            since:
              description: >-
                The server version that added the command (or for module commands, the module version).
              type: string
            group:
              description: The functional group to which the command belongs.
              oneOf:
              - const: bitmap
              - const: cluster
              - const: connection
              - const: generic
              - const: geo
              - const: hash
              - const: hyperloglog
              - const: list
              - const: module
              - const: pubsub
              - const: scripting
              - const: sentinel
              - const: server
              - const: set
              - const: sorted-set
              - const: stream
              - const: string
              - const: transactions
            complexity:
              description: A short explanation about the command's time complexity.
              type: string
            module:
              type: string
            doc_flags:
              description: An array of documentation flags.
              type: array
              items:
                oneOf:
                - description: The command is deprecated.
                  const: deprecated
                - description: A system command that isn't meant to be called by users.
                  const: syscmd
            deprecated_since:
              description: >-
                The server version that deprecated the command (or for module commands, the module version).
              type: string
            replaced_by:
              description: The alternative for a deprecated command.
              type: string
            history:
              description: >-
                An array of historical notes describing changes to the command's behavior or arguments.
              type: array
              items:
                type: array
                minItems: 2
                maxItems: 2
                items:
                - type: string
                  description: The server version that the entry applies to.
                - type: string
                  description: The description of the change.
            arguments:
              description: An array of maps that describe the command's arguments.
              type: array
              items:
                type: object
                additionalProperties: false
                properties:
                  name:
                    type: string
                  type:
                    type: string
                  display_text:
                    type: string
                  key_spec_index:
                    type: integer
                  token:
                    type: string
                  summary:
                    type: string
                  since:
                    type: string
                  deprecated_since:
                    type: string
                  flags:
                    type: array
                    items:
                      type: string
                  arguments:
                    type: array
            reply_schema:
              description: Command reply schema.
              type: object
            subcommands:
              description: >-
                A map where each key is a subcommand, and each value is the documentary information.
              $ref: '#'
    """

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


@command(b"getkeysandflags", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandGetKeysAndFlags(Command):
    """
    summary: Extracts the key names and access flags for an arbitrary command.
    complexity: O(N) where N is the number of arguments to the command
    since: 7.0.0
    function: commandGetKeysAndFlagsCommand
    reply_schema:
      description: List of keys from the given command and their usage flags.
      type: array
      uniqueItems: true
      items:
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Key name
          type: string
        - description: Set of key flags
          type: array
          minItems: 1
          items:
            type: string
    """

    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        keys_result = CommandGetKeys(self.command, self.args).execute()
        if not isinstance(keys_result, list):
            return keys_result
        return [[key, []] for key in keys_result]


@command(b"help", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: commandHelpCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

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
