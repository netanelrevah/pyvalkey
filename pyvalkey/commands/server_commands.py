from __future__ import annotations

import sys
import time
from dataclasses import field, fields
from os import urandom
from typing import TYPE_CHECKING

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import ParameterMetadata, flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.router import CommandsRouter, command
from pyvalkey.database_objects.acl import ACL, ACLUser, CommandRule, KeyPattern, Permission
from pyvalkey.database_objects.configurations import ConfigurationError, Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, RespError, ValueType
from pyvalkey.utils.dependencies import dependency

if TYPE_CHECKING:
    from pyvalkey.blocking import BlockingManager, StreamBlockingManager
    from pyvalkey.commands.context import ClientContext, ServerContext
    from pyvalkey.database_objects.databases import Database
    from pyvalkey.database_objects.information import Information


@command(b"help", {b"slow"}, b"acl", flags={b"loading", b"sentinel", b"stale"})
class AclHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 6.0.0
    function: aclCommand
    reply_schema:
      type: array
      description: A list of subcommands and their description.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return ["genpass"]


@command(b"genpass", {b"slow"}, b"acl", flags={b"loading", b"noscript", b"sentinel", b"stale"})
class AclGeneratePassword(Command):
    """
    summary: >-
      Generates a pseudorandom, secure password that can be used to identify ACL users.
    complexity: O(1)
    since: 6.0.0
    function: aclCommand
    reply_schema:
      type: string
      description: >-
        Pseudorandom data. By default it contains 64 bytes, representing 256 bits of data. If `bits` was given,
        the output string length is the number of specified bits (rounded to the next multiple of 4) divided by
        4.
    """

    length: int = positional_parameter(default=64)

    def execute(self) -> ValueType:
        return urandom(self.length)


@command(b"cat", {b"slow"}, b"acl", flags={b"loading", b"noscript", b"sentinel", b"stale"})
class AclCategory(Command):
    """
    summary: Lists the ACL categories, or the commands inside a category.
    complexity: O(1) since the categories and commands are a fixed set.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      anyOf:
      - type: array
        description: In case `category` was not given, a list of existing ACL categories
        items:
          type: string
      - type: array
        description: >-
          In case `category` was given, list of commands that fall under the provided ACL category.
        items:
          type: string
    """

    category: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.category is not None:
            return ACL.get_category_commands(self.category)
        return ACL.get_categories()


@command(
    b"deluser",
    {b"admin", b"dangerous", b"slow"},
    b"acl",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class AclDeleteUser(Command):
    """
    summary: Deletes ACL users, and terminates their connections.
    complexity: O(1) amortized time considering the typical user.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      type: integer
      description: The number of users that were deleted.
    """

    acl: ACL = dependency()

    user_names: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        user_deleted = 0
        for user_name in self.user_names:
            if user_name == b"default":
                pass
            user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
        return user_deleted


@command(
    b"getuser",
    {b"admin", b"dangerous", b"slow"},
    b"acl",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class AclGetUser(Command):
    """
    summary: Lists the ACL rules of a user.
    complexity: >-
      O(N). Where N is the number of password, command and pattern rules that the user has.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      oneOf:
      - description: A set of ACL rule definitions for the user.
        type: object
        additionalProperties: false
        properties:
          flags:
            type: array
            items:
              type: string
          passwords:
            type: array
            items:
              type: string
          commands:
            description: Root selector's commands.
            type: string
          keys:
            description: Root selector's keys.
            type: string
          channels:
            description: Root selector's channels.
            type: string
          databases:
            description: Root selector's databases.
            type: string
          selectors:
            type: array
            items:
              type: object
              additionalProperties: false
              properties:
                commands:
                  type: string
                keys:
                  type: string
                channels:
                  type: string
                databases:
                  type: string
      - description: If user does not exist
        type: 'null'
    """

    acl: ACL = dependency()
    user_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.user_name not in self.acl:
            return None
        return self.acl[self.user_name].info


@command(
    b"dryrun",
    {b"admin", b"dangerous", b"slow"},
    b"acl",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class AclDryRun(Command):
    """
    summary: Simulates the execution of a command by a user, without executing the command.
    complexity: O(1).
    since: 7.0.0
    function: aclCommand
    reply_schema:
      anyOf:
      - const: OK
        description: The given user may successfully execute the given command.
      - type: string
        description: >-
          The description of the problem, in case the user is not allowed to run the given command.
    """

    acl: ACL = dependency()

    username: bytes = positional_parameter()
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(
    b"setuser",
    {b"admin", b"dangerous", b"slow"},
    b"acl",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class AclSetUser(Command):
    """
    summary: Creates and modifies an ACL user and its rules.
    complexity: O(N). Where N is the number of rules provided.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      const: OK
    """

    acl: ACL = dependency()
    user_name: bytes = positional_parameter()
    rules: list[bytes] = positional_parameter()

    def parse_selector(self, selector: list[bytes], permission: Permission | None = None) -> Permission:
        permission = permission or Permission()
        for rule in selector:
            if rule == b"allcommands":
                rule = b"+@all"
            if rule in {b"+@all", b"-@all"}:
                permission.command_rules.clear()
            if rule.startswith(b"-") or rule.startswith(b"+"):
                permission.command_rules.append(CommandRule.create(rule))
                continue
            if rule == b"allkeys":
                rule = b"~*"
            if rule.startswith(b"%R~") or rule.startswith(b"%RW~") or rule.startswith(b"%W~") or rule.startswith(b"~"):
                if KeyPattern.create(b"~*") in permission.keys_patterns:
                    raise ValueError(
                        b"Adding a pattern after the * pattern (or the 'allkeys' flag)"
                        b" is not valid and does not have any effect."
                        b" Try 'resetkeys' to start with an empty list of patterns"
                    )
                permission.keys_patterns.add(KeyPattern.create(rule))
                continue
            if rule == b"allchannels":
                rule = b"&*"
            if rule.startswith(b"&"):
                if b"&*" in permission.channel_rules:
                    raise ValueError(
                        b"Adding a pattern after the * pattern (or the 'allchannels' flag)"
                        b" is not valid and does not have any effect."
                        b" Try 'resetchannels' to start with an empty list of channels"
                    )
                permission.channel_rules.add(rule)
                continue

            raise ValueError(b"Syntax error")
        return permission

    def execute(self) -> ValueType:
        callbacks = []

        root_permission_role = []
        while self.rules:
            rule = self.rules.pop(0)
            if rule == b"resetkeys":
                callbacks.append(ACLUser.reset_keys)
                continue
            if rule == b"reset":
                callbacks.append(ACLUser.reset)
                continue
            if rule == b"clearselectors":
                callbacks.append(ACLUser.clear_selectors)
                continue
            if rule == b"on":
                callbacks.append(lambda _acl_user: setattr(_acl_user, "is_active", True))  # type: ignore[arg-type]
                continue
            if rule == b"off":
                callbacks.append(lambda _acl_user: setattr(_acl_user, "is_active", False))  # type: ignore[arg-type]
                continue
            if rule == b"nopass":
                callbacks.append(ACLUser.no_password)
                continue
            if rule.startswith(b">"):
                callbacks.append(lambda _acl_user, password=rule[1:]: _acl_user.add_password(password))  # type: ignore[misc]
                continue
            if rule.startswith(b"("):
                full_rule = rule
                while not full_rule.endswith(b")"):
                    try:
                        full_rule += b" " + self.rules.pop(0)
                    except IndexError:
                        raise ServerError(b"ERR Unmatched parenthesis in acl selector starting at '" + rule + b"'.")

                try:
                    selector = self.parse_selector(full_rule[1:-1].split())
                except ValueError as e:
                    raise ServerError(b"ERR Error in ACL SETUSER modifier '" + full_rule + b"': " + e.args[0])
                callbacks.append(lambda _acl_user, s=selector: _acl_user.selectors.append(s))  # type: ignore[misc]
                continue
            if rule.startswith(b"-@") or rule.startswith(b"+@") or rule.startswith(b"+") or rule.startswith(b"-"):
                root_permission_role.append(rule)
                continue
            if rule.startswith(b"~"):
                root_permission_role.append(rule)
                continue
            if rule.startswith(b"%"):
                root_permission_role.append(rule)
                continue
            raise ServerError(b"ERR Error in ACL SETUSER modifier '" + rule + b"': Syntax error")

        acl_user: ACLUser = self.acl.get_or_create_user(self.user_name)
        for callback in callbacks:
            callback(acl_user)
        if root_permission_role:
            self.parse_selector(root_permission_role, acl_user.root_permissions)
        return RESP_OK


@command(b"getkeys", {b"connection", b"slow"}, b"command", flags={b"loading", b"sentinel", b"stale"})
class CommandGetKeys(Command):
    """
    summary: Extracts the key names from an arbitrary command.
    complexity: O(N) where N is the number of arguments to the command
    since: 2.8.13
    function: commandGetKeysCommand
    reply_schema:
      description: List of keys from the given command.
      type: array
      items:
        type: string
      uniqueItems: true
    """

    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        parameters = [self.command, *self.args]
        command_cls: type[Command] = CommandsRouter().internal_route(
            parameters=parameters, routes=CommandsRouter.ROUTES
        )
        parsed_command = command_cls.parse(parameters)

        keys = command_cls.collect_key_arguments(parsed_command)
        if keys is not None:
            return keys

        keys = []
        for cls_field in fields(command_cls):
            if ParameterMetadata.KEY_MODE in cls_field.metadata:
                if isinstance(parsed_command[cls_field.name], list):
                    keys.extend(parsed_command[cls_field.name])
                else:
                    keys.append(parsed_command[cls_field.name])

        if not keys:
            return RespError(b"The command has no key arguments")

        return keys


@command(b"get", {b"admin", b"dangerous", b"slow"}, b"config", flags={b"admin", b"loading", b"noscript", b"stale"})
class ConfigGet(Command):
    """
    summary: Returns the effective values of configuration parameters.
    complexity: O(N) when N is the number of configuration parameters provided
    since: 2.0.0
    function: configGetCommand
    reply_schema:
      type: object
      additionalProperties:
        type: string
    """

    configurations: Configurations = dependency()
    parameters: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        names = self.configurations.get_names(*self.parameters)
        return self.configurations.info(names)


@command(b"set", {b"admin", b"dangerous", b"slow"}, b"config", flags={b"admin", b"loading", b"noscript", b"stale"})
class ConfigSet(Command):
    """
    summary: Sets configuration parameters in-flight.
    complexity: O(N) when N is the number of configuration parameters provided
    since: 2.0.0
    function: configSetCommand
    reply_schema:
      const: OK
    """

    configurations: Configurations = dependency()
    parameters_values: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        for name, value in self.parameters_values:
            try:
                self.configurations.set_value(name, value)
            except ConfigurationError as e:
                return RespError(
                    (f"ERR CONFIG SET failed (possibly related to argument '{name.decode()}') - " + e.args[0]).encode()
                )

        return RESP_OK


@command(
    b"resetstat", {b"admin", b"dangerous", b"slow"}, b"config", flags={b"admin", b"loading", b"noscript", b"stale"}
)
class ConfigResetStatistics(Command):
    """
    summary: Resets the server's statistics.
    complexity: O(1)
    since: 2.0.0
    function: configResetStatCommand
    reply_schema:
      const: OK
    """

    configurations: Configurations = dependency()
    information: Information = dependency()

    def execute(self) -> ValueType:
        self.information.reset_stats()
        return RESP_OK


@command(b"dbsize", {b"keyspace", b"read"}, flags={b"fast", b"readonly"})
class DatabaseSize(Command):
    """
    summary: Returns the number of keys in the database.
    complexity: O(1)
    since: 1.0.0
    function: dbsizeCommand
    reply_schema:
      type: integer
      description: The number of keys in the currently-selected database.
    """

    database: Database = dependency()

    def execute(self) -> ValueType:
        return self.database.size()


@command(b"set-active-expire", acl_categories={b"fast", b"connection"}, parent_command=b"debug")
class DebugSetActiveExpire(Command):
    set_active_expire: int = keyword_parameter(default=b"0")
    object: bytes | None = keyword_parameter(token=b"object", default=None)

    def execute(self) -> ValueType:
        return True


@command(b"log", acl_categories={b"fast", b"connection"}, parent_command=b"debug")
class Debug(Command):
    message: bytes = keyword_parameter()
    server_context: ServerContext = dependency()

    def execute(self) -> ValueType:
        if self.server_context.num_of_blocked_clients() > 0:
            raise Exception()

        print((b"\n===========\n" + self.message.strip() + b"\n===========\n").decode())

        return RESP_OK


def touch_all_databases_watched_keys(databases: dict[int, Database]) -> None:
    for database in databases.values():
        database.touch_all_database_watched_keys()


@command(b"flushall", {b"dangerous", b"keyspace", b"slow"}, flags={b"all_dbs", b"write"})
class FlushAllDatabases(Command):
    """
    summary: Removes all keys from all databases.
    complexity: O(N) where N is the total number of keys in all databases
    since: 1.0.0
    function: flushallCommand
    reply_schema:
      const: OK
    """

    server_context: ServerContext = dependency()
    blocking_manager: StreamBlockingManager = dependency()
    configurations: Configurations = dependency()

    def execute(self) -> ValueType:
        touch_all_databases_watched_keys(self.server_context.databases)
        self.server_context.databases.clear()
        self.configurations.busy_reply_threshold = 5000
        return RESP_OK

    async def after(self, in_multi: bool = False) -> None:
        for key in self.blocking_manager.notifications.mapping.keys():
            await self.blocking_manager.notify_deleted(key, in_multi=in_multi)


@command(b"flushdb", {b"dangerous", b"keyspace", b"slow"}, flags={b"write"})
class FlushDatabase(Command):
    """
    summary: Removes all keys from the current database.
    complexity: O(N) where N is the number of keys in the selected database
    since: 1.0.0
    function: flushdbCommand
    reply_schema:
      const: OK
    """

    blocking_manager: StreamBlockingManager = dependency()
    client_context: ClientContext = dependency()
    information: Information = dependency()

    async_: bool = flag_parameter(token=b"async")

    _flushed_keys: set[bytes] = field(default_factory=set, init=False)

    def execute(self) -> ValueType:
        if self.client_context.current_database in self.client_context.server_context.databases:
            self._flushed_keys.update(self.client_context.database.keys())
            for key in self.client_context.database.keys():
                self.client_context.database.touch_watched_key(key)
                if self.async_:
                    self.information.lazyfreed_objects += 1
            self.client_context.server_context.databases.pop(self.client_context.current_database)
        return RESP_OK

    async def after(self, in_multi: bool = False) -> None:
        for key in self._flushed_keys:
            await self.blocking_manager.notify(key, in_multi=in_multi)


@command(b"info", {b"dangerous", b"slow"}, flags={b"loading", b"sentinel", b"stale"})
class GetInformation(Command):
    """
    summary: Returns information and statistics about the server.
    complexity: O(1)
    since: 1.0.0
    function: infoCommand
    reply_schema:
      description: >-
        A map of info fields, one field per line in the form of <field>:<value> where the value can be a comma separated
        map like <key>=<val>. Also contains section header lines starting with `#` and blank lines.
      type: string
    """

    information: Information = dependency()

    section: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return self.information.sections(self.section)


@command(b"help", {b"slow"}, parent_command=b"memory", flags={b"loading", b"stale"})
class MemoryHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 4.0.0
    function: memoryCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"USAGE <key>",
            b"    Return the internal memory usage for the Redis object associated with the <key>.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"usage", {b"read", b"slow"}, parent_command=b"memory", flags={b"readonly"})
class MemoryUsage(Command):
    """
    summary: Estimates the memory usage of a key.
    complexity: O(N) where N is the number of samples.
    since: 4.0.0
    function: memoryCommand
    reply_schema:
      oneOf:
      - description: Number of bytes that a key and its value require to be stored in RAM.
        type: integer
      - description: Key does not exist.
        type: 'null'
    """

    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return 1


@command(b"swapdb", {b"dangerous", b"keyspace"}, flags={b"fast", b"write"})
class SwapDb(Command):
    """
    summary: Swaps two databases.
    complexity: >-
      O(N) where N is the count of clients watching or blocking on keys from both databases.
    since: 4.0.0
    function: swapdbCommand
    reply_schema:
      const: OK
    """

    server_context: ServerContext = dependency()
    blocking_manager: BlockingManager = dependency()

    index1: int = positional_parameter(parse_error=b"ERR invalid first DB index")
    index2: int = positional_parameter(parse_error=b"ERR invalid second DB index")

    def execute(self) -> ValueType:
        if self.index1 == self.index2:
            return RESP_OK

        database1 = self.server_context.get_or_create_database(self.index1)
        database1.touch_all_database_watched_keys()
        if self.index2 not in self.server_context.databases:
            new_database = self.server_context.get_or_create_database(self.index2)
            new_database.content = self.server_context.databases.pop(self.index1).content
            return RESP_OK

        database2 = self.server_context.databases[self.index2]
        database2.touch_all_database_watched_keys()

        content1, content2 = (database1.content, database2.content)

        database1.replace_content(content2)
        database2.replace_content(content1)

        database1.touch_all_database_watched_keys()
        database2.touch_all_database_watched_keys()

        return RESP_OK

    async def after(self, in_multi: bool = False) -> None:
        if self.index1 in self.server_context.databases:
            await self.blocking_manager.notify_safely_all(self.server_context.databases[self.index1], in_multi=in_multi)
        await self.blocking_manager.notify_safely_all(self.server_context.databases[self.index2], in_multi=in_multi)


@command(b"sync", {b"admin", b"dangerous", b"slow"}, flags={b"admin", b"noscript", b"no_async_loading", b"no_multi"})
class Sync(Command):
    """
    summary: An internal command used in replication.
    since: 1.0.0
    function: syncCommand
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"time", {b"fast"}, flags={b"fast", b"loading", b"stale"})
class Time(Command):
    """
    summary: Returns the server time.
    complexity: O(1)
    since: 2.6.0
    function: timeCommand
    reply_schema:
      type: array
      description: 'Array containing two elements: Unix time in seconds and microseconds.'
      minItems: 2
      maxItems: 2
      items:
        type: string
        pattern: '[0-9]+'
    """

    def execute(self) -> ValueType:
        now = time.time_ns()
        return [str(now // 1_000_000_000).encode(), str((now // 1_000) % 1_000_000).encode()]


@command(b"role", {b"admin", b"dangerous"}, flags={b"fast", b"loading", b"noscript", b"sentinel", b"stale"})
class Role(Command):
    """
    summary: Returns the replication role.
    complexity: O(1)
    since: 2.8.12
    function: roleCommand
    reply_schema:
      oneOf:
      - type: array
        minItems: 3
        maxItems: 3
        items:
        - const: master
        - description: Current replication primary offset.
          type: integer
        - description: Connected replicas.
          type: array
          items:
            type: array
            minItems: 3
            maxItems: 3
            items:
            - description: Replica IP.
              type: string
            - description: Replica port.
              type: string
            - description: Last acknowledged replication offset.
              type: string
      - type: array
        minItems: 5
        maxItems: 5
        items:
        - const: slave
        - description: IP of primary.
          type: string
        - description: Port number of primary.
          type: integer
        - description: State of the replication from the point of view of the primary.
          oneOf:
          - description: The instance is in handshake with its primary.
            const: handshake
          - description: The instance in not active.
            const: none
          - description: The instance needs to connect to its primary.
            const: connect
          - description: The primary-replica connection is in progress.
            const: connecting
          - description: The primary and replica are trying to perform the synchronization.
            const: sync
          - description: The replica is online.
            const: connected
          - description: Instance state is unknown.
            const: unknown
        - description: >-
            The amount of data received from the replica so far in terms of primary replication offset.
          type: integer
      - type: array
        minItems: 2
        maxItems: 2
        items:
        - const: sentinel
        - description: List of primary names monitored by this sentinel instance.
          type: array
          items:
            type: string
    """

    def execute(self) -> ValueType:
        return [b"master", 0, []]


@command(b"lolwut", {b"read"}, flags={b"fast", b"readonly"})
class Lolwut(Command):
    """
    summary: Displays computer art and the server version.
    since: 5.0.0
    function: lolwutCommand
    reply_schema:
      type: string
      description: >-
        String containing the generative computer art, and a text with the server version.
    """

    version: bytes | None = keyword_parameter(flag=b"VERSION", default=None)

    def execute(self) -> ValueType:
        return b"Valkey ver. 255.255.255\n"


@command(b"lastsave", {b"admin", b"dangerous"}, flags={b"fast", b"loading", b"stale"})
class LastSave(Command):
    """
    summary: Returns the Unix timestamp of the last successful save to disk.
    complexity: O(1)
    since: 1.0.0
    function: lastsaveCommand
    reply_schema:
      type: integer
      description: UNIX TIME of the last DB save executed with success.
    """

    information: Information = dependency()

    def execute(self) -> ValueType:
        return int(self.information.start_time // 1000)


@command(b"save", {b"admin", b"dangerous", b"slow"}, flags={b"admin", b"noscript", b"no_async_loading", b"no_multi"})
class Save(Command):
    """
    summary: Synchronously saves the database(s) to disk.
    complexity: O(N) where N is the total number of keys in all databases
    since: 1.0.0
    function: saveCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"bgsave", {b"admin", b"dangerous", b"slow"}, flags={b"admin", b"noscript", b"no_async_loading"})
class BackgroundSave(Command):
    """
    summary: Asynchronously saves the database(s) to disk.
    complexity: O(1)
    since: 1.0.0
    function: bgsaveCommand
    reply_schema:
      oneOf:
      - const: Background saving started
      - const: Background saving scheduled
      - const: Background saving cancelled
      - const: Scheduled background saving cancelled
    """

    schedule: bool = flag_parameter(token=b"SCHEDULE")

    def execute(self) -> ValueType:
        return b"Background saving started"


@command(b"bgrewriteaof", {b"admin", b"dangerous", b"slow"}, flags={b"admin", b"noscript", b"no_async_loading"})
class BackgroundRewriteAof(Command):
    """
    summary: Asynchronously rewrites the append-only file to disk.
    complexity: O(1)
    since: 1.0.0
    function: bgrewriteaofCommand
    reply_schema:
      description: >-
        A simple string reply indicating that the rewriting started or is about to start ASAP
      type: string
    """

    def execute(self) -> ValueType:
        return b"Background append only file rewriting started"


@command(
    b"shutdown",
    {b"admin", b"dangerous", b"slow"},
    flags={b"admin", b"allow_busy", b"loading", b"noscript", b"no_multi", b"sentinel", b"stale"},
)
class Shutdown(Command):
    """
    summary: Synchronously saves the database(s) to disk and shuts down the server.
    complexity: >-
      O(N) when saving, where N is the total number of keys in all databases when saving data, otherwise O(1)
    since: 1.0.0
    function: shutdownCommand
    reply_schema:
      description: >-
        OK if ABORT was specified and shutdown was aborted. On successful shutdown, nothing is returned since the
        server quits and the connection is closed. On failure, an error is returned.
      const: OK
    """

    nosave: bool = flag_parameter(token=b"NOSAVE")
    save: bool = flag_parameter(token=b"SAVE")
    now: bool = flag_parameter(token=b"NOW")
    force: bool = flag_parameter(token=b"FORCE")
    abort: bool = flag_parameter(token=b"ABORT")

    def execute(self) -> ValueType:
        if self.abort:
            return RESP_OK
        sys.exit(0)


@command(b"help", {b"slow"}, parent_command=b"config", flags={b"loading", b"stale"})
class ConfigHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: configHelpCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"GET <pattern>",
            b"    Return parameters matching the glob-like <pattern> and their values.",
            b"SET <directive> <value>",
            b"    Set the configuration <directive> to <value>.",
            b"RESETSTAT",
            b"    Reset statistics reported by the INFO command.",
            b"REWRITE",
            b"    Rewrite the configuration file.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(
    b"rewrite",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"config",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class ConfigRewrite(Command):
    """
    summary: Persists the effective configuration to file.
    complexity: O(1)
    since: 2.8.0
    function: configRewriteCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"doctor", {b"slow"}, parent_command=b"memory")
class MemoryDoctor(Command):
    """
    summary: Outputs a memory problems report.
    complexity: O(1)
    since: 4.0.0
    function: memoryCommand
    reply_schema:
      description: Memory problems report.
      type: string
    """

    def execute(self) -> ValueType:
        return b"Sam, I detected a few issues in this Valkey instance memory implants:\n"


@command(b"stats", {b"slow"}, parent_command=b"memory")
class MemoryStats(Command):
    """
    summary: Returns details about memory usage.
    complexity: O(1)
    since: 4.0.0
    function: memoryCommand
    reply_schema:
      description: Memory usage details.
      type: object
      additionalProperties: false
      properties:
        peak.allocated:
          type: integer
        total.allocated:
          type: integer
        startup.allocated:
          type: integer
        replication.backlog:
          type: integer
        replicas.repl.buffer:
          type: integer
        clients.slaves:
          type: integer
        clients.normal:
          type: integer
        cluster.links:
          type: integer
        cluster.slot_import:
          type: integer
        cluster.slot_export:
          type: integer
        aof.buffer:
          type: integer
        lua.caches:
          type: integer
        functions.caches:
          type: integer
        overhead.db.hashtable.lut:
          type: integer
        overhead.db.hashtable.rehashing:
          type: integer
        overhead.total:
          type: integer
        db.dict.rehashing.count:
          type: integer
        keys.count:
          type: integer
        keys.bytes-per-key:
          type: integer
        dataset.bytes:
          type: integer
        dataset.percentage:
          type: number
        peak.percentage:
          type: number
        allocator.allocated:
          type: integer
        allocator.active:
          type: integer
        allocator.resident:
          type: integer
        allocator.muzzy:
          type: integer
        allocator-fragmentation.ratio:
          type: number
        allocator-fragmentation.bytes:
          type: integer
        allocator-rss.ratio:
          type: number
        allocator-rss.bytes:
          type: integer
        rss-overhead.ratio:
          type: number
        rss-overhead.bytes:
          type: integer
        fragmentation:
          type: number
        fragmentation.bytes:
          type: integer
      patternProperties:
        ^db\.\d+$:
          type: object
          properties:
            overhead.hashtable.main:
              type: integer
            overhead.hashtable.expires:
              type: integer
          additionalProperties: false
    """

    def execute(self) -> ValueType:
        return [
            b"peak.allocated",
            0,
            b"total.allocated",
            0,
            b"startup.allocated",
            0,
            b"clients.slaves",
            0,
            b"clients.normal",
            0,
            b"cluster.links",
            0,
            b"aof.buffer",
            0,
            b"lua.caches",
            0,
            b"functions.caches",
            0,
            b"overhead.total",
            0,
            b"keys.count",
            0,
            b"keys.bytes-per-key",
            0,
            b"dataset.bytes",
            0,
            b"dataset.percentage",
            b"0",
            b"peak.percentage",
            b"0",
            b"allocator.allocated",
            0,
            b"allocator.active",
            0,
            b"allocator.resident",
            0,
            b"allocator-fragmentation.ratio",
            b"0",
            b"allocator-fragmentation.bytes",
            0,
            b"allocator-rss.ratio",
            b"0",
            b"allocator-rss.bytes",
            0,
            b"rss-overhead.ratio",
            b"0",
            b"rss-overhead.bytes",
            0,
            b"fragmentation",
            b"0",
            b"fragmentation.bytes",
            0,
        ]


@command(b"malloc-stats", {b"slow"}, parent_command=b"memory", flags={b"loading"})
class MemoryMallocStats(Command):
    """
    summary: Returns the allocator statistics.
    complexity: Depends on how much memory is allocated, could be slow
    since: 4.0.0
    function: memoryCommand
    reply_schema:
      type: string
      description: The memory allocator's internal statistics report.
    """

    def execute(self) -> ValueType:
        return b"malloc stats unavailable\n"


@command(b"purge", {b"slow"}, parent_command=b"memory", flags={b"loading"})
class MemoryPurge(Command):
    """
    summary: Asks the allocator to release memory.
    complexity: Depends on how much memory is allocated, could be slow
    since: 4.0.0
    function: memoryCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(
    b"list", {b"admin", b"dangerous", b"slow"}, b"acl", flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"}
)
class AclList(Command):
    """
    summary: Dumps the effective rules in ACL file format.
    complexity: O(N). Where N is the number of configured users.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      type: array
      description: A list of currently active ACL rules.
      items:
        type: string
    """

    acl: ACL = dependency()

    def execute(self) -> ValueType:
        lines: list[ValueType] = []
        for user in self.acl.values():
            info = user.info
            flags_raw = info[b"flags"]
            flags = b" ".join(flags_raw) if isinstance(flags_raw, list) else flags_raw
            parts: list[bytes] = [b"user", user.name, flags]
            passwords_raw = info[b"passwords"]
            if isinstance(passwords_raw, list):
                parts.extend(b"#" + password for password in passwords_raw)
            for key_name, default in [(b"keys", b"~*"), (b"channels", b"&*"), (b"commands", b"+@all")]:
                value = info[key_name]
                if isinstance(value, bytes):
                    parts.append(value or default)
                else:
                    parts.append(default)
            lines.append(b" ".join(parts))
        return lines


@command(
    b"users",
    {b"admin", b"dangerous", b"slow"},
    b"acl",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class AclUsers(Command):
    """
    summary: Lists all ACL users.
    complexity: O(N). Where N is the number of configured users.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      type: array
      description: List of existing ACL users.
      items:
        type: string
    """

    acl: ACL = dependency()

    def execute(self) -> ValueType:
        return list(self.acl.keys())


@command(b"whoami", {b"slow"}, b"acl", flags={b"loading", b"noscript", b"sentinel", b"stale"})
class AclWhoAmI(Command):
    """
    summary: Returns the authenticated username of the current connection.
    complexity: O(1)
    since: 6.0.0
    function: aclCommand
    reply_schema:
      type: string
      description: The username of the current connection.
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        user = self.client_context.current_user
        return user.name if user is not None else b"default"


@command(
    b"save", {b"admin", b"dangerous", b"slow"}, b"acl", flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"}
)
class AclSave(Command):
    """
    summary: Saves the effective ACL rules in the configured ACL file.
    complexity: O(N). Where N is the number of configured users.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(
    b"load", {b"admin", b"dangerous", b"slow"}, b"acl", flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"}
)
class AclLoad(Command):
    """
    summary: Reloads the rules from the configured ACL file.
    complexity: O(N). Where N is the number of configured users.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(
    b"log", {b"admin", b"dangerous", b"slow"}, b"acl", flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"}
)
class AclLog(Command):
    """
    summary: Lists recent security events generated due to ACL rules.
    complexity: O(N) with N being the number of entries shown.
    since: 6.0.0
    function: aclCommand
    reply_schema:
      oneOf:
      - description: In case `RESET` was not given, a list of recent ACL security events.
        type: array
        items:
          type: object
          additionalProperties: false
          properties:
            count:
              type: integer
            reason:
              type: string
            context:
              type: string
            object:
              type: string
            username:
              type: string
            age-seconds:
              type: number
            client-info:
              type: string
            entry-id:
              type: integer
            timestamp-created:
              type: integer
            timestamp-last-updated:
              type: integer
      - const: OK
        description: In case `RESET` was given, OK indicates ACL log was cleared.
    """

    operation: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.operation is not None and self.operation.upper() == b"RESET":
            return RESP_OK
        return []


@command(b"list", {b"admin", b"dangerous", b"slow"}, parent_command=b"module", flags={b"admin", b"noscript"})
class ModuleList(Command):
    """
    summary: Returns all loaded modules.
    complexity: O(N) where N is the number of loaded modules.
    since: 4.0.0
    function: moduleCommand
    reply_schema:
      type: array
      description: Returns information about the modules loaded to the server.
      items:
        type: object
        additionalProperties: false
        properties:
          name:
            type: string
            description: Name of the module.
          ver:
            type: integer
            description: Version of the module.
          path:
            type: string
            description: Module path.
          args:
            type: array
            description: Module arguments.
            items:
              type: string
    """

    def execute(self) -> ValueType:
        return []


@command(
    b"load",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"module",
    flags={b"admin", b"noscript", b"no_async_loading", b"protected"},
)
class ModuleLoad(Command):
    """
    summary: Loads a module.
    complexity: O(1)
    since: 4.0.0
    function: moduleCommand
    reply_schema:
      const: OK
    """

    path: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        raise ServerError(b"ERR module system not supported")


@command(
    b"loadex",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"module",
    flags={b"admin", b"noscript", b"no_async_loading", b"protected"},
)
class ModuleLoadEx(Command):
    """
    summary: Loads a module using extended parameters.
    complexity: O(1)
    since: 7.0.0
    function: moduleCommand
    reply_schema:
      const: OK
    """

    path: bytes = positional_parameter()
    config_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        raise ServerError(b"ERR module system not supported")


@command(
    b"unload",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"module",
    flags={b"admin", b"noscript", b"no_async_loading", b"protected"},
)
class ModuleUnload(Command):
    """
    summary: Unloads a module.
    complexity: O(1)
    since: 4.0.0
    function: moduleCommand
    reply_schema:
      const: OK
    """

    name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        raise ServerError(b"ERR module system not supported")


@command(b"help", {b"slow"}, parent_command=b"module", flags={b"loading", b"stale"})
class ModuleHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: moduleCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"LIST",
            b"    Return a list of loaded modules.",
            b"LOAD <path> [<arg> ...]",
            b"    Load a module library from <path>, passing the remaining arguments to its onload function.",
            b"LOADEX <path> [CONFIG <name> <value> ...] [ARGS <arg> ...]",
            b"    Load a module with extended options.",
            b"UNLOAD <name>",
            b"    Unload a module.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"get", {b"admin", b"dangerous", b"slow"}, parent_command=b"slowlog", flags={b"admin", b"loading", b"stale"})
class SlowLogGet(Command):
    """
    summary: Returns the slow log's entries.
    complexity: O(N) where N is the number of entries returned
    since: 2.2.12
    function: slowlogCommand
    reply_schema:
      type: array
      description: Entries from the slow log in chronological order.
      uniqueItems: true
      items:
        type: array
        minItems: 6
        maxItems: 6
        items:
        - type: integer
          description: Slow log entry ID.
        - type: integer
          description: The unix timestamp at which the logged command was processed.
          minimum: 0
        - type: integer
          description: The amount of time needed for its execution, in microseconds.
          minimum: 0
        - type: array
          description: The arguments of the command.
          items:
            type: string
        - type: string
          description: Client IP address and port.
        - type: string
          description: Client name if set via the CLIENT SETNAME command.
    """

    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return []


@command(b"len", {b"admin", b"dangerous", b"slow"}, parent_command=b"slowlog", flags={b"admin", b"loading", b"stale"})
class SlowLogLen(Command):
    """
    summary: Returns the number of entries in the slow log.
    complexity: O(1)
    since: 2.2.12
    function: slowlogCommand
    reply_schema:
      type: integer
      description: Number of entries in the slow log.
      minimum: 0
    """

    def execute(self) -> ValueType:
        return 0


@command(b"reset", {b"admin", b"dangerous", b"slow"}, parent_command=b"slowlog", flags={b"admin", b"loading", b"stale"})
class SlowLogReset(Command):
    """
    summary: Clears all entries from the slow log.
    complexity: O(N) where N is the number of entries in the slowlog
    since: 2.2.12
    function: slowlogCommand
    reply_schema:
      const: OK
    """

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"help", {b"slow"}, parent_command=b"slowlog", flags={b"loading", b"stale"})
class SlowLogHelp(Command):
    """
    summary: Shows helpful text about the different subcommands.
    complexity: O(1)
    since: 6.2.0
    function: slowlogCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"GET [<count>]",
            b"    Return top <count> entries from the slowlog (default: 10).",
            b"LEN",
            b"    Return the length of the slowlog.",
            b"RESET",
            b"    Reset the slowlog.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(
    b"get", {b"admin", b"dangerous", b"slow"}, parent_command=b"commandlog", flags={b"admin", b"loading", b"stale"}
)
class CommandLogGet(Command):
    """
    summary: Returns the specified command log's entries.
    complexity: O(N) where N is the number of entries returned
    since: 8.1.0
    function: commandlogCommand
    reply_schema:
      type: array
      description: Entries from the command log in chronological order.
      uniqueItems: true
      items:
        type: array
        minItems: 6
        maxItems: 6
        items:
        - type: integer
          description: Command log entry ID.
        - type: integer
          description: The unix timestamp at which the logged command was processed.
          minimum: 0
        - type: integer
          description: Determined by the type parameter.
          minimum: 0
        - type: array
          description: The arguments of the command.
          items:
            type: string
        - type: string
          description: Client IP address and port.
        - type: string
          description: Client name if set via the CLIENT SETNAME command.
    """

    count: int = positional_parameter()
    log_type: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return []


@command(
    b"len", {b"admin", b"dangerous", b"slow"}, parent_command=b"commandlog", flags={b"admin", b"loading", b"stale"}
)
class CommandLogLen(Command):
    """
    summary: Returns the number of entries in the specified type of command log.
    complexity: O(1)
    since: 8.1.0
    function: commandlogCommand
    reply_schema:
      type: integer
      description: Number of entries in the command log.
      minimum: 0
    """

    log_type: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 0


@command(
    b"reset", {b"admin", b"dangerous", b"slow"}, parent_command=b"commandlog", flags={b"admin", b"loading", b"stale"}
)
class CommandLogReset(Command):
    """
    summary: Clears all entries from the specified type of command log.
    complexity: O(N) where N is the number of entries in the commandlog
    since: 8.1.0
    function: commandlogCommand
    reply_schema:
      const: OK
    """

    log_type: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"help", {b"slow"}, parent_command=b"commandlog", flags={b"loading", b"stale"})
class CommandLogHelp(Command):
    """
    summary: Shows helpful text about the different subcommands.
    complexity: O(1)
    since: 8.1.0
    function: commandlogCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"GET <count> <type>",
            b"    Return top <count> entries of the given log type (slow|large-request|large-reply).",
            b"LEN <type>",
            b"    Return the length of the given log type.",
            b"RESET <type>",
            b"    Reset the given log type.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(
    b"doctor",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"latency",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class LatencyDoctor(Command):
    """
    summary: Returns a human-readable latency analysis report.
    complexity: O(1)
    since: 2.8.13
    function: latencyCommand
    reply_schema:
      type: string
      description: A human readable latency analysis report.
    """

    def execute(self) -> ValueType:
        return b"Dave, no latency events have been observed yet.\n"


@command(
    b"graph",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"latency",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class LatencyGraph(Command):
    """
    summary: Returns a latency graph for an event.
    complexity: O(1)
    since: 2.8.13
    function: latencyCommand
    reply_schema:
      type: string
      description: Latency graph
    """

    event: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return b""


@command(
    b"history",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"latency",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class LatencyHistory(Command):
    """
    summary: Returns timestamp-latency samples for an event.
    complexity: O(1)
    since: 2.8.13
    function: latencyCommand
    reply_schema:
      type: array
      description: >-
        An array where each element is a two elements array representing the timestamp and the latency of the event.
      items:
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Timestamp of the event.
          type: integer
          minimum: 0
        - description: Latency of the event.
          type: integer
          minimum: 0
    """

    event: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return []


@command(
    b"latest",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"latency",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class LatencyLatest(Command):
    """
    summary: Returns the latest latency samples for all events.
    complexity: O(1)
    since: 2.8.13
    function: latencyCommand
    reply_schema:
      type: array
      description: >-
        An array where each element is an array representing the event name, timestamp, latest and all-time latency
        measurements.
      items:
        type: array
        minItems: 6
        maxItems: 6
        items:
        - type: string
          description: Event name.
        - type: integer
          description: Timestamp.
        - type: integer
          description: Latest latency in milliseconds.
        - type: integer
          description: Max latency in milliseconds.
        - type: integer
          description: Sum of the latencies recorded in the time series for this event.
        - type: integer
          description: The number of latency spikes recorded in the time series for this event.
    """

    def execute(self) -> ValueType:
        return []


@command(
    b"reset",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"latency",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class LatencyReset(Command):
    """
    summary: Resets the latency data for one or more events.
    complexity: O(1)
    since: 2.8.13
    function: latencyCommand
    reply_schema:
      type: integer
      description: Number of event time series that were reset.
    """

    events: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 0


@command(
    b"histogram",
    {b"admin", b"dangerous", b"slow"},
    parent_command=b"latency",
    flags={b"admin", b"loading", b"noscript", b"stale"},
)
class LatencyHistogram(Command):
    """
    summary: Returns the cumulative distribution of latencies of a subset or all commands.
    complexity: O(N) where N is the number of commands with latency information being retrieved.
    since: 7.0.0
    function: latencyCommand
    reply_schema:
      type: object
      description: >-
        A map where each key is a command name, and each value is a map with the total calls, and an inner map of
        the histogram time buckets.
      patternProperties:
        ^.*$:
          type: object
          additionalProperties: false
          properties:
            calls:
              description: The total calls for the command.
              type: integer
              minimum: 0
            histogram_usec:
              description: Histogram map, bucket id to latency
              type: object
              additionalProperties:
                type: integer
    """

    commands: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return []


@command(b"help", {b"slow"}, parent_command=b"latency", flags={b"loading", b"stale"})
class LatencyHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 2.8.13
    function: latencyCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"DOCTOR",
            b"    Return a human readable latency analysis report.",
            b"GRAPH <event>",
            b"    Return a latency graph for the <event> class.",
            b"HISTORY <event>",
            b"    Return time-latency samples for the <event> class.",
            b"LATEST",
            b"    Return the latest latency samples for all events.",
            b"RESET [<event> ...]",
            b"    Reset latency data of one or more <event> (or all if not specified).",
            b"HISTOGRAM [<command> ...]",
            b"    Return a cumulative distribution of latencies for the given commands.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"monitor", {b"admin", b"dangerous", b"slow"}, flags={b"admin", b"loading", b"noscript", b"stale"})
class Monitor(Command):
    """
    summary: Listens for all requests received by the server in real-time.
    since: 1.0.0
    function: monitorCommand
    """

    def execute(self) -> ValueType:
        return RESP_OK
