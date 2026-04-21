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


@command(b"help", {b"slow"}, b"acl")
class AclHelp(Command):
    def execute(self) -> ValueType:
        return ["genpass"]


@command(b"genpass", {b"slow"}, b"acl")
class AclGeneratePassword(Command):
    length: int = positional_parameter(default=64)

    def execute(self) -> ValueType:
        return urandom(self.length)


@command(b"cat", {b"slow"}, b"acl")
class AclCategory(Command):
    category: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.category is not None:
            return ACL.get_category_commands(self.category)
        return ACL.get_categories()


@command(b"deluser", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclDeleteUser(Command):
    acl: ACL = dependency()

    user_names: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        user_deleted = 0
        for user_name in self.user_names:
            if user_name == b"default":
                pass
            user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
        return user_deleted


@command(b"getuser", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclGetUser(Command):
    acl: ACL = dependency()
    user_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.user_name not in self.acl:
            return None
        return self.acl[self.user_name].info


@command(b"dryrun", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclDryRun(Command):
    acl: ACL = dependency()

    username: bytes = positional_parameter()
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"setuser", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclSetUser(Command):
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


@command(b"getkeys", {b"connection", b"slow"}, b"command")
class CommandGetKeys(Command):
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


@command(b"get", {b"admin", b"dangerous", b"slow"}, b"config")
class ConfigGet(Command):
    configurations: Configurations = dependency()
    parameters: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        names = self.configurations.get_names(*self.parameters)
        return self.configurations.info(names)


@command(b"set", {b"admin", b"dangerous", b"slow"}, b"config")
class ConfigSet(Command):
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


@command(b"resetstat", {b"admin", b"dangerous", b"slow"}, b"config")
class ConfigResetStatistics(Command):
    configurations: Configurations = dependency()
    information: Information = dependency()

    def execute(self) -> ValueType:
        self.information.reset_stats()
        return RESP_OK


@command(b"dbsize", {b"fast", b"keyspace", b"read"})
class DatabaseSize(Command):
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


@command(b"flushall", {b"dangerous", b"keyspace", b"slow"}, flags={b"write"})
class FlushAllDatabases(Command):
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


@command(b"info", {b"connection", b"dangerous", b"slow"})
class GetInformation(Command):
    information: Information = dependency()

    section: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return self.information.sections(self.section)


@command(b"help", {b"slow"}, parent_command=b"memory")
class MemoryHelp(Command):
    def execute(self) -> ValueType:
        return [
            b"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"USAGE <key>",
            b"    Return the internal memory usage for the Redis object associated with the <key>.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"usage", {b"read", b"slow"}, parent_command=b"memory")
class MemoryUsage(Command):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"swapdb", {b"dangerous", b"fast", b"keyspace"}, flags={b"write"})
class SwapDb(Command):
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


@command(b"sync", {b"admin", b"dangerous", b"slow"}, flags={b"write"})
class Sync(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"time", {b"fast"})
class Time(Command):
    def execute(self) -> ValueType:
        now = time.time_ns()
        return [str(now // 1_000_000_000).encode(), str((now // 1_000) % 1_000_000).encode()]


@command(b"role", {b"admin", b"fast", b"dangerous"})
class Role(Command):
    def execute(self) -> ValueType:
        return [b"master", 0, []]


@command(b"lolwut", {b"fast"})
class Lolwut(Command):
    version: bytes | None = keyword_parameter(flag=b"VERSION", default=None)

    def execute(self) -> ValueType:
        return b"Valkey ver. 255.255.255\n"


@command(b"lastsave", {b"admin", b"fast", b"dangerous"})
class LastSave(Command):
    information: Information = dependency()

    def execute(self) -> ValueType:
        return int(self.information.start_time // 1000)


@command(b"save", {b"admin", b"slow", b"dangerous"})
class Save(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"bgsave", {b"admin", b"slow", b"dangerous"})
class BackgroundSave(Command):
    schedule: bool = flag_parameter(token=b"SCHEDULE")

    def execute(self) -> ValueType:
        return b"Background saving started"


@command(b"bgrewriteaof", {b"admin", b"slow", b"dangerous"})
class BackgroundRewriteAof(Command):
    def execute(self) -> ValueType:
        return b"Background append only file rewriting started"


@command(b"shutdown", {b"admin", b"slow", b"dangerous"}, flags={b"no-script"})
class Shutdown(Command):
    nosave: bool = flag_parameter(token=b"NOSAVE")
    save: bool = flag_parameter(token=b"SAVE")
    now: bool = flag_parameter(token=b"NOW")
    force: bool = flag_parameter(token=b"FORCE")
    abort: bool = flag_parameter(token=b"ABORT")

    def execute(self) -> ValueType:
        if self.abort:
            return RESP_OK
        sys.exit(0)


@command(b"help", {b"admin", b"dangerous", b"slow"}, parent_command=b"config")
class ConfigHelp(Command):
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


@command(b"rewrite", {b"admin", b"dangerous", b"slow"}, parent_command=b"config")
class ConfigRewrite(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"doctor", {b"slow"}, parent_command=b"memory")
class MemoryDoctor(Command):
    def execute(self) -> ValueType:
        return b"Sam, I detected a few issues in this Valkey instance memory implants:\n"


@command(b"stats", {b"slow"}, parent_command=b"memory")
class MemoryStats(Command):
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


@command(b"malloc-stats", {b"slow"}, parent_command=b"memory")
class MemoryMallocStats(Command):
    def execute(self) -> ValueType:
        return b"malloc stats unavailable\n"


@command(b"purge", {b"slow"}, parent_command=b"memory")
class MemoryPurge(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"list", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclList(Command):
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


@command(b"users", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclUsers(Command):
    acl: ACL = dependency()

    def execute(self) -> ValueType:
        return list(self.acl.keys())


@command(b"whoami", {b"slow"}, b"acl")
class AclWhoAmI(Command):
    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        user = self.client_context.current_user
        return user.name if user is not None else b"default"


@command(b"save", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclSave(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"load", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclLoad(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"log", {b"admin", b"dangerous", b"slow"}, b"acl")
class AclLog(Command):
    operation: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.operation is not None and self.operation.upper() == b"RESET":
            return RESP_OK
        return []


@command(b"list", {b"admin", b"slow", b"dangerous"}, parent_command=b"module")
class ModuleList(Command):
    def execute(self) -> ValueType:
        return []


@command(b"load", {b"admin", b"slow", b"dangerous"}, parent_command=b"module")
class ModuleLoad(Command):
    path: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        raise ServerError(b"ERR module system not supported")


@command(b"loadex", {b"admin", b"slow", b"dangerous"}, parent_command=b"module")
class ModuleLoadEx(Command):
    path: bytes = positional_parameter()
    config_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        raise ServerError(b"ERR module system not supported")


@command(b"unload", {b"admin", b"slow", b"dangerous"}, parent_command=b"module")
class ModuleUnload(Command):
    name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        raise ServerError(b"ERR module system not supported")


@command(b"help", {b"admin", b"slow", b"dangerous"}, parent_command=b"module")
class ModuleHelp(Command):
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


@command(b"get", {b"admin", b"slow"}, parent_command=b"slowlog")
class SlowLogGet(Command):
    count: int | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return []


@command(b"len", {b"admin", b"slow"}, parent_command=b"slowlog")
class SlowLogLen(Command):
    def execute(self) -> ValueType:
        return 0


@command(b"reset", {b"admin", b"slow"}, parent_command=b"slowlog")
class SlowLogReset(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"help", {b"admin", b"slow"}, parent_command=b"slowlog")
class SlowLogHelp(Command):
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


@command(b"get", {b"admin", b"slow"}, parent_command=b"commandlog")
class CommandLogGet(Command):
    count: int = positional_parameter()
    log_type: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return []


@command(b"len", {b"admin", b"slow"}, parent_command=b"commandlog")
class CommandLogLen(Command):
    log_type: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 0


@command(b"reset", {b"admin", b"slow"}, parent_command=b"commandlog")
class CommandLogReset(Command):
    log_type: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"help", {b"admin", b"slow"}, parent_command=b"commandlog")
class CommandLogHelp(Command):
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


@command(b"doctor", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyDoctor(Command):
    def execute(self) -> ValueType:
        return b"Dave, no latency events have been observed yet.\n"


@command(b"graph", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyGraph(Command):
    event: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return b""


@command(b"history", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyHistory(Command):
    event: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return []


@command(b"latest", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyLatest(Command):
    def execute(self) -> ValueType:
        return []


@command(b"reset", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyReset(Command):
    events: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 0


@command(b"histogram", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyHistogram(Command):
    commands: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return []


@command(b"help", {b"admin", b"slow"}, parent_command=b"latency")
class LatencyHelp(Command):
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


@command(b"monitor", {b"admin", b"slow", b"dangerous"}, flags={b"no-script"})
class Monitor(Command):
    def execute(self) -> ValueType:
        return RESP_OK
