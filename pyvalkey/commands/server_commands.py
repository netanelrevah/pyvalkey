from __future__ import annotations

from dataclasses import fields
from os import urandom

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command, DatabaseCommand
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import ParameterMetadata, keyword_parameter, positional_parameter
from pyvalkey.commands.router import CommandsRouter, command
from pyvalkey.database_objects.acl import ACL, ACLUser, CommandRule, KeyPattern, Permission
from pyvalkey.database_objects.configurations import ConfigurationError, Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.information import Information
from pyvalkey.resp import RESP_OK, RespError, ValueType


@command(b"help", {b"slow", b"connection"}, b"acl")
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


@command(b"deluser", {b"admin", b"slow", b"dangerous"}, b"acl")
class AclDeleteUser(Command):
    acl: ACL = server_command_dependency()

    user_names: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        user_deleted = 0
        for user_name in self.user_names:
            if user_name == b"default":
                pass
            user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
        return user_deleted


@command(b"getuser", {b"admin", b"slow", b"dangerous"}, b"acl")
class AclGetUser(Command):
    acl: ACL = server_command_dependency()
    user_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.user_name not in self.acl:
            return None
        return self.acl[self.user_name].info


@command(b"dryrun", {b"admin", b"slow", b"dangerous"}, b"acl")
class AclDryRun(Command):
    acl: ACL = server_command_dependency()

    username: bytes = positional_parameter()
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"setuser", {b"admin", b"slow", b"dangerous"}, b"acl")
class AclSetUser(Command):
    acl: ACL = server_command_dependency()
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
                callbacks.append(lambda _acl_user, password=rule[1]: _acl_user.add_password(password))  # type: ignore[misc]
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


@command(b"getkeys", {b"connection", b"fast"}, b"command")
class CommandGetKeys(Command):
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        parameters = [self.command, *self.args]
        command_cls: type[Command] = CommandsRouter().internal_route(
            parameters=parameters, routes=CommandsRouter.ROUTES
        )
        parsed_command = command_cls.parse(parameters)

        keys = []
        for field in fields(command_cls):
            if ParameterMetadata.KEY_MODE in field.metadata:
                if isinstance(parsed_command[field.name], list):
                    keys.extend(parsed_command[field.name])
                else:
                    keys.append(parsed_command[field.name])

        if not keys:
            return RespError(b"The command has no key arguments")

        return keys


@command(b"get", {b"admin", b"slow", b"dangerous"}, b"config")
class ConfigGet(Command):
    configurations: Configurations = server_command_dependency()
    parameters: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        names = self.configurations.get_names(*self.parameters)
        return self.configurations.info(names)


@command(b"set", {b"admin", b"slow", b"dangerous"}, b"config")
class ConfigSet(Command):
    configurations: Configurations = server_command_dependency()
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


@command(b"dbsize", {b"keyspace", b"read", b"fast"})
class DatabaseSize(DatabaseCommand):
    def execute(self) -> ValueType:
        return self.database.size()


@command(b"debug", acl_categories={b"fast", b"connection"})
class Debug(Command):
    set_active_expire: int = keyword_parameter(flag=b"set-active-expire", default=b"0")
    object: bytes | None = keyword_parameter(token=b"object", default=None)

    def execute(self) -> ValueType:
        return True


def touch_all_database_watched_keys(database: Database) -> None:
    for key in database.data.keys():
        database.touch_watched_key(key)


def touch_all_databases_watched_keys(databases: dict[int, Database]) -> None:
    for database in databases.values():
        touch_all_database_watched_keys(database)


@command(b"flushall", {b"keyspace", b"write", b"slow", b"dangerous"})
class FlushAllDatabases(Command):
    server_context: ServerContext = server_command_dependency()

    def execute(self) -> ValueType:
        touch_all_databases_watched_keys(self.server_context.databases)
        self.server_context.databases.clear()
        return RESP_OK


@command(b"flushdb", {b"keyspace", b"write", b"slow", b"dangerous"})
class FlushDatabase(Command):
    client_context: ClientContext = server_command_dependency()

    def execute(self) -> ValueType:
        if self.client_context.current_database in self.client_context.server_context.databases:
            for key in self.client_context.database.data.keys():
                self.client_context.database.touch_watched_key(key)
            self.client_context.server_context.databases.pop(self.client_context.current_database)
        return RESP_OK


@command(b"info", {b"slow", b"dangerous"})
class GetInformation(Command):
    information: Information = server_command_dependency()

    section: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return self.information.all()


@command(b"usage", {b"read", b"slow"}, b"memory")
class MemoryUsage(Command):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@command(b"swapdb", {b"keyspace", b"write", b"slow", b"dangerous"})
class SwapDb(Command):
    server_context: ServerContext = server_command_dependency()

    index1: int = positional_parameter(parse_error=b"ERR invalid first DB index")
    index2: int = positional_parameter(parse_error=b"ERR invalid second DB index")

    def execute(self) -> ValueType:
        if self.index1 == self.index2:
            return RESP_OK

        if self.index1 not in self.server_context.databases:
            raise ServerError(b"ERR DB index is out of range")

        touch_all_database_watched_keys(self.server_context.databases[self.index1])
        if self.index2 not in self.server_context.databases:
            self.server_context.databases[self.index2] = self.server_context.databases.pop(self.index1)
            self.server_context.databases[self.index2].index = self.index2
            self.server_context.databases[self.index2].watchlist = {}
            return RESP_OK

        touch_all_database_watched_keys(self.server_context.databases[self.index2])

        (self.server_context.databases[self.index1], self.server_context.databases[self.index2]) = (
            self.server_context.databases[self.index2],
            self.server_context.databases[self.index1],
        )

        self.server_context.databases[self.index1].index = self.index1
        self.server_context.databases[self.index2].index = self.index2
        self.server_context.databases[self.index1].watchlist, self.server_context.databases[self.index2].watchlist = (
            self.server_context.databases[self.index2].watchlist,
            self.server_context.databases[self.index1].watchlist,
        )
        touch_all_database_watched_keys(self.server_context.databases[self.index1])
        touch_all_database_watched_keys(self.server_context.databases[self.index2])

        return RESP_OK


@command(b"sync", {b"keyspace", b"write", b"slow", b"dangerous"})
class Sync(Command):
    def execute(self) -> ValueType:
        return RESP_OK
