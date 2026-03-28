from __future__ import annotations

import asyncio
from functools import partial
from hashlib import sha1
from typing import TYPE_CHECKING, Any

from lua_runtime import LuaRuntime, lua_type, unpacks_lua_table

from pyvalkey.commands.lua.consts import LIBRARY_NAME_PATTERN
from pyvalkey.commands.core import Command
from pyvalkey.commands.lua.core import CallContext, RegisteredFunction, RegisteredLibrary
from pyvalkey.commands.lua.errors import LuaServerError
from pyvalkey.commands.lua.scripts import (
    LUA_MAKE_READONLY_SERVER_TABLE,
)
from pyvalkey.database_objects.errors import (
    NoPermissionError,
    RouterKeyError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ServerWrongTypeError,
)
from pyvalkey.resp import RESP_OK, DoNotReply, RespError, RespProtocolVersion, ValueType

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyvalkey.commands.context import ClientContext

MAX_CONVERT_DEPTH = 100


def convert_lua_value_to_valkey_value(lua_value: Any, depth: int = 1) -> ValueType:  # noqa: ANN401
    if depth > MAX_CONVERT_DEPTH:
        return RespError(b"ERR reached lua stack limit")

    if isinstance(lua_value, float):
        return int(lua_value)
    if isinstance(lua_value, bool):
        return lua_value if lua_value else None
    if isinstance(lua_value, bytes):
        return lua_value
    if lua_type(lua_value) == "table":
        if b"ok" in lua_value:
            return convert_lua_value_to_valkey_value(lua_value.ok, depth + 1)
        elif b"_G" in lua_value:
            return None
        elif b"err" in lua_value:
            raise ServerError(lua_value.err)
        else:
            all_numbers = True
            values = {}
            for key, value in lua_value.items():
                if not isinstance(key, int):
                    all_numbers = False
                values[key] = convert_lua_value_to_valkey_value(value, depth + 1)
            if all_numbers:
                return list(values.values())
            return values
    if not isinstance(lua_value, int | str | bytes):
        return None
    return lua_value


def register_function(
    library: RegisteredLibrary,
    function_name_or_kwargs: Any = None,  # noqa: ANN401
    callback: Any = None,  # noqa: ANN401
    *args: Any,  # noqa: ANN401
) -> None:
    called_with_table_args = False

    flags: Any | None = None
    description: bytes | None = None
    function_name: bytes | None = function_name_or_kwargs
    if lua_type(function_name_or_kwargs) == "table":
        called_with_table_args = True
        table_args = dict(function_name_or_kwargs)
        function_name = table_args.pop(b"function_name", None)
        callback = table_args.pop(b"callback", None)
        flags = table_args.pop(b"flags", None)
        description = table_args.pop(b"description", None)
        if table_args:
            raise LuaServerError(b"ERR unknown argument given to server.register_function")

    if args != ():
        raise LuaServerError(b"ERR wrong number of arguments to server.register_function")

    if called_with_table_args:
        if function_name is None:
            raise LuaServerError(b"ERR server.register_function must get a function name argument")
        if not isinstance(function_name, bytes):
            raise LuaServerError(b"ERR function_name argument given to server.register_function must be a string")
    else:
        if function_name is None and callback is None:
            raise LuaServerError(b"ERR wrong number of arguments to server.register_function")
        if not isinstance(function_name, bytes):
            raise LuaServerError(b"ERR first argument to server.register_function must be a string")

    if LIBRARY_NAME_PATTERN.match(function_name.decode()) is None:
        raise LuaServerError(
            b"ERR Function names can only contain letters, numbers, "
            b"or underscores(_) and must be at least one character long"
        )

    if called_with_table_args:
        if callback is None:
            raise LuaServerError(b"ERR server.register_function must get a callback argument")
        if lua_type(callback) != "function":
            raise LuaServerError(b"ERR callback argument given to server.register_function must be a function")
    else:
        if callback is None:
            raise LuaServerError(
                b"ERR calling server.register_function with a single argument is only applicable to Lua table"
            )
        if lua_type(callback) != "function":
            raise LuaServerError(b"ERR second argument to server.register_function must be a function")

    if description is not None and not isinstance(description, bytes):
        raise LuaServerError(b"ERR description argument given to server.register_function must be a string")

    if flags is not None:
        if lua_type(flags) != "table":
            raise LuaServerError(
                b"ERR flags argument to server.register_function must be a table representing function flags"
            )
        for flag in flags.values():
            if flag not in {b"no-writes", b"allow-oom", b"no-cluster", b"allow-stale"}:
                raise LuaServerError(b"ERR unknown flag given")

    if function_name.lower() in library.functions:
        raise LuaServerError(b"ERR Function already exists in the library")

    library.functions[function_name.lower()] = RegisteredFunction(
        function_name,
        library.name,
        [v for _, v in flags.items()] if flags is not None else [],
        description or b"",
        callback,
    )


def _convert_list_to_lua(lua_runtime: LuaRuntime, value: list) -> Any:  # noqa: ANN401
    items = []
    for item in value:
        if isinstance(item, list):
            items.append(_convert_list_to_lua(lua_runtime, item))
        elif isinstance(item, dict):
            flat = []
            for k, v in item.items():
                flat.append(k)
                flat.append(v)
            items.append(_convert_list_to_lua(lua_runtime, flat))
        elif item is None:
            items.append(False)
        else:
            items.append(item)
    return lua_runtime.table(*items)


def _call(
    call_context: CallContext,
    *args: bytes | int,
) -> Any:  # noqa: ANN401
    if not args:
        raise LuaServerError(b"ERR Please specify at least one argument for this call script")

    command = [str(p).encode() if isinstance(p, int) else p for p in args]

    try:
        routed_command_cls, parameters = call_context.commands_router.route(command)
    except RouterKeyError:
        raise LuaServerError(
            f"ERR Unknown command '{command[0].decode()}', "
            f"with args beginning with: {command[1].decode() if len(command) > 1 else ''}".encode()
        )

    client_context = call_context.client_context.__class__(
        call_context.client_context.server_context,
        call_context.client_context.current_client,
        call_context.client_context.subscriptions,
        call_context.client_context.current_database,
        call_context.client_context.current_user,
        call_context.client_context.transaction_context,
        call_context.client_context.client_watchlist,
        call_context.client_context.protocol,
        call_context.client_context.propagated_commands,
    )

    try:
        routed_command = routed_command_cls.create(parameters, client_context)
    except ServerWrongNumberOfArgumentsError:
        raise LuaServerError(b"ERR Wrong number of args calling command from script")

    if b"no-script" in routed_command.flags:
        raise LuaServerError(b"ERR This Valkey command is not allowed from scripts")
    if b"write" in routed_command.flags and call_context.readonly is True:
        raise LuaServerError(b"ERR Write commands are not allowed from read-only scripts")

    if type(routed_command).before is not Command.before:
        asyncio.run(routed_command.before(in_multi=True))

    try:
        result: ValueType = routed_command.execute()
    except ServerWrongTypeError:
        raise LuaServerError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    except ServerError as e:
        raise LuaServerError(e.message)

    if result is DoNotReply:
        return None
    if result == RESP_OK:
        return call_context.lua_runtime.table_from({b"ok": b"OK"})
    if result is None:
        return False
    if isinstance(result, RespError):
        raise LuaServerError(result if isinstance(result, bytes) else bytes(result))
    if isinstance(result, list):
        return _convert_list_to_lua(call_context.lua_runtime, result)

    return result


def call(
    call_context: CallContext,
    *args: bytes | int,
) -> Any:  # noqa: ANN401
    return _call(call_context, *args)


def pcall(
    call_context: CallContext,
    *args: bytes | int,
) -> Any:  # noqa: ANN401
    try:
        return _call(call_context, *args)
    except LuaServerError as e:
        return call_context.lua_runtime.table_from({b"err": e.message})
    except Exception as e:
        return call_context.lua_runtime.table_from({b"err": str(e).encode()})


@unpacks_lua_table
def sha1hex(value: bytes) -> bytes:
    return sha1(value).hexdigest().encode()


def set_resp(client_context: ClientContext, value: int) -> None:
    client_context.protocol = RespProtocolVersion(value)


def _make_acl_check_cmd(call_context: CallContext) -> Callable:
    def wrapped(*args: Any) -> int | None:  # noqa: ANN401
        if not args:
            raise LuaServerError(b"ERR Invalid command passed to server.acl_check_cmd()")
        command = [str(p).encode() if isinstance(p, int) else p for p in args]
        try:
            routed_command_cls, parameters = call_context.commands_router.route(command)
        except RouterKeyError:
            raise LuaServerError(b"ERR Invalid command passed to server.acl_check_cmd()")
        client_context = call_context.client_context
        try:
            routed_command = routed_command_cls.create(parameters, client_context)
        except ServerWrongNumberOfArgumentsError:
            raise LuaServerError(b"ERR Wrong number of args calling command from script")
        current_user = client_context.current_user
        if current_user is None:
            return 1
        try:
            current_user.check_permissions(routed_command)
            return 1
        except NoPermissionError:
            return None
    return wrapped


def set_repl(value: Any = None, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
    if value not in {0, 1, 2, 3}:
        raise LuaServerError(b"ERR Invalid replication flags. Use REPL_NONE, REPL_AOF, REPL_REPLICA or REPL_ALL.")


def replicate_commands() -> int:
    return 1


def lua_server_error_raiser(message: bytes) -> None:
    raise LuaServerError(message)


def _make_call_wrapper(call_fn: Callable, ctx: CallContext) -> Callable:
    def wrapped(*args: Any) -> Any:  # noqa: ANN401
        return call_fn(ctx, *args)
    return wrapped


def _make_sha1hex_wrapper(sha1hex_fn: Callable) -> Callable:
    @unpacks_lua_table
    def wrapped(value: Any = None) -> Any:  # noqa: ANN401
        if value is None:
            raise LuaServerError(b"ERR wrong number of arguments")
        return sha1hex_fn(value)
    return wrapped


def _make_prohibit(message: bytes) -> Callable:
    def wrapped(*args: Any) -> None:  # noqa: ANN401
        raise LuaServerError(message)
    return wrapped


def fill_load_server_globals(call_context: CallContext) -> Any:  # noqa: ANN401
    lua_runtime = call_context.lua_runtime
    lua_globals = lua_runtime.globals()

    redis_server = lua_runtime.table()

    redis_server.sha1hex = _make_sha1hex_wrapper(sha1hex)
    redis_server.pcall = _make_call_wrapper(pcall, call_context)

    lua_globals.math.random = _make_prohibit(
        b"ERR attempted to access nonexistent global variable 'math'"
    )

    server_version = call_context.client_context.server_context.information.server_version
    major, minor, patch = (int(p) for p in server_version.split(b"."))
    redis_server.REDIS_VERSION = server_version
    redis_server.REDIS_VERSION_NUM = (major << 16) | (minor << 8) | patch
    redis_server.VALKEY_VERSION = server_version
    redis_server.VALKEY_VERSION_NUM = (major << 16) | (minor << 8) | patch

    make_readonly = lua_runtime.eval(LUA_MAKE_READONLY_SERVER_TABLE)
    readonly_server = make_readonly(redis_server)
    readonly_server.set_readonly(True)
    lua_globals.server = readonly_server
    lua_globals.redis = readonly_server

    return redis_server


def fill_server_globals(call_context: CallContext) -> None:
    lua_globals = call_context.lua_runtime.globals()

    redis_server = call_context.lua_runtime.table()

    redis_server.sha1hex = _make_sha1hex_wrapper(sha1hex)

    redis_server.set_repl = set_repl
    redis_server.setresp = unpacks_lua_table(partial(set_resp, call_context.client_context))
    redis_server.acl_check_cmd = _make_acl_check_cmd(call_context)

    redis_server.call = _make_call_wrapper(call, call_context)
    redis_server.pcall = _make_call_wrapper(pcall, call_context)
    redis_server.replicate_commands = replicate_commands

    def error_reply(message: Any = None) -> Any:  # noqa: ANN401
        return call_context.lua_runtime.table_from(
            {b"err": message if isinstance(message, bytes) else str(message).encode()}
        )

    def status_reply(message: Any = None) -> Any:  # noqa: ANN401
        return call_context.lua_runtime.table_from(
            {b"ok": message if isinstance(message, bytes) else str(message).encode()}
        )

    redis_server.error_reply = error_reply
    redis_server.status_reply = status_reply

    redis_server.REPL_NONE = 0
    redis_server.REPL_AOF = 1
    redis_server.REPL_REPLICA = 2
    redis_server.REPL_ALL = 3

    server_version = call_context.client_context.server_context.information.server_version
    major, minor, patch = (int(p) for p in server_version.split(b"."))
    redis_server.REDIS_VERSION = server_version
    redis_server.REDIS_VERSION_NUM = (major << 16) | (minor << 8) | patch
    redis_server.VALKEY_VERSION = server_version
    redis_server.VALKEY_VERSION_NUM = (major << 16) | (minor << 8) | patch

    redis_server.set_readonly(True)
    lua_globals.server = redis_server
    lua_globals.redis = redis_server
