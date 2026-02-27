from __future__ import annotations

from functools import partial
from hashlib import sha1
from typing import TYPE_CHECKING, Any

from lupa import lua51
from lupa.lua51 import lua_type, unpacks_lua_table

from pyvalkey.commands.lua.consts import LIBRARY_NAME_PATTERN
from pyvalkey.commands.lua.core import CallContext, CompiledFunction, RegisteredFunction, RegisteredLibrary
from pyvalkey.commands.lua.errors import LuaServerError
from pyvalkey.commands.lua.scripts import LUA_CALL_WRAPPER, LUA_IMITATE_LUA_FUNCTION
from pyvalkey.database_objects.errors import (
    RouterKeyError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ServerWrongTypeError,
)
from pyvalkey.resp import RESP_OK, DoNotReply, RespError, RespProtocolVersion, ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext

MAX_CONVERT_DEPTH = 100


def convert_lua_value_to_valkey_value(lua_value: Any, depth: int = 1) -> ValueType:  # noqa: ANN401
    print("convert_lua_value_to_valkey_value", depth)

    if depth > MAX_CONVERT_DEPTH:
        return RespError(b"ERR reached lua stack limit")

    if isinstance(lua_value, float):
        return int(lua_value)
    if isinstance(lua_value, bool):
        return lua_value if lua_value else None
    if isinstance(lua_value, bytes):
        return lua_value
    if lua_type(lua_value) == "table":
        if b"ok" in lua_value:  # type: ignore[operator, index]
            return convert_lua_value_to_valkey_value(lua_value.ok, depth + 1)  # type: ignore[attr-defined]
        elif b"_G" in lua_value:  # type: ignore[operator, index]
            return None
        elif b"err" in lua_value:  # type: ignore[operator, index]
            raise ServerError(lua_value.err)  # type: ignore[attr-defined]
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
    writeable: bool,
    function_name_or_kwargs: Any,  # noqa: ANN401
    callback: Any,  # noqa: ANN401
    *args: Any,  # noqa: ANN401
) -> None:
    called_with_table_args = False

    flags: Any | None = None
    description: bytes | None = None
    function_name: bytes | None = function_name_or_kwargs
    if lua51.lua_type(function_name_or_kwargs) == "table":
        called_with_table_args = True
        table_args = dict(function_name_or_kwargs)
        function_name = table_args.pop(b"function_name", None)
        callback = table_args.pop(b"callback", None)
        flags = table_args.pop(b"flags", None)
        description = table_args.pop(b"description", None)
        if table_args:
            raise LuaServerError(b"ERR unknown argument given to server.register_function")

    if (function_name is None and callback is None) or args != ():
        raise LuaServerError(b"ERR wrong number of arguments to server.register_function")

    if not isinstance(function_name, bytes):
        raise LuaServerError(
            f"ERR {'function_name' if called_with_table_args else 'first'} "
            f"argument to server.register_function must be a string".encode()
        )

    if LIBRARY_NAME_PATTERN.match(function_name.decode()) is None:
        raise LuaServerError(
            b"ERR Function names can only contain letters, numbers, "
            b"or underscores(_) and must be at least one character long"
        )

    if callback is None:
        raise LuaServerError(
            b"ERR calling server.register_function with a single argument is only applicable to Lua table"
        )

    if lua51.lua_type(callback) != "function":
        raise LuaServerError(
            f"ERR {'callback' if called_with_table_args else 'second'}"
            f" argument to server.register_function must be a function".encode()
        )

    if description is not None and not isinstance(description, bytes):
        raise LuaServerError(b"ERR description argument given to server.register_function must be a string")

    if flags is not None:
        if lua51.lua_type(flags) != "table":
            raise LuaServerError(
                b"ERR flags argument to server.register_function must be a table representing function flags"
            )
        for flag in flags.values():
            if flag not in {b"no-writes", b"allow-oom", b"no-cluster", b"allow-stale"}:
                raise LuaServerError(b"ERR unknown flag given")

    if function_name.lower() not in library.functions:
        library.functions[function_name.lower()] = RegisteredFunction(
            function_name,
            library.name,
            [v for _, v in flags.items()] if flags is not None else [],
            description or b"",
            CompiledFunction(
                callback if writeable else None,
                callback if not writeable else None,
            ),
        )
        return

    registered_function = library.functions[function_name.lower()]

    compiled_function = registered_function.compiled_function.get(writeable)
    if compiled_function is not None:
        raise LuaServerError(b"ERR Function already exists in the library")
    registered_function.compiled_function.set(writeable, callback)


def _call(
    call_context: CallContext,
    *args: bytes | int,
) -> Any:  # noqa: ANN401
    if not args:
        return call_context.lua_runtime.table(err=b"ERR Please specify at least one argument for this call script")

    command = [str(p).encode() if isinstance(p, int) else p for p in args]

    print(
        "redis.call",
        call_context.client_context.current_client.client_id,
        [i[:300] for i in command],
    )

    try:
        routed_command_cls, parameters = call_context.commands_router.route(command)
    except RouterKeyError:
        return call_context.lua_runtime.table(
            err=f"ERR unknown command '{command[0].decode()}', "
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
        return call_context.lua_runtime.table(err=b"ERR Write commands are not allowed from read-only scripts")

    try:
        result: ValueType = routed_command.execute()
    except ServerWrongTypeError:
        raise LuaServerError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    except ServerError as e:
        return call_context.lua_runtime.table(err=e.message)

    print("redis.call", client_context.current_client.client_id, "result", result, type(result))

    if result is DoNotReply:
        return None
    if result == RESP_OK:
        return call_context.lua_runtime.table(ok="OK")
    if result is None:
        return False
    if isinstance(result, RespError):
        return call_context.lua_runtime.table(err=result)
    if isinstance(result, list):
        return call_context.lua_runtime.table(*result)

    return result


def call(
    call_context: CallContext,
    *args: bytes | int,
) -> Any:  # noqa: ANN401
    print(args)
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


def acl_check_cmd(*args: Any, **kwargs: Any) -> bool:  # noqa: ANN401
    print("acl_check_cmd", args, kwargs)
    return False


def set_repl(*args: Any, **kwargs: Any) -> None:  # noqa: ANN401
    print("set_repl", args, kwargs)


def lua_server_error_raiser(message: bytes) -> None:
    raise LuaServerError(message)


def fill_load_server_globals(call_context: CallContext) -> None:
    lua_runtime = call_context.lua_runtime
    lua_globals = lua_runtime.globals()

    prohibit_calling = lua_runtime.compile(b"""
    return function(lua_server_error_raiser, message)
       return function(...)
          lua_server_error_raiser(message)
       end
    end
    """)()

    redis_server = lua_runtime.table()

    lua_imitate_lua_function = lua_runtime.eval(LUA_IMITATE_LUA_FUNCTION)
    redis_server.sha1hex = lua_imitate_lua_function(sha1hex)

    redis_server.set_repl = set_repl
    redis_server.setresp = unpacks_lua_table(partial(set_resp, call_context.client_context))
    redis_server.acl_check_cmd = acl_check_cmd

    call_wrapper = lua_runtime.eval(LUA_CALL_WRAPPER)
    redis_server.call = prohibit_calling(
        lua_server_error_raiser, b"ERR attempted to access nonexistent global variable 'call'"
    )
    redis_server.pcall = call_wrapper(pcall, call_context)

    lua_globals.math.random = prohibit_calling(
        lua_server_error_raiser, b"ERR attempted to access nonexistent global variable 'math'"
    )

    lua_globals.server = redis_server
    lua_globals.redis = redis_server


def fill_server_globals(call_context: CallContext) -> None:
    lua_globals = call_context.lua_runtime.globals()

    redis_server = call_context.lua_runtime.table()

    lua_imitate_lua_function = call_context.lua_runtime.eval(LUA_IMITATE_LUA_FUNCTION)
    redis_server.sha1hex = lua_imitate_lua_function(sha1hex)

    redis_server.set_repl = set_repl
    redis_server.setresp = unpacks_lua_table(partial(set_resp, call_context.client_context))
    redis_server.acl_check_cmd = acl_check_cmd

    call_wrapper = call_context.lua_runtime.eval(LUA_CALL_WRAPPER)
    redis_server.call = call_wrapper(call, call_context)
    redis_server.pcall = call_wrapper(pcall, call_context)

    lua_globals.server = redis_server
    lua_globals.redis = redis_server
