from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from hashlib import sha1
from typing import TYPE_CHECKING, Any, NoReturn

from lupa import lua51
from lupa.lua51 import LuaError, LuaRuntime, LuaSyntaxError, lua_type, unpacks_lua_table

from pyvalkey.commands.lua.bit import register_bit_module
from pyvalkey.commands.lua.cjson import register_cjson_module
from pyvalkey.commands.lua.cmsgpack import register_cmsgpack_module
from pyvalkey.commands.lua.core import RegisteredFunction, RegisteredLibrary
from pyvalkey.commands.lua.scripts import LIBRARY_NAME_PATTERN, LUA_CALL_WRAPPER, LUA_IMITATE_LUA_FUNCTION
from pyvalkey.database_objects.errors import (
    RouterKeyError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ServerWrongTypeError,
)
from pyvalkey.resp import RESP_OK, DoNotReply, RespError, RespProtocolVersion, ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter

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
    readonly: bool,
    function_name: bytes,
    callback: Any,  # noqa: ANN401
    *args: Any,  # noqa: ANN401
) -> None:
    flags: list[bytes] | None = None
    description: bytes | None = None
    if lua51.lua_type(function_name) == "table":
        flags = function_name.flags if b"flags" in function_name else None  # type: ignore[attr-defined]
        description = function_name.description if b"description" in function_name else None  # type: ignore[attr-defined]
        callback = function_name.callback if b"callback" in function_name else None  # type: ignore[attr-defined]
        function_name = function_name.function_name if b"function_name" in function_name else None  # type: ignore[assignment, attr-defined]

    if (function_name is None and callback is None) or args != ():
        raise ServerLuaError(b"ERR wrong number of arguments to server.register_function")

    if not isinstance(function_name, bytes):
        raise ServerLuaError(b"ERR first argument to server.register_function must be a string")

    if LIBRARY_NAME_PATTERN.match(function_name.decode()) is None:
        raise ServerLuaError(
            b"ERR Function names can only contain letters, numbers, "
            b"or underscores(_) and must be at least one character long"
        )

    if callback is None:
        raise ServerLuaError(
            b"ERR calling server.register_function with a single argument is only applicable to Lua table"
        )
    if lua51.lua_type(callback) != "function":
        raise ServerLuaError(b"ERR second argument to server.register_function must be a function")

    registered_function = None
    if function_name.lower() in library.functions:
        registered_function = library.functions[function_name.lower()]

        compiled_function = (
            registered_function.readonly_compiled_function if readonly else registered_function.compiled_function
        )
        if compiled_function is not None:
            raise ServerLuaError(b"ERR Function already exists in the library")

    if registered_function is None:
        registered_function = RegisteredFunction(
            function_name,
            library.name,
            callback if not readonly else None,
            callback if readonly else None,
            list(flags or []),
            description=description,
        )
        library.functions[function_name.lower()] = registered_function
    elif not readonly:
        registered_function.compiled_function = callback
    elif readonly:
        registered_function.readonly_compiled_function = callback


class ServerLuaError(LuaError):
    def __init__(self, message: bytes = b"") -> None:
        super().__init__(message)
        self.message = message


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
        raise ServerLuaError(b"ERR Wrong number of args calling command from script")

    if b"no-script" in routed_command.flags:
        raise ServerLuaError(b"ERR This Valkey command is not allowed from scripts")
    if b"write" in routed_command.flags and call_context.readonly is True:
        return call_context.lua_runtime.table(err=b"ERR Write commands are not allowed from read-only scripts")

    try:
        result: ValueType = routed_command.execute()
    except ServerWrongTypeError:
        raise ServerLuaError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
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
    except ServerLuaError as e:
        return call_context.lua_runtime.table_from({b"err": e.message})
    except Exception as e:
        return call_context.lua_runtime.table_from({b"err": str(e).encode()})


@unpacks_lua_table
def sha1hex(value: bytes) -> bytes:
    return sha1(value).hexdigest().encode()


def prohibit_calling(function_name: bytes, *args: Any, **kwargs: Any) -> NoReturn:  # noqa: ANN401
    raise ServerError(f"ERR attempt to call field '{function_name.decode()}'".encode())


def set_resp(client_context: ClientContext, value: int) -> None:
    client_context.protocol = RespProtocolVersion(value)


def create_lua_runtime() -> LuaRuntimeWrapper:
    lua_runtime = LuaRuntimeWrapper(
        LuaRuntime(
            encoding=None,  # type: ignore[call-arg]
            source_encoding=None,  # type: ignore[call-arg]
            register_eval=False,  # type: ignore[call-arg]
            register_builtins=False,  # type: ignore[call-arg]
            unpack_returned_tuples=True,
            overflow_handler=lambda value: str(value),  # type: ignore[call-arg]
        )
    )

    lua_globals = lua_runtime.globals()

    register_cjson_module(lua_runtime, lua_globals)
    register_cmsgpack_module(lua_runtime, lua_globals)
    register_bit_module(lua_runtime, lua_globals)

    lua_globals.os.execute = partial(prohibit_calling, b"execute")
    lua_globals.os.exit = partial(prohibit_calling, b"exit")
    lua_globals.os.getenv = partial(prohibit_calling, b"getenv")
    lua_globals.os.remove = partial(prohibit_calling, b"remove")
    lua_globals.os.rename = partial(prohibit_calling, b"rename")
    lua_globals.os.setlocale = partial(prohibit_calling, b"setlocale")
    lua_globals.os.tmpname = partial(prohibit_calling, b"tmpname")

    lua_runtime.eval(b"""
        function(protection)
            setmetatable(_G, {
                __index = function(t, k) return protection(t, k) end,
            })
        end
    """)(table_protection)

    return lua_runtime


@unpacks_lua_table
def table_protection(*args: Any, **kwargs: Any) -> NoReturn:  # noqa: ANN401
    if len(args) != 2:  # noqa: PLR2004
        raise LuaError("Wrong number of arguments to luaProtectedTableError")
    if not isinstance(args[1], int | bytes):
        raise LuaError("Second argument to luaProtectedTableError must be a string or number")
    variable_name = str(args[1] if isinstance(args[1], int) else args[1].decode())
    raise LuaError(f"Script attempted to access nonexistent global variable '{variable_name}'")


@dataclass
class LuaRuntimeWrapper:
    lua_runtime: LuaRuntime

    def globals(self) -> Any:  # noqa: ANN401
        return self.lua_runtime.globals()

    def compile(self, code: bytes) -> Any:  # noqa: ANN401
        return self.lua_runtime.compile(code)  # type: ignore[arg-type]

    def execute(self, code: bytes) -> Any:  # noqa: ANN401
        try:
            return self.lua_runtime.execute(code)  # type: ignore[arg-type]
        except LuaSyntaxError as e:
            raise e
        except ServerLuaError:
            raise
        except Exception as e:
            raise e

    def eval(self, code: bytes) -> Any:  # noqa: ANN401
        return self.lua_runtime.eval(code)  # type: ignore[arg-type]

    def table(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        table_value = self.lua_runtime.table()
        for index, value in enumerate(args):
            table_value[index + 1] = value
        for key, value in kwargs.items():
            setattr(table_value, key, value)
        return table_value

    def table_from(self, *args: Any, recursive: bool = False) -> Any:  # noqa: ANN401
        return self.lua_runtime.table_from(*args, recursive=recursive)


@dataclass
class CallContext:
    readonly: bool
    commands_router: CommandsRouter
    lua_runtime: LuaRuntimeWrapper
    client_context: ClientContext


def acl_check_cmd(*args: Any, **kwargs: Any) -> bool:  # noqa: ANN401
    print("acl_check_cmd", args, kwargs)
    return False


def set_repl(*args: Any, **kwargs: Any) -> None:  # noqa: ANN401
    print("set_repl", args, kwargs)


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
