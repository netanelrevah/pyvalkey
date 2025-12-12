from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from hashlib import sha1
from typing import TYPE_CHECKING, Any, NoReturn

from lupa.lua51 import LuaError, LuaRuntime, lua_type, unpacks_lua_table

from pyvalkey.commands.lua.bit import register_bit_module
from pyvalkey.commands.lua.cjson import register_cjson_module
from pyvalkey.commands.lua.cmsgpack import register_cmsgpack_module
from pyvalkey.database_objects.errors import (
    RouterKeyError,
    ServerError,
    ServerWrongNumberOfArgumentsError,
    ServerWrongTypeError,
)
from pyvalkey.resp import RESP_OK, DoNotReply, RespError, RespProtocolVersion, ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.scripting import ScriptingEngine


MAX_CONVERT_DEPTH = 100


@dataclass
class RegisteredFunction:
    compiled_function: Any
    flags: list[str]


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


def register_function(scripting_manager, function_name, callback, flags=None) -> None:  # noqa: ANN001
    scripting_manager.registered_functions[function_name] = RegisteredFunction(
        callback, flags.values() if flags else []
    )


class ServerLuaError(LuaError):
    def __init__(self, message: bytes = b"") -> None:
        super().__init__(message)
        self.message = message


def _call(scripting_manager: ScriptingEngine, *args: bytes | int, readonly: bool = False) -> Any:  # noqa: ANN401
    if not args:
        return scripting_manager.lua_runtime.table(err=b"ERR Please specify at least one argument for this call script")

    command = [str(p).encode() if isinstance(p, int) else p for p in args]

    print(
        "redis.call",
        scripting_manager._client_context.current_client.client_id,
        [i[:300] for i in command],
    )

    try:
        routed_command_cls, parameters = scripting_manager._commands_router.route(command)
    except RouterKeyError:
        return scripting_manager.lua_runtime.table(
            err=f"ERR unknown command '{command[0].decode()}', "
            f"with args beginning with: {command[1].decode() if len(command) > 1 else ''}".encode()
        )

    client_context = scripting_manager._client_context.__class__(
        scripting_manager._client_context.server_context,
        scripting_manager._client_context.current_client,
        scripting_manager._client_context.scripting_manager,
        scripting_manager._client_context.subscriptions,
        scripting_manager._client_context.current_database,
        scripting_manager._client_context.current_user,
        scripting_manager._client_context.transaction_context,
        scripting_manager._client_context.client_watchlist,
        scripting_manager._client_context.protocol,
        scripting_manager._client_context.propagated_commands,
    )

    try:
        routed_command = routed_command_cls.create(parameters, client_context)
    except ServerWrongNumberOfArgumentsError:
        raise ServerLuaError(b"ERR Wrong number of args calling command from script script")

    if b"no-script" in routed_command.flags:
        raise ServerLuaError(b"ERR This Valkey command is not allowed from script")
    if b"write" in routed_command.flags and readonly is True:
        return scripting_manager.lua_runtime.table(err=b"ERR Write commands are not allowed from read-only scripts")

    try:
        result: ValueType = routed_command.execute()
    except ServerWrongTypeError:
        raise ServerLuaError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    except ServerError as e:
        return scripting_manager.lua_runtime.table(err=e.message)

    print("redis.call", scripting_manager._client_context.current_client.client_id, "result", result, type(result))

    if result is DoNotReply:
        return None
    if result == RESP_OK:
        return scripting_manager.lua_runtime.table(ok="OK")
    if result is None:
        return False
    if isinstance(result, RespError):
        return scripting_manager.lua_runtime.table(err=result)
    if isinstance(result, list):
        return scripting_manager.lua_runtime.table(*result)

    return result


@unpacks_lua_table
def call(scripting_manager: ScriptingEngine, *args: bytes | int) -> Any:  # noqa: ANN401
    print(args)
    return _call(scripting_manager, *args, readonly=False)


@unpacks_lua_table
def ro_call(scripting_manager: ScriptingEngine, *args: bytes | int) -> Any:  # noqa: ANN401
    return _call(scripting_manager, *args, readonly=True)


@unpacks_lua_table
def pcall(scripting_manager: ScriptingEngine, *args: bytes | int) -> Any:  # noqa: ANN401
    try:
        return _call(scripting_manager, *args)
    except Exception as e:
        return scripting_manager.lua_runtime.table_from({b"err": b"ERR" + str(e).encode()})


@unpacks_lua_table
def ro_pcall(scripting_manager: ScriptingEngine, *args: bytes | int) -> Any:  # noqa: ANN401
    try:
        return _call(scripting_manager, *args, readonly=True)
    except Exception as e:
        return scripting_manager.lua_runtime.table_from({b"err": b"ERR" + str(e).encode()})


@unpacks_lua_table
def sha1hex(value: bytes) -> bytes:
    return sha1(value).hexdigest().encode()


def prohibit_calling(function_name: bytes, *args: Any, **kwargs: Any) -> NoReturn:  # noqa: ANN401
    raise ServerError(f"ERR attempt to call field '{function_name.decode()}'".encode())


def set_resp(client_context: ClientContext, value: int) -> None:
    client_context.protocol = RespProtocolVersion(value)


def create_lua_runtime(scripting_manager: ScriptingEngine, readonly: bool = False) -> LuaRuntimeWrapper:
    lua_runtime = LuaRuntimeWrapper(
        LuaRuntime(
            encoding=None,
            source_encoding=None,
            register_eval=False,
            register_builtins=False,
            unpack_returned_tuples=True,
            overflow_handler=lambda value: str(value),
        )  # type: ignore[call-arg]
    )

    lua_globals = lua_runtime.globals()
    register_cjson_module(lua_runtime, lua_globals)
    register_cmsgpack_module(lua_runtime, lua_globals)
    register_bit_module(lua_runtime, lua_globals)

    sha1hex_wrapper = lua_runtime.eval(b"""
      function(f)
        local sha1hex = function(x) 
          if x == nil then
            error("wrong number of arguments")
          end
          return f(x) 
        end
        return sha1hex
      end
      """)

    call_wrapper = lua_runtime.eval(b"""
      function(f, scripting_manager)
        local call = function(command, ...) 
          return f(scripting_manager, command, unpack(arg)) 
        end
        return call
      end
      """)

    lua_globals.server = lua_runtime.table(
        register_function=unpacks_lua_table(partial(register_function, scripting_manager)),
        sha1hex=sha1hex_wrapper(sha1hex),
    )
    if readonly:
        lua_globals.server.call = call_wrapper(ro_call, scripting_manager)
        lua_globals.server.pcall = call_wrapper(ro_pcall, scripting_manager)  # type: ignore[attr-defined]
    else:
        lua_globals.server.call = call_wrapper(call, scripting_manager)  # type: ignore[attr-defined]
        lua_globals.server.pcall = call_wrapper(pcall, scripting_manager)  # type: ignore[attr-defined]

    lua_globals.server.setresp = partial(set_resp, scripting_manager._client_context)

    lua_globals.os.execute = partial(prohibit_calling, b"execute")
    lua_globals.os.exit = partial(prohibit_calling, b"exit")
    lua_globals.os.getenv = partial(prohibit_calling, b"getenv")
    lua_globals.os.remove = partial(prohibit_calling, b"remove")
    lua_globals.os.rename = partial(prohibit_calling, b"rename")
    lua_globals.os.setlocale = partial(prohibit_calling, b"setlocale")
    lua_globals.os.tmpname = partial(prohibit_calling, b"tmpname")

    lua_globals.__index = table_protection

    lua_globals.redis = lua_globals.server  # type: ignore[attr-defined]

    return lua_runtime


@unpacks_lua_table
def table_protection(*args: Any, **kwargs: Any) -> NoReturn:  # noqa: ANN401
    if len(args) != 2:  # noqa: PLR2004
        raise LuaError(b"Wrong number of arguments to luaProtectedTableError")
    if not isinstance(args[1], int | bytes):
        raise LuaError(b"Second argument to luaProtectedTableError must be a string or number")
    variable_name = str(args[1] if isinstance(args[1], int) else args[1].decode())
    raise LuaError(f"Script attempted to access nonexistent global variable '{variable_name}'".encode())


@dataclass
class LuaRuntimeWrapper:
    lua_runtime: LuaRuntime

    def globals(self) -> Any:  # noqa: ANN401
        return self.lua_runtime.globals()

    def execute(self, code: bytes) -> Any:  # noqa: ANN401
        return self.lua_runtime.execute(code)  # type: ignore[arg-type]

    def eval(self, code: bytes) -> Any:  # noqa: ANN401
        return self.lua_runtime.eval(code)  # type: ignore[arg-type]

    def table(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        table_value = self.lua_runtime.table(*args)
        for key, value in kwargs.items():
            setattr(table_value, key, value)
        return table_value

    def table_from(self, *args: Any, recursive: bool = False) -> Any:  # noqa: ANN401
        return self.lua_runtime.table_from(*args, recursive=recursive)
