from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from lua_runtime import LuaError, LuaRuntime

from pyvalkey.commands.lua.errors import LuaServerError

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter


def table_protection(*args: Any) -> NoReturn:  # noqa: ANN401
    if len(args) != 2:  # noqa: PLR2004
        raise LuaError("Wrong number of arguments to luaProtectedTableError")
    if not isinstance(args[1], int | bytes):
        raise LuaError("Second argument to luaProtectedTableError must be a string or number")
    variable_name = str(args[1] if isinstance(args[1], int) else args[1].decode())
    raise LuaError(f"Script attempted to access nonexistent global variable '{variable_name}'")


def create_lua_runtime() -> LuaRuntime:
    lua_runtime = LuaRuntime(
        register_eval=False,
        register_builtins=False,
        unpack_returned_tuples=True,
        overflow_handler=lambda value: str(value),
    )

    lua_globals = lua_runtime.globals()

    os_clock = lua_globals.os.clock
    lua_runtime.execute(b"os = {}")
    lua_globals.os.clock = os_clock

    def _os_prohibit(_, key: bytes, *__: Any) -> NoReturn:  # noqa: ANN401
        name = key.decode() if isinstance(key, bytes) else str(key)
        raise LuaServerError(f"ERR attempt to call field '{name}'".encode())

    lua_runtime.eval(b"""
        function(prohibit)
            setmetatable(os, {__index = function(t, k) prohibit(t, k) end})
        end
    """)(_os_prohibit)

    lua_runtime.execute(b"loadfile = nil; dofile = nil; print = nil")

    lua_runtime.eval(b"""
        function(protection)
            setmetatable(_G, {
                __index = function(t, k) return protection(t, k) end,
            })
        end
    """)(table_protection)

    return lua_runtime


@dataclass
class RegisteredFunction:
    function_name: bytes
    library_name: bytes
    flags: list[bytes]
    description: bytes
    compiled_function: Any


@dataclass
class RegisteredLibrary:
    name: bytes
    code: bytes
    engine: bytes
    functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)


@dataclass
class CallContext:
    readonly: bool
    commands_router: CommandsRouter
    lua_runtime: LuaRuntime
    client_context: ClientContext


@dataclass
class FunctionsCompiler:
    lua_runtime: LuaRuntime = field(default_factory=create_lua_runtime)

    def eval(self, code: bytes) -> Any:  # noqa: ANN401
        return self.lua_runtime.eval(code)

    def compile(self, code: bytes) -> Any:  # noqa: ANN401
        return self.lua_runtime.compile(code)


@dataclass
class CurrentlyRunningFunction:
    start_ms: int
    register_function: RegisteredFunction
    command: bytes


@dataclass
class RegisteredScript:
    script: bytes
    compiled_script: Any
