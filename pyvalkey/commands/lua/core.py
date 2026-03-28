from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from lua_runtime import LuaRuntime

from pyvalkey.commands.lua.errors import LuaServerError

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter


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
    lua_runtime.execute(b"__pyv_state = {}")

    for mod in (b"cjson", b"cmsgpack", b"bit", b"struct"):
        lua_globals[mod].set_readonly(True)

    g_metatable = lua_runtime.eval(b"""
        function()
            local mt = {
                __index = function(t, k)
                    error(("Script attempted to access nonexistent global variable '%s'")
                          :format(tostring(k)), 2)
                end,
            }
            setmetatable(_G, mt)
            return mt
        end
    """)()
    g_metatable.set_readonly(True)

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
