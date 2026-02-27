from dataclasses import dataclass
from functools import partial
from typing import Any, NoReturn

from lupa.lua51 import LuaError, LuaRuntime, LuaSyntaxError

from pyvalkey.commands.lua.bit import register_bit_module
from pyvalkey.commands.lua.cjson import register_cjson_module
from pyvalkey.commands.lua.cmsgpack import register_cmsgpack_module
from pyvalkey.commands.lua.errors import LuaServerError


def prohibit_calling(function_name: bytes, *_: Any) -> NoReturn:  # noqa: ANN401
    raise LuaServerError(f"ERR attempt to call field '{function_name.decode()}'".encode())


def table_protection(*args: Any) -> NoReturn:  # noqa: ANN401
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
        except LuaServerError:
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
