from __future__ import annotations

from dataclasses import dataclass, field
from traceback import print_exc
from typing import TYPE_CHECKING, Self

from lupa.lua51 import LuaError

from pyvalkey.commands.lua.helpers import (
    LuaRuntimeWrapper,
    RegisteredFunction,
    ServerLuaError,
    convert_lua_value_to_valkey_value,
    create_lua_runtime,
)
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter


@dataclass
class ScriptingEngine:
    registered_functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)

    _client_context: ClientContext = field(init=False)
    _commands_router: CommandsRouter = field(init=False)
    _lua_runtime: LuaRuntimeWrapper | None = field(init=False, default=None)
    _ro_lua_runtime: LuaRuntimeWrapper | None = field(init=False, default=None)

    @property
    def lua_runtime(self) -> LuaRuntimeWrapper:
        if self._lua_runtime is None:
            self._lua_runtime = create_lua_runtime(self)
        return self._lua_runtime

    @property
    def ro_lua_runtime(self) -> LuaRuntimeWrapper:
        if self._lua_runtime is None:
            self._lua_runtime = create_lua_runtime(self, readonly=True)
        return self._lua_runtime

    def load_function(self, script: bytes) -> bytes:
        metadata, code = script.split(b"\n", 1)

        shebang, *args = metadata.split()
        if shebang != b"#!lua":
            raise Exception()
        if not args or not args[0].startswith(b"name="):
            raise Exception()
        name = args[0].split(b"=", 1)[1]

        self.lua_runtime.execute(code)

        return name

    def call_function(self, function_name: bytes, keys: list[bytes], argv: list[bytes | int]) -> ValueType:
        try:
            registered_function = self.registered_functions[function_name]
            if "no-writes" in registered_function.flags:
                lua_runtime = self.ro_lua_runtime
            else:
                lua_runtime = self.lua_runtime

            print("call_function", keys, argv)
            return_value = registered_function.compiled_function(lua_runtime.table(*keys), lua_runtime.table(*argv))
        except ServerLuaError as e:
            raise ServerError(e.message)
        except LuaError as e:
            raise ServerError(str(e.args[0]).encode())
        except Exception as e:
            print_exc()
            raise ServerError(str(e).encode())
        return convert_lua_value_to_valkey_value(return_value)

    def eval(self, script: bytes, keys: list[bytes], argv: list[bytes]) -> ValueType:
        self.lua_runtime.globals().KEYS = self.lua_runtime.table(*keys)  # type: ignore[attr-defined]
        self.lua_runtime.globals().ARGV = self.lua_runtime.table(*argv)  # type: ignore[attr-defined]

        return_value = self.lua_runtime.execute(script)

        self.lua_runtime.globals().KEYS = None  # type: ignore[attr-defined]
        self.lua_runtime.globals().ARGV = None  # type: ignore[attr-defined]

        return convert_lua_value_to_valkey_value(return_value)

    @classmethod
    def create(cls) -> Self:
        return cls()
