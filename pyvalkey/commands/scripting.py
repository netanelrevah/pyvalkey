from __future__ import annotations

import re
from dataclasses import dataclass, field
from traceback import print_exc
from typing import TYPE_CHECKING, Any, Self

from lupa.lua51 import LuaError

from pyvalkey.commands.lua.helpers import (
    LuaRuntimeWrapper,
    RegisteredFunction,
    ServerLuaError,
    convert_lua_value_to_valkey_value,
    create_lua_runtime,
)
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter

LIBRARY_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


@dataclass
class ScriptingEngine:
    configuration: Configurations

    registered_functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)

    _client_context: ClientContext = field(init=False)
    _commands_router: CommandsRouter = field(init=False)
    _lua_runtime: LuaRuntimeWrapper | None = field(init=False, default=None)
    _ro_lua_runtime: LuaRuntimeWrapper | None = field(init=False, default=None)
    _lua_run_with_timeout: Any = field(init=False, default=None)

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

    @property
    def lua_run_with_timeout(self) -> Any:  # noqa: ANN401
        if self._lua_run_with_timeout is None:
            self._lua_run_with_timeout = self.lua_runtime.eval(b"""
                function(f, timeout_seconds, KEYS, ARGV)
                    local start_time = os.time()
                    
                    debug.sethook(function()
                        if os.difftime(os.time(), start_time) > timeout_seconds then
                            error("Timeout reached!")
                        end
                    end, "", 500)
                    
                    local status, result = pcall(f)
            
                    debug.sethook()
            
                    return status, result
                end
            """)
        return self._lua_run_with_timeout

    def load_function(self, script: bytes, replace: bool = False) -> bytes:
        metadata, code = script.split(b"\n", 1)

        shebang, *args = metadata.split()
        if shebang.lower() != b"#!lua":
            engine = shebang[2:].decode()
            raise ServerError(f"ERR Engine '{engine}' not found".encode())
        if not args or not args[0].startswith(b"name="):
            raise Exception()
        name = args[0].split(b"=", 1)[1].lower()

        if LIBRARY_NAME_PATTERN.match(name.decode()) is None:
            raise ServerError(
                b"ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one "
                b"character long"
            )

        old_function: RegisteredFunction | None = None
        if replace and name in self.registered_functions:
            old_function = self.registered_functions.pop(name)

        try:
            self.lua_runtime.execute(code)
        except LuaError as e:
            raise ServerError(b"ERR Error compiling function: " + str(e.args[0]).encode())
        finally:
            if old_function and name not in self.registered_functions:
                self.registered_functions[name] = old_function

        return name

    def call_function(self, function_name: bytes, keys: list[bytes], argv: list[bytes | int]) -> ValueType:
        try:
            if function_name.lower() not in self.registered_functions:
                raise Exception(b"ERR Function not found")

            registered_function = self.registered_functions[function_name.lower()]
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

    def delete_function(self, function_name: bytes) -> bool:
        function_name_lower = function_name.lower()
        if function_name_lower in self.registered_functions:
            del self.registered_functions[function_name_lower]
            return True
        return False

    def eval(self, script: bytes, keys: list[bytes], argv: list[bytes]) -> ValueType:
        compiled = self.lua_runtime.compile(script)

        status, return_value = self.lua_run_with_timeout(
            compiled, self.configuration.lua_time_limit, self.lua_runtime.table(*keys), self.lua_runtime.table(*argv)
        )

        if status is False and b"Timeout reached!" in str(return_value).encode():
            raise ServerError(b"ERR Lua script execution timed out.")

        return convert_lua_value_to_valkey_value(return_value)

    @classmethod
    def create(cls, configurations: Configurations) -> Self:
        return cls(configurations)
