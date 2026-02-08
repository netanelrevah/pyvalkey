from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from functools import partial
from traceback import print_exc
from typing import TYPE_CHECKING, Any, Self

from lupa.lua51 import LuaError, unpacks_lua_table

from pyvalkey.commands.lua.helpers import (
    LuaRuntimeWrapper,
    ServerLuaError,
    convert_lua_value_to_valkey_value,
    create_lua_runtime,
    register_function,
)
from pyvalkey.commands.lua.scripts import LUA_RUN_WITH_TIMEOUT
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter

LIBRARY_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


@dataclass
class RegisteredFunction:
    library_name: bytes
    compiled_function: Any | None
    readonly_compiled_function: Any | None
    flags: list[str]


@dataclass
class RegisteredLibrary:
    name: bytes
    code: bytes
    engine: bytes
    functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)


@dataclass
class ScriptingEngine:
    configuration: Configurations

    registered_libraries: dict[bytes, RegisteredLibrary] = field(default_factory=dict)
    registered_functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)
    script_cache: dict[bytes, bytes] = field(default_factory=dict)

    should_stop: bool = field(init=False, default=False)
    currently_running: bool = field(init=False, default=False)

    _client_context: ClientContext = field(init=False)
    _commands_router: CommandsRouter = field(init=False)
    _lua_runtime: LuaRuntimeWrapper | None = field(init=False, default=None)
    _ro_lua_runtime: LuaRuntimeWrapper | None = field(init=False, default=None)
    _lua_run_with_timeout: Any = field(init=False, default=None)
    _readonly_lua_run_with_timeout: Any = field(init=False, default=None)

    @property
    def lua_runtime(self) -> LuaRuntimeWrapper:
        if self._lua_runtime is None:
            self._lua_runtime = create_lua_runtime(self)
        return self._lua_runtime

    @property
    def ro_lua_runtime(self) -> LuaRuntimeWrapper:
        if self._ro_lua_runtime is None:
            self._ro_lua_runtime = create_lua_runtime(self, readonly=True)
        return self._ro_lua_runtime

    @property
    def mortal_lua_function_wrapper(self) -> Any:  # noqa: ANN401
        if self._lua_run_with_timeout is None:
            self._lua_run_with_timeout = self.lua_runtime.eval(LUA_RUN_WITH_TIMEOUT)
        return self._lua_run_with_timeout

    @property
    def readonly_mortal_lua_function_wrapper(self) -> Any:  # noqa: ANN401
        if self._readonly_lua_run_with_timeout is None:
            self._readonly_lua_run_with_timeout = self.ro_lua_runtime.eval(LUA_RUN_WITH_TIMEOUT)
        return self._readonly_lua_run_with_timeout

    def register_function(
        self,
        library_name: bytes,
        readonly: bool,
        function_name: bytes,
        callback: Any,  # noqa: ANN401
        flags: list[str] | None = None,
    ) -> None:
        registered_function = None
        if function_name.lower() in self.registered_functions:
            registered_function = self.registered_functions[function_name.lower()]

            if (not readonly and registered_function.compiled_function is not None) or (
                readonly and registered_function.readonly_compiled_function is not None
            ):
                raise ServerError(b"ERR Library already exists")

        if registered_function is None:
            registered_function = RegisteredFunction(
                library_name, callback if not readonly else None, callback if readonly else None, flags or []
            )
            self.registered_libraries[library_name].functions[function_name.lower()] = registered_function
            self.registered_functions[function_name.lower()] = registered_function
        elif not readonly:
            registered_function.compiled_function = callback
        elif readonly:
            registered_function.readonly_compiled_function = callback

    def parse_load_function_script(self, script: bytes) -> tuple[bytes, bytes, bytes]:
        metadata, code = script.split(b"\n", 1)

        shebang, *args = metadata.split()

        if not shebang.startswith(b"#!"):
            raise ServerError(b"ERR Invalid script format, missing shebang line")
        engine = shebang[2:].lower()
        if engine != b"lua":
            raise ServerError(f"ERR Engine '{shebang[2:].decode()}' not found".encode())

        if not args or not args[0].startswith(b"name="):
            raise Exception()
        library_name = args[0].split(b"=", 1)[1].lower()

        if LIBRARY_NAME_PATTERN.match(library_name.decode()) is None:
            raise ServerError(
                b"ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one "
                b"character long"
            )

        return engine, library_name, code

    def load_function_to_runtime(self, lua_runtime: LuaRuntimeWrapper, library_name: bytes, code: bytes) -> None:
        readonly = lua_runtime is self.ro_lua_runtime

        lua_runtime.globals().server.register_function = unpacks_lua_table(
            partial(register_function, self, readonly, library_name)
        )
        try:
            lua_runtime.execute(code)
        finally:
            lua_runtime.globals().server.register_function = None

    def load_function(self, script: bytes, replace: bool = False) -> bytes:
        engine, library_name, code = self.parse_load_function_script(script)

        old_library: RegisteredLibrary | None = None
        if replace and library_name in self.registered_libraries:
            old_library = self.registered_libraries.pop(library_name)
            for function_name in old_library.functions:
                del self.registered_functions[function_name]

        if library_name not in self.registered_libraries:
            self.registered_libraries[library_name] = RegisteredLibrary(library_name, code, engine)

        try:
            self.load_function_to_runtime(self.lua_runtime, library_name, code)
            self.load_function_to_runtime(self.ro_lua_runtime, library_name, code)
        except LuaError as e:
            self.registered_libraries.pop(library_name, None)
            raise ServerError(b"ERR Error compiling function: " + str(e.args[0]).encode())
        finally:
            if old_library and library_name not in self.registered_libraries:
                self.registered_libraries[library_name] = old_library
                for function_name, registered_function in old_library.functions.items():
                    self.registered_functions[function_name] = registered_function

        return library_name

    def call_function(
        self, function_name: bytes, keys: list[bytes], argv: list[bytes | int], readonly: bool = False
    ) -> ValueType:
        if function_name.lower() not in self.registered_functions:
            raise ServerError(b"ERR Function not found")

        try:
            registered_function = self.registered_functions[function_name.lower()]
            if "no-writes" in registered_function.flags or readonly:
                lua_runtime = self.ro_lua_runtime
                if registered_function.readonly_compiled_function is None:
                    raise Exception()
                compiled_func = registered_function.readonly_compiled_function
            else:
                lua_runtime = self.lua_runtime
                if registered_function.compiled_function is None:
                    raise Exception()
                compiled_func = registered_function.compiled_function

            print("call_function", keys, argv)
            return_value = compiled_func(lua_runtime.table(*keys), lua_runtime.table(*argv))
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
            del self.registered_libraries[self.registered_functions[function_name_lower].library_name].functions[
                function_name_lower
            ]
            del self.registered_functions[function_name_lower]
            return True
        return False


    def eval(self, script: bytes, keys: list[bytes], argv: list[bytes], readonly: bool = False) -> ValueType:
        sha1 = hashlib.sha1(script).hexdigest().encode()
        self.script_cache[sha1] = script

        lua_runtime = self.ro_lua_runtime if readonly else self.lua_runtime
        mortal_wrapper = self.readonly_mortal_lua_function_wrapper if readonly else self.mortal_lua_function_wrapper
        compiled = lua_runtime.compile(script)

        self.currently_running = True
        status, return_value = mortal_wrapper(
            compiled,
            self.configuration.busy_reply_threshold,
            lambda: self.should_stop,
            lua_runtime.table(*keys),
            lua_runtime.table(*argv),
        )
        self.currently_running = False

        if status is False:
            if b"Timeout reached!" in str(return_value).encode():
                raise ServerError(b"ERR Lua script execution timed out.")

            msg = return_value
            if not isinstance(msg, bytes):
                msg = str(msg).encode()
            raise ServerError(msg)

        return convert_lua_value_to_valkey_value(return_value)

    @classmethod
    def create(cls, configurations: Configurations) -> Self:
        return cls(configurations)
