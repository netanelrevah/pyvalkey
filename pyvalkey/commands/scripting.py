from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from traceback import print_exc
from typing import TYPE_CHECKING, Any, Self

from lupa.lua51 import LuaError, LuaSyntaxError

from pyvalkey.commands.lua.consts import LIBRARY_NAME_PATTERN
from pyvalkey.commands.lua.core import (
    CompiledFunction,
    CurrentlyRunningFunction,
    FunctionsCompiler,
    RegisteredFunction,
    RegisteredLibrary,
    RegisteredScript,
)
from pyvalkey.commands.lua.helpers import (
    CallContext,
    LuaServerError,
    convert_lua_value_to_valkey_value,
    fill_load_server_globals,
    fill_server_globals,
    lua_safe,
    register_function,
)
from pyvalkey.commands.lua.scripts import (
    FUNCTION_CALL_EXECUTOR,
    FUNCTION_LOAD_EXECUTOR,
    LUA_REGISTER_FUNCTION_WRAPPER,
    LUA_SETUP_CALL_ENV,
)
from pyvalkey.commands.utils import is_integer
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.utils.times import now_ms

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.router import CommandsRouter
    from pyvalkey.database_objects.configurations import Configurations
    from pyvalkey.resp import ValueType


@dataclass
class LuaEngineBase:
    configuration: Configurations
    commands_router: CommandsRouter

    kill: asyncio.Event = field(init=False, default_factory=asyncio.Event)
    is_busy: asyncio.Event = field(init=False, default_factory=asyncio.Event)

    _function_compiler: FunctionsCompiler = field(init=False, default_factory=FunctionsCompiler)

    _function_call_executor: CompiledFunction = field(init=False)
    _function_load_executor: CompiledFunction = field(init=False)

    @classmethod
    def create(cls, configurations: Configurations, commands_router: CommandsRouter) -> Self:
        return cls(configurations, commands_router)

    def __post_init__(self) -> None:
        self._function_call_executor = self._function_compiler.eval(FUNCTION_CALL_EXECUTOR)
        self._function_load_executor = self._function_compiler.eval(FUNCTION_LOAD_EXECUTOR)

    @lua_safe
    def check_killed(self) -> bool:
        return self.kill.is_set()

    @lua_safe
    def check_busy_reply_threshold(self, start_ms: int) -> None:
        if self.is_busy.is_set():
            return

        if self.check_timeout(start_ms):
            self.is_busy.set()

    @lua_safe
    def check_timeout(self, start_ms: int) -> bool:
        elapsed = now_ms() - start_ms
        return elapsed > self.configuration.busy_reply_threshold

    async def execute_compiled_code(
        self,
        writeable: bool,
        start_ms: int,
        client_context: ClientContext,
        compiled_func: Any,  # noqa: ANN401
        keys: list[bytes],
        argv: list[bytes],
    ) -> ValueType:
        arguments: list[int | bytes] = []
        for argument in argv:
            if is_integer(argument):
                arguments.append(int(argument))
            else:
                arguments.append(argument)

        lua_runtime = self._function_compiler.get_runtime(writeable)
        try:
            function_executor = self._function_call_executor.get(writeable)

            call_context = CallContext(not writeable, self.commands_router, lua_runtime, client_context)
            fill_server_globals(call_context)

            status, return_value = await asyncio.to_thread(
                function_executor,
                compiled_func,
                start_ms,
                self.check_busy_reply_threshold,
                self.check_killed,
                lua_runtime.table(*keys),
                lua_runtime.table(*arguments),
            )

            if status is False:
                if "Timeout reached!" in str(return_value):
                    self.is_busy.clear()
                    self.kill.clear()
                    raise TimeoutError()

                msg = return_value
                if not isinstance(msg, bytes):
                    msg = str(msg).encode()
                raise ServerError(msg)

            return convert_lua_value_to_valkey_value(return_value)

        except ServerError:
            raise
        except TimeoutError:
            raise
        except LuaServerError as e:
            raise ServerError(e.message)
        except LuaError as e:
            raise ServerError(str(e.args[0]).encode())
        except Exception as e:
            print_exc()
            raise ServerError(str(e).encode())
        finally:
            del lua_runtime.globals().server
            del lua_runtime.globals().redis


@dataclass
class ScriptsEngine(LuaEngineBase):
    commands_router: Any
    registered_scripts: dict[bytes, RegisteredScript] = field(default_factory=dict)

    currently_running: bool = field(init=False, default=False)

    def load(self, script: bytes) -> bytes:
        script_hash = hashlib.sha1(script).hexdigest().encode()
        if script_hash not in self.registered_scripts:
            self.registered_scripts[script_hash] = RegisteredScript(
                script,
                self._function_compiler.compile(script),
            )
        return script_hash

    async def eval(
        self,
        client_context: ClientContext,
        script: bytes,
        keys: list[bytes],
        argv: list[bytes],
        readonly: bool = False,
    ) -> ValueType:
        script_hash = hashlib.sha1(script).hexdigest().encode()
        if script_hash not in self.registered_scripts:
            self.registered_scripts[script_hash] = RegisteredScript(
                script,
                self._function_compiler.compile(script),
            )

        registered_script = self.registered_scripts[script_hash]
        self.currently_running = True
        try:
            return await self.execute_compiled_code(
                not readonly,
                now_ms(),
                client_context,
                registered_script.compiled_script.get(not readonly),
                keys,
                argv,
            )
        except TimeoutError:
            raise ServerError(b"ERR Script killed by user with SCRIPT KILL")
        finally:
            self.currently_running = False


@dataclass
class FunctionsEngine(LuaEngineBase):
    commands_router: Any
    registered_libraries: dict[bytes, RegisteredLibrary] = field(default_factory=dict)
    registered_functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)

    kill: asyncio.Event = field(init=False, default_factory=asyncio.Event)
    currently_running: CurrentlyRunningFunction | None = field(init=False, default=None)
    is_busy: asyncio.Event = field(init=False, default_factory=asyncio.Event)

    _load_env_setup: dict[bool, Any] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        super().__post_init__()
        for writeable in (True, False):
            lua_runtime = self._function_compiler.get_runtime(writeable)
            self._load_env_setup[writeable] = lua_runtime.eval(
                b"function()\n"
                b"  local function make_blocked(name)\n"
                b"    return setmetatable({}, { __call = function(...)\n"
                b"      error(\"Script attempted to access nonexistent global variable '\" .. name .. \"'\", 2)\n"
                b"    end })\n"
                b"  end\n"
                b"  local blocked_gm = make_blocked('getmetatable')\n"
                b"  local blocked_sm = make_blocked('setmetatable')\n"
                b"  return function(f)\n"
                b"    setfenv(f, setmetatable({\n"
                b"      getmetatable = blocked_gm,\n"
                b"      setmetatable = blocked_sm,\n"
                b"    }, {\n"
                b"      __index = _G,\n"
                b"      __newindex = function(_, k, v)\n"
                b"        error('Attempt to modify a readonly table', 2)\n"
                b"      end,\n"
                b"    }))\n"
                b"  end\n"
                b"end"
            )()

    @classmethod
    def parse_load_function_script(cls, script: bytes) -> tuple[bytes, bytes, bytes]:
        metadata, code = script.split(b"\n", 1)

        shebang, *args = metadata.split()

        if not shebang.startswith(b"#!"):
            raise ServerError(b"ERR Invalid script format, missing shebang line")
        engine = shebang[2:].lower()
        if engine != b"lua":
            raise ServerError(f"ERR Engine '{shebang[2:].decode()}' not found".encode())

        library_name = None
        for arg in args:
            if arg.startswith(b"name="):
                if library_name is not None:
                    raise ServerError(b"ERR Invalid metadata value, name argument was given multiple times")
                library_name = arg.split(b"=", 1)[1].strip(b"\"'").lower()
            else:
                raise ServerError(b"ERR Invalid metadata value given: " + arg)

        if library_name is None:
            raise ServerError(b"ERR Library name was not given")

        if LIBRARY_NAME_PATTERN.match(library_name.decode()) is None:
            raise ServerError(
                b"ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one "
                b"character long"
            )

        return engine, library_name, code

    def load_function_to_runtime(
        self, writeable: bool, library: RegisteredLibrary, client_context: ClientContext, code: bytes
    ) -> None:
        lua_runtime = self._function_compiler.get_runtime(writeable)

        call_context = CallContext(not writeable, self.commands_router, lua_runtime, client_context)

        writeable_redis_server = fill_load_server_globals(call_context)

        wrapped_register_function = lua_runtime.eval(LUA_REGISTER_FUNCTION_WRAPPER)(
            register_function, writeable, library
        )

        writeable_redis_server.register_function = wrapped_register_function

        try:
            compiled_code = lua_runtime.compile(code)
        except LuaSyntaxError:
            raise ServerError(b"ERR Error compiling function")

        self._load_env_setup[writeable](compiled_code)

        start_ms = now_ms()
        try:
            self._function_load_executor.get(writeable)(start_ms, compiled_code, self.check_timeout)
        except LuaServerError as e:
            raise ServerError(e.message)
        except LuaError as e:
            if "Timeout reached!" in str(e.args[0]):
                raise ServerError(b"ERR FUNCTION LOAD timeout")
            raise ServerError(str(e.args[0]).encode().split(b"\n")[0])
        else:
            setup_call_env = lua_runtime.eval(LUA_SETUP_CALL_ENV)
            for registered_function in library.functions.values():
                callback = registered_function.compiled_function.get(writeable)
                if callback is not None:
                    setup_call_env(callback)

            prohibit = lua_runtime.eval(b"function(msg) return function(...) error(msg, 2) end end")(
                b"server.register_function can only be called on FUNCTION LOAD command"
            )
            writeable_redis_server.register_function = prohibit
        finally:
            del lua_runtime.globals().server
            del lua_runtime.globals().redis

    def load_function(
        self, client_context: ClientContext, script: bytes, replace: bool = False, quoted_name: bool = True
    ) -> bytes:
        engine, library_name, code = self.parse_load_function_script(script)

        if not replace and library_name in self.registered_libraries:
            if quoted_name:
                raise ServerError(b"ERR Library '" + library_name + b"' already exists")
            raise ServerError(b"ERR Library " + library_name + b" already exists")

        new_library = RegisteredLibrary(library_name, code, engine)

        try:
            self.load_function_to_runtime(True, new_library, client_context, code)
            self.load_function_to_runtime(False, new_library, client_context, code)

            if not new_library.functions:
                raise ServerError(b"ERR No functions registered")
        except ServerError as e:
            raise e
        except LuaError as e:
            msg = e.message if isinstance(e, ServerError) else str(e.args[0]).encode()
            raise ServerError(b"ERR Error compiling function: " + msg)

        for function_name_str, registered_function in new_library.functions.items():
            function_name_bytes = (
                function_name_str.encode() if isinstance(function_name_str, str) else function_name_str
            )
            if function_name_bytes.lower() in self.registered_functions:
                existing_func = self.registered_functions[function_name_bytes.lower()]
                if existing_func.library_name != library_name:
                    raise ServerError(b"ERR Function " + function_name_bytes + b" already exists")
        self.registered_libraries[library_name] = new_library
        for function_name, registered_function in new_library.functions.items():
            self.registered_functions[function_name.lower()] = registered_function

        return library_name

    async def call_function(
        self,
        command: bytes,
        client_context: ClientContext,
        function_name: bytes,
        keys: list[bytes],
        argv: list[bytes],
        readonly: bool = False,
    ) -> ValueType:
        if function_name.lower() not in self.registered_functions:
            raise ServerError(b"ERR Function not found")

        registered_function = self.registered_functions[function_name.lower()]
        has_no_writes = b"no-writes" in registered_function.flags
        if readonly and not has_no_writes:
            raise ServerError(b"ERR Can not execute a script with write flag using *_ro command")
        readonly = readonly or has_no_writes
        compiled_function = registered_function.compiled_function.get(not readonly)

        if self.currently_running is not None:
            raise Exception("Another function is currently running, this should not happen")

        try:
            self.currently_running = CurrentlyRunningFunction(now_ms(), registered_function, command)
            return await self.execute_compiled_code(
                not readonly,
                self.currently_running.start_ms,
                client_context,
                compiled_function,
                keys,
                argv,
            )
        except TimeoutError:
            raise ServerError(b"ERR Script killed by user with FUNCTION KILL")
        finally:
            self.currently_running = None

    def delete_library(self, library_name: bytes) -> bool:
        library_name_lower = library_name.lower()
        if library_name_lower in self.registered_libraries:
            library = self.registered_libraries[library_name_lower]
            del self.registered_libraries[library_name_lower]
            for function_name in library.functions:
                del self.registered_functions[function_name]
            return True
        return False
