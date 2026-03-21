from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pyvalkey.commands.lua.runtimes import create_lua_runtime

if TYPE_CHECKING:
    from pyvalkey.commands.context import ClientContext
    from pyvalkey.commands.lua.runtimes import LuaRuntimeWrapper
    from pyvalkey.commands.router import CommandsRouter


@dataclass
class CompiledFunction:
    writeable: Any
    readonly: Any

    def get(self, writeable: bool) -> Any:  # noqa: ANN401
        return self.writeable if writeable else self.readonly

    def set(self, writeable: bool, value: Any) -> None:  # noqa: ANN401
        if writeable:
            self.writeable = value
        else:
            self.readonly = value


@dataclass
class RegisteredFunction:
    function_name: bytes
    library_name: bytes
    flags: list[bytes]
    description: bytes

    compiled_function: CompiledFunction


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
    lua_runtime: LuaRuntimeWrapper
    client_context: ClientContext


@dataclass
class FunctionsCompiler:
    _writeable_lua_runtime: LuaRuntimeWrapper = field(default_factory=create_lua_runtime)
    _readonly_lua_runtime: LuaRuntimeWrapper = field(default_factory=create_lua_runtime)

    def eval(self, code: bytes) -> CompiledFunction:
        return CompiledFunction(
            writeable=self._writeable_lua_runtime.eval(code),
            readonly=self._readonly_lua_runtime.eval(code),
        )

    def compile(self, code: bytes) -> CompiledFunction:
        print(b"Compiling code:", code)
        return CompiledFunction(
            writeable=self._writeable_lua_runtime.compile(code),
            readonly=self._readonly_lua_runtime.compile(code),
        )

    def get_runtime(self, writeable: bool) -> LuaRuntimeWrapper:
        return self._writeable_lua_runtime if writeable else self._readonly_lua_runtime


@dataclass
class CurrentlyRunningFunction:
    start_ms: int
    register_function: RegisteredFunction
    command: bytes


@dataclass
class RegisteredScript:
    script: bytes
    compiled_script: CompiledFunction
