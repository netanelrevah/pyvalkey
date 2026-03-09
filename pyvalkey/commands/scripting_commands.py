import fnmatch
import json
from dataclasses import field
from typing import Any

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.commands.scripting import FunctionsEngine, ScriptsEngine
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ValueType
from pyvalkey.utils.dependencies import dependency


@command(b"eval", {b"scripting", b"slow"})
class Eval(Command):
    client_context: ClientContext = dependency()
    scripts_engine: ScriptsEngine = dependency()

    script: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    result: ValueType = field(init=False, default=None)

    async def before(self, in_multi: bool = False) -> None:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        self.result = await self.scripts_engine.eval(
            self.client_context, self.script, self.keys_and_args[: self.num_keys], self.keys_and_args[self.num_keys :]
        )

    def execute(self) -> ValueType:
        return self.result


@command(b"evalsha", {b"scripting", b"slow"})
class EvalSha(Command):
    client_context: ClientContext = dependency()
    scripts_engine: ScriptsEngine = dependency()

    sha1: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    result: ValueType = field(init=False, default=None)

    async def before(self, in_multi: bool = False) -> None:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        registered_scripts = self.scripts_engine.registered_scripts.get(self.sha1.lower())
        if registered_scripts is None:
            raise ServerError(b"NOSCRIPT No matching script. Please use EVAL.")

        self.result = await self.scripts_engine.eval(
            self.client_context,
            registered_scripts.script,
            self.keys_and_args[: self.num_keys],
            self.keys_and_args[self.num_keys :],
        )

    def execute(self) -> ValueType:
        return self.result


@command(b"evalsha_ro", {b"scripting", b"slow", b"read"})
class EvalShaReadOnly(Command):
    client_context: ClientContext = dependency()
    scripts_engine: ScriptsEngine = dependency()

    sha1: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    result: ValueType = field(init=False, default=None)

    async def before(self, in_multi: bool = False) -> None:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        registered_script = self.scripts_engine.registered_scripts.get(self.sha1.lower())
        if registered_script is None:
            raise ServerError(b"NOSCRIPT No matching script. Please use EVAL.")

        self.result = await self.scripts_engine.eval(
            self.client_context,
            registered_script.script,
            self.keys_and_args[: self.num_keys],
            self.keys_and_args[self.num_keys :],
            readonly=True,
        )

    def execute(self) -> ValueType:
        return self.result


@command(b"eval_ro", {b"scripting", b"slow", b"read"})
class EvalReadOnly(Command):
    client_context: ClientContext = dependency()
    scripts_engine: ScriptsEngine = dependency()

    script: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    result: ValueType = field(init=False, default=None)

    async def before(self, in_multi: bool = False) -> None:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        self.result = await self.scripts_engine.eval(
            self.client_context,
            self.script,
            self.keys_and_args[: self.num_keys],
            self.keys_and_args[self.num_keys :],
            readonly=True,
        )

    def execute(self) -> ValueType:
        return self.result


@command(b"fcall", {b"scripting", b"slow"})
class FunctionCall(Command):
    client_context: ClientContext = dependency()
    functions_engine: FunctionsEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter(parse_error=b"Bad number of keys provided")
    keys_and_args: list[bytes] = positional_parameter()

    result: ValueType = field(init=False, default=None)

    @classmethod
    def collect_key_arguments(cls, command_arguments: dict[str, Any]) -> list[bytes] | None:
        num_keys = command_arguments.get("num_keys")
        keys_and_args = command_arguments.get("keys_and_args")

        if num_keys is None or keys_and_args is None:
            return None

        if num_keys < 0 or num_keys > len(keys_and_args):
            return None

        return keys_and_args[:num_keys]

    async def _execute(self, readonly: bool = False) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        if len(self.keys_and_args) < self.num_keys:
            raise ServerError(b"ERR Number of keys can't be greater than number of args")

        command = f"fcall {self.function.decode()} {self.num_keys}".encode()
        if self.keys_and_args:
            command += f" {' '.join(arg.decode() for arg in self.keys_and_args)}".encode()

        return await self.functions_engine.call_function(
            command,
            self.client_context,
            self.function,
            self.keys_and_args[: self.num_keys],
            self.keys_and_args[self.num_keys :],
            readonly=readonly,
        )

    async def before(self, in_multi: bool = False) -> None:
        self.result = await self._execute(readonly=False)

    def execute(self) -> ValueType:
        return self.result


@command(b"fcall_ro", {b"scripting", b"slow"})
class ReadOnlyFunctionCall(FunctionCall):
    functions_engine: FunctionsEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    async def before(self, in_multi: bool = False) -> None:
        self.result = await self._execute(readonly=True)

    def execute(self) -> ValueType:
        return self.result


@command(
    b"flush",
    {b"scripting", b"slow"},
    parent_command=b"function",
    flags={b"write"},
)
class FunctionFlush(Command):
    functions_engine: FunctionsEngine = dependency()

    async_sync: bytes = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.async_sync is not None and self.async_sync.lower() not in [b"async", b"sync"]:
            raise ServerError(b"ERR FUNCTION FLUSH only supports SYNC|ASYNC option")

        self.functions_engine.registered_functions.clear()
        self.functions_engine.registered_libraries.clear()
        self.functions_engine.currently_running = None
        return RESP_OK


@command(b"dump", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionDump(Command):
    functions_engine: FunctionsEngine = dependency()

    def execute(self) -> ValueType:
        result = [
            {
                "library_name": library.name.decode(),
                "engine": library.engine.decode(),
                "code": library.code.decode(),
            }
            for library in self.functions_engine.registered_libraries.values()
        ]

        return json.dumps(result).encode()


@command(b"restore", {b"scripting", b"slow"}, parent_command=b"function", flags={b"write"})
class FunctionRestore(Command):
    client_context: ClientContext = dependency()
    functions_engine: FunctionsEngine = dependency()

    serialized_value: bytes = positional_parameter()
    append: bool = flag_parameter(token=b"APPEND", default=True)
    flush: bool = flag_parameter(token=b"FLUSH")
    replace: bool = flag_parameter(token=b"REPLACE")

    def execute(self) -> ValueType:
        try:
            value = json.loads(self.serialized_value)
        except json.JSONDecodeError:
            raise ServerError(b"ERR DUMP payload version or checksum are wrong")

        if self.flush:
            self.functions_engine.registered_functions.clear()
            self.functions_engine.registered_libraries.clear()
            self.functions_engine.currently_running = None

        for library in value:
            code = library["code"].encode()
            engine = library["engine"].encode()
            library_name = library["library_name"].encode()

            script = f"#!{engine.decode()} name={library_name.decode()}\n{code.decode()}"

            self.functions_engine.load_function(self.client_context, script.encode(), replace=self.replace)

        return RESP_OK


@command(
    b"load",
    {b"scripting", b"slow"},
    parent_command=b"function",
    metadata={
        CommandMetadata.PARAMETERS_LEFT_ERROR: b"Unknown option given: {next_parameter}",
    },
    flags={b"write"},
)
class FunctionLoad(Command):
    client_context: ClientContext = dependency()
    functions_engine: FunctionsEngine = dependency()

    replace: bool = flag_parameter(token=b"REPLACE")
    function_code: bytes = positional_parameter()

    def execute(self) -> ValueType:
        name = self.functions_engine.load_function(self.client_context, self.function_code, self.replace)
        return name


@command(b"delete", {b"scripting", b"slow"}, parent_command=b"function", flags={b"write"})
class FunctionDelete(Command):
    functions_engine: FunctionsEngine = dependency()

    function_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        deleted = self.functions_engine.delete_library(self.function_name)

        if not deleted:
            raise ServerError(b"ERR Library not found")
        return RESP_OK


@command(b"kill", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionKill(Command):
    functions_engine: FunctionsEngine = dependency()

    def execute(self) -> ValueType:
        if not self.functions_engine.currently_running is not None:
            raise ServerError(b"ERR No scripts in execution right now")
        self.functions_engine.kill.set()
        return RESP_OK


@command(b"kill", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptKill(Command):
    scripts_engine: ScriptsEngine = dependency()

    def execute(self) -> ValueType:
        if not self.scripts_engine.currently_running:
            raise ServerError(b"ERR No scripts in execution right now")
        self.scripts_engine.kill.set()
        return RESP_OK


@command(b"exists", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptExists(Command):
    scripts_engine: ScriptsEngine = dependency()
    sha1s: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return [1 if sha1.lower() in self.scripts_engine.registered_scripts else 0 for sha1 in self.sha1s]


@command(b"flush", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptFlush(Command):
    scripts_engine: ScriptsEngine = dependency()
    async_sync: bool | None = keyword_parameter(flag={b"ASYNC": True, b"SYNC": False}, default=None)

    def execute(self) -> ValueType:
        self.scripts_engine.registered_scripts.clear()
        return RESP_OK


@command(b"load", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptLoad(Command):
    scripts_engine: ScriptsEngine = dependency()
    script: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.scripts_engine.load(self.script)


@command(b"help", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptHelp(Command):
    def execute(self) -> ValueType:
        return [
            b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"DEBUG <YES|SYNC|NO>",
            b"    Set the debug mode for subsequent scripts executed.",
            b"EXISTS <sha1> [<sha1> ...]",
            b"    Return information about the existence of the scripts in the script cache.",
            b"FLUSH [ASYNC|SYNC]",
            b"    Flush the Lua scripts cache.",
            b"KILL",
            b"    Kill the currently executing Lua script.",
            b"LOAD <script>",
            b"    Load a script into the scripts cache without executing it.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"help", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionHelp(Command):
    def execute(self) -> ValueType:
        return [
            b"FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"LOAD <function-code>",
            b"    Create a new library with the given name and code.",
            b"DELETE <library-name>",
            b"    Delete the given library.",
            b"LIST",
            b"    Return information about all libraries.",
            b"FLUSH [ASYNC|SYNC]",
            b"    Remove all libraries.",
            b"KILL",
            b"    Kill the currently executing function.",
            b"DUMP",
            b"    Return a serialized payload of all libraries and their code.",
            b"RESTORE <payload> [REPLACE|FLUSH|APPEND]",
            b"    Restore libraries from a serialized payload.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"stats", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionStats(Command):
    functions_engine: FunctionsEngine = dependency()

    def execute(self) -> ValueType:
        res: dict = {b"running_script": {}}
        # In a real server we would track the currently running function name/command
        if self.functions_engine.currently_running is not None:
            res[b"running_script"] = {
                b"name": self.functions_engine.currently_running.register_function.function_name,
                b"command": self.functions_engine.currently_running.command,
                b"duration_ms": 0,
            }

        res[b"engines"] = {
            b"LUA": {
                b"libraries_count": len(self.functions_engine.registered_libraries),
                b"functions_count": len(self.functions_engine.registered_functions),
            }
        }
        return res


@command(
    b"list",
    {b"scripting", b"slow"},
    parent_command=b"function",
    metadata={
        CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR Unknown argument {next_parameter}",
    },
)
class FunctionList(Command):
    functions_engine: FunctionsEngine = dependency()

    library_name_pattern: bytes | None = keyword_parameter(
        token=b"LIBRARYNAME", default=None, parse_error=b"ERR library name argument was not given"
    )
    with_code: bool = flag_parameter(token=b"WITHCODE")

    def execute(self) -> ValueType:
        result = []
        for library in self.functions_engine.registered_libraries.values():
            if self.library_name_pattern:
                if self.library_name_pattern != b"*" and not fnmatch.fnmatch(
                    library.name.decode(), self.library_name_pattern.decode()
                ):
                    continue

            functions = []
            for function_name, registered_function in library.functions.items():
                functions.append(
                    {
                        b"name": function_name,
                        b"description": registered_function.description or b"",
                        b"flags": list(registered_function.flags),
                    }
                )

            res = {
                b"library_name": library.name,
                b"engine": library.engine.upper(),
                b"functions": functions,
            }
            if self.with_code:
                res[b"library_code"] = library.code
            result.append(res)

        return result
