import json
import hashlib
from typing import Any

from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.commands.scripting import ScriptingEngine
from pyvalkey.commands.utils import is_integer
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ValueType


@command(b"eval", {b"scripting", b"slow"})
class Eval(Command):
    scripting_engine: ScriptingEngine = dependency()

    script: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        return self.scripting_engine.eval(
            self.script, self.keys_and_args[: self.num_keys], self.keys_and_args[self.num_keys :]
        )


@command(b"evalsha", {b"scripting", b"slow"})
class EvalSha(Command):
    scripting_engine: ScriptingEngine = dependency()

    sha1: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        script = self.scripting_engine.script_cache.get(self.sha1.lower())
        if script is None:
            raise ServerError(b"NOSCRIPT No matching script. Please use EVAL.")

        return self.scripting_engine.eval(
            script, self.keys_and_args[: self.num_keys], self.keys_and_args[self.num_keys :]
        )


@command(b"evalsha_ro", {b"scripting", b"slow", b"read"})
class EvalShaReadOnly(Command):
    scripting_engine: ScriptingEngine = dependency()

    sha1: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        script = self.scripting_engine.script_cache.get(self.sha1.lower())
        if script is None:
            raise ServerError(b"NOSCRIPT No matching script. Please use EVAL.")

        # Note: scripting_engine.eval currently doesn't take a readonly flag, 
        # but the commands flags {b"read"} should handle it if implemented in engine.
        return self.scripting_engine.eval(
            script, self.keys_and_args[: self.num_keys], self.keys_and_args[self.num_keys :], readonly=True
        )


@command(b"eval_ro", {b"scripting", b"slow", b"read"})
class EvalReadOnly(Command):
    scripting_engine: ScriptingEngine = dependency()

    script: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        # Note: scripting_engine.eval currently doesn't take a readonly flag, 
        # but it will use the default lua_runtime which is NOT the ro_lua_runtime.
        # However, the command flag {b"read"} should ideally enforce this.
        return self.scripting_engine.eval(
            self.script, self.keys_and_args[: self.num_keys], self.keys_and_args[self.num_keys :], readonly=True
        )


@command(b"fcall", {b"scripting", b"slow"})
class FunctionCall(Command):
    scripting_engine: ScriptingEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter(parse_error=b"Bad number of keys provided")
    keys_and_args: list[bytes] = positional_parameter()

    @classmethod
    def collect_key_arguments(cls, command_arguments: dict[str, Any]) -> list[bytes] | None:
        num_keys = command_arguments.get("num_keys")
        keys_and_args = command_arguments.get("keys_and_args")

        if num_keys is None or keys_and_args is None:
            return None

        if num_keys < 0 or num_keys > len(keys_and_args):
            return None

        return keys_and_args[:num_keys]

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        if len(self.keys_and_args) < self.num_keys:
            raise ServerError(b"ERR Number of keys can't be greater than number of args")

        arguments: list[int | bytes] = []
        for argument in self.keys_and_args[self.num_keys :]:
            if is_integer(argument):
                arguments.append(int(argument))
            else:
                arguments.append(argument)

        return self.scripting_engine.call_function(self.function, self.keys_and_args[: self.num_keys], arguments)


@command(b"fcall_ro", {b"scripting", b"slow"})
class ReadOnlyFunctionCall(Command):
    scripting_engine: ScriptingEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    @classmethod
    def collect_key_arguments(cls, command_arguments: dict[str, Any]) -> list[bytes] | None:
        num_keys = command_arguments.get("num_keys")
        keys_and_args = command_arguments.get("keys_and_args")

        if num_keys is None or keys_and_args is None:
            return None

        if num_keys < 0 or num_keys > len(keys_and_args):
            return None

        return keys_and_args[:num_keys]

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        if len(self.keys_and_args) < self.num_keys:
            raise ServerError(b"ERR Number of keys can't be greater than number of args")

        arguments: list[int | bytes] = []
        for argument in self.keys_and_args[self.num_keys :]:
            if is_integer(argument):
                arguments.append(int(argument))
            else:
                arguments.append(argument)

        return self.scripting_engine.call_function(
            self.function, self.keys_and_args[: self.num_keys], arguments, readonly=True
        )


@command(b"flush", {b"scripting", b"slow"}, parent_command=b"function", flags={b"write"})
class FunctionFlush(Command):
    scripting_engine: ScriptingEngine = dependency()

    async_sync: bool | None = keyword_parameter(flag={b"ASYNC": True, b"SYNC": False}, default=None)

    def execute(self) -> ValueType:
        self.scripting_engine.registered_functions.clear()
        self.scripting_engine.registered_libraries.clear()
        self.scripting_engine.currently_running = False
        return RESP_OK


@command(b"dump", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionDump(Command):
    scripting_engine: ScriptingEngine = dependency()

    def execute(self) -> ValueType:
        result = []
        for library in self.scripting_engine.registered_libraries.values():
            result.append(
                {
                    "library_name": library.name.decode(),
                    "engine": library.engine.decode(),
                    "code": library.code.decode(),
                }
            )

        return json.dumps(result).encode()


@command(b"restore", {b"scripting", b"slow"}, parent_command=b"function", flags={b"write"})
class FunctionRestore(Command):
    scripting_engine: ScriptingEngine = dependency()

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
            self.scripting_engine.registered_functions.clear()
            self.scripting_engine.registered_libraries.clear()
            self.scripting_engine.currently_running = False

        for library in value:
            code = library["code"].encode()
            engine = library["engine"].encode()
            library_name = library["library_name"].encode()

            script = f"#!{engine.decode()} name={library_name.decode()}\n{code.decode()}"

            self.scripting_engine.load_function(script.encode(), replace=self.replace)

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
    scripting_engine: ScriptingEngine = dependency()

    replace: bool = flag_parameter(token=b"REPLACE")
    function_code: bytes = positional_parameter()

    def execute(self) -> ValueType:
        name = self.scripting_engine.load_function(self.function_code, self.replace)
        return name


@command(b"delete", {b"scripting", b"slow"}, parent_command=b"function", flags={b"write"})
class FunctionDelete(Command):
    scripting_engine: ScriptingEngine = dependency()

    function_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        deleted = self.scripting_engine.delete_function(self.function_name)

        if not deleted:
            raise ServerError(b"ERR Library not found")
        return RESP_OK


@command(b"kill", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionKill(Command):
    scripting_engine: ScriptingEngine = dependency()

    def execute(self) -> ValueType:
        if not self.scripting_engine.currently_running:
            raise ServerError(b"ERR No scripts in execution right now")
        self.scripting_engine.should_stop = True
        return RESP_OK


@command(b"kill", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptKill(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"exists", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptExists(Command):
    scripting_engine: ScriptingEngine = dependency()
    sha1s: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return [1 if sha1.lower() in self.scripting_engine.script_cache else 0 for sha1 in self.sha1s]


@command(b"flush", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptFlush(Command):
    scripting_engine: ScriptingEngine = dependency()
    async_sync: bool | None = keyword_parameter(flag={b"ASYNC": True, b"SYNC": False}, default=None)

    def execute(self) -> ValueType:
        self.scripting_engine.script_cache.clear()
        return RESP_OK


@command(b"load", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptLoad(Command):
    scripting_engine: ScriptingEngine = dependency()
    script: bytes = positional_parameter()

    def execute(self) -> ValueType:
        sha1 = hashlib.sha1(self.script).hexdigest().encode()
        self.scripting_engine.script_cache[sha1] = self.script
        return sha1


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
    scripting_engine: ScriptingEngine = dependency()

    def execute(self) -> ValueType:
        engines = {
            b"LUA": {
                b"libraries_count": len(self.scripting_engine.registered_libraries),
                b"functions_count": len(self.scripting_engine.registered_functions),
            }
        }
        res = {
            b"running_script": None,
            b"engines": engines,
        }
        # In a real server we would track the currently running function name/command
        if self.scripting_engine.currently_running:
            res[b"running_script"] = {
                b"duration_ms": 0,
            }
        return res


@command(b"list", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionList(Command):
    scripting_engine: ScriptingEngine = dependency()

    library_name_pattern: bytes | None = keyword_parameter(token=b"LIBRARYNAME", default=None)
    with_code: bool = flag_parameter(token=b"WITHCODE")

    def execute(self) -> ValueType:
        result = []
        for library in self.scripting_engine.registered_libraries.values():
            if self.library_name_pattern:
                # Simple pattern match (could use fnmatch for real glob support)
                if self.library_name_pattern != b"*" and self.library_name_pattern not in library.name:
                    continue

            for function_name, registered_function in library.functions.items():
                res = {
                    b"library_name": registered_function.library_name,
                    b"engine": b"LUA",
                    b"functions": [
                        {
                            b"name": function_name,
                            b"description": b"",
                            b"flags": list(registered_function.flags),
                        }
                    ],
                }
                if self.with_code:
                    res[b"library_code"] = library.code
                result.append(res)

        return result
