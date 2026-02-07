import json

from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import flag_parameter, positional_parameter
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


@command(b"fcall", {b"scripting", b"slow"})
class FunctionCall(Command):
    scripting_engine: ScriptingEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter(parse_error=b"Bad number of keys provided")
    keys_and_args: list[bytes] = positional_parameter()

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
            for function_name, registered_function in library.functions.items():
                result.append(
                    {
                        "library_name": registered_function.library_name.decode(),
                        "engine": "LUA",
                        "functions": [
                            {
                                "name": function_name.decode(),
                                "description": "",
                                "flags": list(registered_function.flags),
                            }
                        ],
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

        if self.flush and self.append and self.replace:
            raise ServerError(b"ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.")

        if self.flush:
            self.scripting_engine.registered_functions.clear()
            self.scripting_engine.registered_libraries.clear()
            self.scripting_engine.currently_running = False

        for library in value:
            code = library["code"].encode()

            script = f"#!{library['engine'].lower()} name={library['library_name']}\n{code.decode()}"

            self.scripting_engine.load_function(script.encode(), replace=self.replace)

        return RESP_OK


@command(
    b"load",
    {b"scripting", b"slow"},
    parent_command=b"function",
    metadata={
        CommandMetadata.PARAMETERS_LEFT_ERROR: b"Unknown option given: {next_parameter}",
    },
    flags={b"write"}
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


@command(b"help", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptHelp(Command):
    def execute(self) -> ValueType:
        return [
            b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"EXISTS <sha1> [<sha1> ...]",
            b"    Return information about the existence of the scripts in the script cache.",
            b"FLUSH [ASYNC|SYNC]",
            b"    Remove all the scripts from the script cache.",
            b"KILL",
            b"    Kill the currently executing Lua script.",
            b"LOAD <script>",
            b"    Load a script into the script cache, without executing it.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"flush", {b"scripting", b"slow"}, parent_command=b"script")
class ScriptFlush(Command):
    scripting_engine: ScriptingEngine = dependency()

    def execute(self) -> ValueType:
        return RESP_OK


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


@command(b"list", {b"scripting", b"slow"}, parent_command=b"function")
class FunctionList(Command):
    scripting_engine: ScriptingEngine = dependency()

    def execute(self) -> ValueType:
        result = []
        for library in self.scripting_engine.registered_libraries.values():
            for function_name, registered_function in library.functions.items():
                result.append(
                    {
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
                )

        return result
