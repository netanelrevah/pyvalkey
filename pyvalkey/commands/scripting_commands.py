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

SHA1_HEX_LENGTH = 40


@command(
    b"eval",
    {b"scripting", b"slow"},
    flags={b"may_replicate", b"noscript", b"no_mandatory_keys", b"skip_monitor", b"stale"},
)
class Eval(Command):
    """
    summary: Executes a server-side Lua script.
    complexity: Depends on the script that is executed.
    since: 2.6.0
    function: evalCommand
    reply_schema:
      description: Return value depends on the script that is executed
    """

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


@command(
    b"evalsha",
    {b"scripting", b"slow"},
    flags={b"may_replicate", b"noscript", b"no_mandatory_keys", b"skip_monitor", b"stale"},
)
class EvalSha(Command):
    """
    summary: Executes a server-side Lua script by SHA1 digest.
    complexity: Depends on the script that is executed.
    since: 2.6.0
    function: evalShaCommand
    reply_schema:
      description: Return value depends on the script that is executed
    """

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


@command(
    b"evalsha_ro",
    {b"scripting", b"slow"},
    flags={b"noscript", b"no_mandatory_keys", b"readonly", b"skip_monitor", b"stale"},
)
class EvalShaReadOnly(Command):
    """
    summary: Executes a read-only server-side Lua script by SHA1 digest.
    complexity: Depends on the script that is executed.
    since: 7.0.0
    function: evalShaRoCommand
    reply_schema:
      description: Return value depends on the script that is executed
    """

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


@command(
    b"eval_ro",
    {b"scripting", b"slow"},
    flags={b"noscript", b"no_mandatory_keys", b"readonly", b"skip_monitor", b"stale"},
)
class EvalReadOnly(Command):
    """
    summary: Executes a read-only server-side Lua script.
    complexity: Depends on the script that is executed.
    since: 7.0.0
    function: evalRoCommand
    reply_schema:
      description: Return value depends on the script that is executed
    """

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


@command(
    b"fcall",
    {b"scripting", b"slow"},
    flags={b"may_replicate", b"noscript", b"no_mandatory_keys", b"skip_monitor", b"stale"},
)
class FunctionCall(Command):
    """
    summary: Invokes a function.
    complexity: Depends on the function that is executed.
    since: 7.0.0
    function: fcallCommand
    reply_schema:
      description: Return value depends on the function that is executed
    """

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


@command(
    b"fcall_ro",
    {b"scripting", b"slow"},
    flags={b"noscript", b"no_mandatory_keys", b"readonly", b"skip_monitor", b"stale"},
)
class ReadOnlyFunctionCall(FunctionCall):
    """
    summary: Invokes a read-only function.
    complexity: Depends on the function that is executed.
    since: 7.0.0
    function: fcallroCommand
    reply_schema:
      description: Return value depends on the function that is executed
    """

    functions_engine: FunctionsEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    async def before(self, in_multi: bool = False) -> None:
        self.result = await self._execute(readonly=True)

    def execute(self) -> ValueType:
        return self.result


@command(b"flush", {b"scripting", b"slow"}, parent_command=b"function", flags={b"noscript", b"write"})
class FunctionFlush(Command):
    """
    summary: Deletes all libraries and functions.
    complexity: O(N) where N is the number of functions deleted
    since: 7.0.0
    function: functionFlushCommand
    reply_schema:
      const: OK
    """

    functions_engine: FunctionsEngine = dependency()

    async_sync: bytes = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.async_sync is not None and self.async_sync.lower() not in [b"async", b"sync"]:
            raise ServerError(b"ERR FUNCTION FLUSH only supports SYNC|ASYNC option")

        self.functions_engine.registered_functions.clear()
        self.functions_engine.registered_libraries.clear()
        self.functions_engine.currently_running = None
        return RESP_OK


@command(b"dump", {b"scripting", b"slow"}, parent_command=b"function", flags={b"noscript"})
class FunctionDump(Command):
    """
    summary: Dumps all libraries into a serialized binary payload.
    complexity: O(N) where N is the number of functions
    since: 7.0.0
    function: functionDumpCommand
    reply_schema:
      description: The serialized payload.
      type: string
    """

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


@command(b"restore", {b"scripting", b"slow"}, parent_command=b"function", flags={b"denyoom", b"noscript", b"write"})
class FunctionRestore(Command):
    """
    summary: Restores all libraries from a payload.
    complexity: O(N) where N is the number of functions on the payload
    since: 7.0.0
    function: functionRestoreCommand
    reply_schema:
      const: OK
    """

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

            self.functions_engine.load_function(
                self.client_context, script.encode(), replace=self.replace, quoted_name=False
            )

        return RESP_OK


@command(
    b"load",
    {b"scripting", b"slow"},
    parent_command=b"function",
    flags={b"denyoom", b"noscript", b"write"},
    metadata={
        CommandMetadata.PARAMETERS_LEFT_ERROR: b"Unknown option given: {next_parameter}",
    },
)
class FunctionLoad(Command):
    """
    summary: Creates a library.
    complexity: O(1) (considering compilation time is redundant)
    since: 7.0.0
    function: functionLoadCommand
    reply_schema:
      description: The library name that was loaded
      type: string
    """

    client_context: ClientContext = dependency()
    functions_engine: FunctionsEngine = dependency()

    replace: bool = flag_parameter(token=b"REPLACE")
    function_code: bytes = positional_parameter()

    def execute(self) -> ValueType:
        name = self.functions_engine.load_function(self.client_context, self.function_code, self.replace)
        return name


@command(b"delete", {b"scripting", b"slow"}, parent_command=b"function", flags={b"noscript", b"write"})
class FunctionDelete(Command):
    """
    summary: Deletes a library and its functions.
    complexity: O(1)
    since: 7.0.0
    function: functionDeleteCommand
    reply_schema:
      const: OK
    """

    functions_engine: FunctionsEngine = dependency()

    function_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        deleted = self.functions_engine.delete_library(self.function_name)

        if not deleted:
            raise ServerError(b"ERR Library not found")
        return RESP_OK


@command(b"kill", {b"scripting", b"slow"}, parent_command=b"function", flags={b"allow_busy", b"noscript"})
class FunctionKill(Command):
    """
    summary: Terminates a function during execution.
    complexity: O(1)
    since: 7.0.0
    function: functionKillCommand
    reply_schema:
      const: OK
    """

    functions_engine: FunctionsEngine = dependency()

    def execute(self) -> ValueType:
        if self.functions_engine.currently_running is None:
            raise ServerError(b"ERR No scripts in execution right now")
        self.functions_engine.kill.set()
        return RESP_OK


@command(b"kill", {b"scripting", b"slow"}, parent_command=b"script", flags={b"allow_busy", b"noscript"})
class ScriptKill(Command):
    """
    summary: Terminates a server-side Lua script during execution.
    complexity: O(1)
    since: 2.6.0
    function: scriptCommand
    reply_schema:
      const: OK
    """

    scripts_engine: ScriptsEngine = dependency()

    def execute(self) -> ValueType:
        if not self.scripts_engine.currently_running:
            raise ServerError(b"ERR No scripts in execution right now")
        self.scripts_engine.kill.set()
        return RESP_OK


@command(b"exists", {b"scripting", b"slow"}, parent_command=b"script", flags={b"noscript", b"stale"})
class ScriptExists(Command):
    """
    summary: Determines whether server-side Lua scripts exist in the script cache.
    complexity: >-
      O(N) with N being the number of scripts to check (so checking a single script is an O(1) operation).
    since: 2.6.0
    function: scriptCommand
    reply_schema:
      description: An array of integers that correspond to the specified SHA1 digest arguments.
      type: array
      items:
        oneOf:
        - description: Sha1 hash exists in script cache.
          const: 1
        - description: Sha1 hash does not exist in script cache.
          const: 0
    """

    scripts_engine: ScriptsEngine = dependency()
    sha1s: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return [1 if sha1.lower() in self.scripts_engine.registered_scripts else 0 for sha1 in self.sha1s]


@command(b"flush", {b"scripting", b"slow"}, parent_command=b"script", flags={b"noscript", b"stale"})
class ScriptFlush(Command):
    """
    summary: Removes all server-side Lua scripts from the script cache.
    complexity: O(N) with N being the number of scripts in cache
    since: 2.6.0
    function: scriptCommand
    reply_schema:
      const: OK
    """

    scripts_engine: ScriptsEngine = dependency()
    async_sync: bool | None = keyword_parameter(flag={b"ASYNC": True, b"SYNC": False}, default=None)

    def execute(self) -> ValueType:
        self.scripts_engine.registered_scripts.clear()
        return RESP_OK


@command(b"show", {b"scripting", b"slow"}, parent_command=b"script", flags={b"noscript", b"stale"})
class ScriptShow(Command):
    """
    summary: Show server-side Lua script in the script cache.
    complexity: O(1).
    since: 8.0.0
    function: scriptCommand
    reply_schema:
      description: Lua script if sha1 hash exists in script cache.
      type: string
    """

    scripts_engine: ScriptsEngine = dependency()
    sha1: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if len(self.sha1) != SHA1_HEX_LENGTH or not all(c in b"0123456789abcdefABCDEF" for c in self.sha1):
            raise ServerError(b"NOSCRIPT No matching script. Please use EVAL.")
        script_hash = self.sha1.lower()
        if script_hash not in self.scripts_engine.registered_scripts:
            raise ServerError(b"NOSCRIPT No matching script. Please use EVAL.")
        return self.scripts_engine.registered_scripts[script_hash].script


@command(b"load", {b"scripting", b"slow"}, parent_command=b"script", flags={b"noscript", b"stale"})
class ScriptLoad(Command):
    """
    summary: Loads a server-side Lua script to the script cache.
    complexity: O(N) with N being the length in bytes of the script body.
    since: 2.6.0
    function: scriptCommand
    reply_schema:
      description: The SHA1 digest of the script added into the script cache
      type: string
    """

    scripts_engine: ScriptsEngine = dependency()
    script: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.scripts_engine.load(self.script)


@command(b"debug", {b"scripting", b"slow"}, parent_command=b"script", flags={b"noscript"})
class ScriptDebug(Command):
    """
    summary: Sets the debug mode of server-side Lua scripts.
    complexity: O(1)
    since: 3.2.0
    function: scriptCommand
    reply_schema:
      const: OK
    """

    mode: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"help", {b"scripting", b"slow"}, parent_command=b"script", flags={b"loading", b"stale"})
class ScriptHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: scriptCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

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


@command(b"help", {b"scripting", b"slow"}, parent_command=b"function", flags={b"loading", b"stale"})
class FunctionHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 7.0.0
    function: functionHelpCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

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


@command(b"stats", {b"scripting", b"slow"}, parent_command=b"function", flags={b"allow_busy", b"noscript"})
class FunctionStats(Command):
    """
    summary: Returns information about a function during execution.
    complexity: O(1)
    since: 7.0.0
    function: functionStatsCommand
    reply_schema:
      type: object
      additionalProperties: false
      properties:
        running_script:
          description: Information about the running script.
          oneOf:
          - description: If there's no in-flight function.
            type: 'null'
          - description: A map with the information about the running script.
            type: object
            additionalProperties: false
            properties:
              name:
                description: The name of the function.
                type: string
              command:
                description: The command and arguments used for invoking the function.
                type: array
                items:
                  type: string
              duration_ms:
                description: The function's runtime duration in milliseconds.
                type: integer
        engines:
          description: A map when each entry in the map represent a single engine.
          type: object
          patternProperties:
            ^.*$:
              description: Engine map contains statistics about the engine.
              type: object
              additionalProperties: false
              properties:
                libraries_count:
                  description: Number of libraries.
                  type: integer
                functions_count:
                  description: Number of functions.
                  type: integer
    """

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
    flags={b"noscript"},
    metadata={
        CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR Unknown argument {next_parameter}",
    },
)
class FunctionList(Command):
    """
    summary: Returns information about all libraries.
    complexity: O(N) where N is the number of functions
    since: 7.0.0
    function: functionListCommand
    reply_schema:
      type: array
      items:
        type: object
        additionalProperties: false
        properties:
          library_name:
            description: The name of the library.
            type: string
          engine:
            description: The engine of the library.
            type: string
          functions:
            description: The list of functions in the library.
            type: array
            items:
              type: object
              additionalProperties: false
              properties:
                name:
                  description: The name of the function.
                  type: string
                description:
                  description: The function's description.
                  oneOf:
                  - type: 'null'
                  - type: string
                flags:
                  description: An array of function flags.
                  type: array
                  items:
                    type: string
          library_code:
            description: The library's source code (when given the WITHCODE modifier).
            type: string
    """

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
