from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.scripting import ScriptingEngine
from pyvalkey.commands.utils import is_integer
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ValueType


@command(b"eval", {b"connection", b"fast"})
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


@command(b"fcall", {b"connection", b"fast"})
class FunctionCall(Command):
    scripting_engine: ScriptingEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        arguments: list[int | bytes] = []
        for argument in self.keys_and_args[self.num_keys :]:
            if is_integer(argument):
                arguments.append(int(argument))
            else:
                arguments.append(argument)

        return self.scripting_engine.call_function(self.function, self.keys_and_args[: self.num_keys], arguments)


@command(b"fcall_ro", {b"connection", b"fast"})
class ReadOnlyFunctionCall(Command):
    scripting_engine: ScriptingEngine = dependency()

    function: bytes = positional_parameter()
    num_keys: int = positional_parameter()
    keys_and_args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if self.num_keys < 0:
            raise ServerError(b"ERR Number of keys can't be negative")

        return RESP_OK


@command(b"flush", {b"fast", b"connection"}, parent_command=b"function")
class FunctionFlush(Command):
    def execute(self) -> ValueType:
        return True


@command(b"load", {b"fast", b"connection"}, parent_command=b"function")
class FunctionLoad(Command):
    scripting_engine: ScriptingEngine = dependency()

    replace: bool = keyword_parameter(flag=b"REPLACE", default=False)
    function_code: bytes = positional_parameter()

    def execute(self) -> ValueType:
        name = self.scripting_engine.load_function(self.function_code)
        return name


@command(b"kill", {b"connection", b"fast"}, parent_command=b"script")
class ScriptKill(Command):
    def execute(self) -> ValueType:
        return RESP_OK


@command(b"flush", {b"fast", b"connection"}, parent_command=b"script")
class ScriptFlush(Command):
    def execute(self) -> ValueType:
        return True
