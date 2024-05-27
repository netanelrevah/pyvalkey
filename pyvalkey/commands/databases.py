import fnmatch
from dataclasses import dataclass

from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerWrongTypeError, ValkeySyntaxError
from pyvalkey.resp import RESP_OK, ValueType


@dataclass
class DatabaseCommand(Command):
    database: Database = server_command_dependency()

    def execute(self) -> ValueType:
        raise NotImplementedError()


@ServerCommandsRouter.command(b"echo", [b"fast", b"connection"])
class Echo(Command):
    message: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.message


@ServerCommandsRouter.command(b"ping", [b"fast", b"connection"])
class Ping(Command):
    message: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.message:
            return self.message
        return b"PONG"


@ServerCommandsRouter.command(b"get", [b"read", b"string", b"fast"])
class Get(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        s = self.database.get_string_or_none(self.key)
        if s is not None:
            return s.bytes_value
        return None


@ServerCommandsRouter.command(b"mget", [b"read", b"string", b"fast"])
class MultipleGet(DatabaseCommand):
    keys: list[bytes] = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        result: list[bytes | None] = []
        for key in self.keys:
            s = None
            try:
                s = self.database.get_string_or_none(key)
            except ServerWrongTypeError:
                pass

            if s is None:
                result.append(None)
            else:
                result.append(s.bytes_value)
        return result


@ServerCommandsRouter.command(b"getdel", [b"read", b"string", b"fast"])
class GetDelete(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        s = self.database.pop_string(self.key)
        if s is not None:
            return s.bytes_value
        return None


@ServerCommandsRouter.command(b"getex", [b"write", b"string", b"fast"])
class GetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)
    persist: bool = keyword_parameter(flag=b"PERSIST")

    def execute(self) -> ValueType:
        s = self.database.get_string_or_none(self.key)
        if s is None:
            return None

        fields_names = ["ex", "px", "exat", "pxat", "persist"]

        filled = list(map(bool, [getattr(self, name) for name in fields_names]))
        if filled.count(True) > 1:
            raise ValkeySyntaxError()

        if True in filled:
            name = fields_names[filled.index(True)]
            value = getattr(self, name)

            if name in ["ex", "px"]:
                self.database.set_expiration(self.key, value * (1000 if name == "ex" else 1))
            if name in ["exat", "pxat"]:
                self.database.set_expiration_at(self.key, value * (1000 if name == "exat" else 1))
            if name == "persist":
                self.database.set_persist(self.key)

        return s.bytes_value


@ServerCommandsRouter.command(b"ttl", [b"write", b"string", b"fast"])
class TimeToLive(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_expiration(self.key)
            if expiration is None:
                return -1
            return expiration // 1000
        except KeyError:
            return -2


@ServerCommandsRouter.command(b"pttl", [b"write", b"string", b"fast"])
class TimeToLiveMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_expiration(self.key)
            if expiration is None:
                return -1
            return expiration
        except KeyError:
            return -2


@ServerCommandsRouter.command(b"set", [b"write", b"string", b"slow"])
class Set(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)

        fields_names = ["ex", "px", "exat", "pxat"]

        filled = list(map(bool, [getattr(self, name) for name in fields_names]))
        if filled.count(True) > 1:
            raise ValkeySyntaxError()

        if True in filled:
            name = fields_names[filled.index(True)]
            value = getattr(self, name)

            if name in ["ex", "px"]:
                self.database.set_expiration(self.key, value * (1000 if name == "ex" else 1))
            if name in ["exat", "pxat"]:
                self.database.set_expiration_at(self.key, value * (1000 if name == "exat" else 1))

        s.update_with_bytes_value(self.value)
        return RESP_OK


@ServerCommandsRouter.command(b"setex", [b"write", b"string", b"slow"])
class SetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)
        self.database.set_expiration(self.key, self.seconds)
        s.update_with_bytes_value(self.value)
        return RESP_OK


@ServerCommandsRouter.command(b"setnx", [b"write", b"string", b"fast"])
class SetIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_string_or_none(self.key)
        if s is not None:
            return False
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(self.value)
        return True


@ServerCommandsRouter.command(b"del", [b"keyspace", b"write", b"slow"])
class Delete(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return len([1 for _ in filter(None, [self.database.data.pop(key, None) for key in self.keys])])


@ServerCommandsRouter.command(b"expire", [b"keyspace", b"write", b"fast"])
class Expire(DatabaseCommand):
    key: bytes = positional_parameter()
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration(self.key, self.seconds)


@ServerCommandsRouter.command(b"keys", [b"keyspace", b"read", b"slow", b"dangerous"])
class Keys(DatabaseCommand):
    pattern: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(fnmatch.filter(self.database.data.keys(), self.pattern))


@ServerCommandsRouter.command(b"dbsize", [b"keyspace", b"read", b"fast"])
class DatabaseSize(DatabaseCommand):
    def execute(self) -> ValueType:
        return len(self.database.data.keys())


@ServerCommandsRouter.command(b"append", [b"write", b"string", b"fast"])
class Append(DatabaseCommand):
    key: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)
        s.update_with_bytes_value(s.bytes_value + self.value)
        return len(s)
