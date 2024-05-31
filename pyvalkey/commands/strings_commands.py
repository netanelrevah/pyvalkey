import fnmatch

from pyvalkey.commands.core import Command, DatabaseCommand
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.errors import ServerWrongTypeError, ValkeySyntaxError
from pyvalkey.resp import RESP_OK, ValueType


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
            return s.value
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
                result.append(s.value)
        return result


@ServerCommandsRouter.command(b"getdel", [b"read", b"string", b"fast"])
class GetDelete(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        s = self.database.pop_string(self.key)
        if s is not None:
            return s.value
        return None


@ServerCommandsRouter.command(b"exists", [b"read", b"string", b"fast"])
class Exists(DatabaseCommand):
    keys: list[bytes] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return sum(1 for key in self.keys if self.database.get_string_or_none(key) is not None)


@ServerCommandsRouter.command(b"strlen", [b"read", b"string", b"fast"])
class StringLength(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return len(self.database.get_string(self.key))


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

        return s.value


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

        s.value = self.value
        return RESP_OK


@ServerCommandsRouter.command(b"mset", [b"write", b"string", b"slow"])
class SetMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, value in self.key_value:  # Todo: should be atomic (use update in database)
            s = self.database.get_or_create_string(key)
            s.value = value
        return RESP_OK


@ServerCommandsRouter.command(b"msetnx", [b"write", b"string", b"slow"])
class SetIfNotExistsMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        string_value_to_update = []
        for key, value in self.key_value:
            s = self.database.get_string_or_none(key)
            if s is not None:
                return False
            string_value_to_update.append((s, value))
        if None in string_value_to_update:
            return False
        for key, value in self.key_value:
            s = self.database.get_or_create_string(key)
            s.value = value
        return True


@ServerCommandsRouter.command(b"getset", [b"write", b"string", b"slow"])
class GetSet(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)
        old_value = s.value
        s.value = self.value
        return old_value


@ServerCommandsRouter.command(b"setex", [b"write", b"string", b"slow"])
class SetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)
        self.database.set_expiration(self.key, self.seconds)
        s.value = self.value
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
        s.value = self.value
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
        s.value = s.value + self.value
        return len(s)
