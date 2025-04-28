import time

from pyvalkey.commands.core import DatabaseCommand
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import increment_bytes_value_as_float, parse_range_parameters
from pyvalkey.database_objects.databases import Database, KeyValue
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.resp import RESP_OK, ValueType


def increment_by_int(database: Database, key: bytes, increment: int = 1) -> int:
    key_value: KeyValue[int] | None = database.int_database.get_or_none(key)
    if key_value is None:
        key_value = KeyValue(key, 0)
    previous_value = key_value.value
    key_value.value = previous_value + increment
    database.int_database.set_key_value(key_value)
    return previous_value


def increment_by_float(database: Database, key: bytes, increment: float = 1) -> bytes:
    key_value: KeyValue[bytes] | None = database.bytes_database.get_or_none(key)
    if key_value is None:
        key_value = KeyValue(key, b"0")
    previous_value = key_value.value
    key_value.value = increment_bytes_value_as_float(previous_value, increment)
    database.string_database.set_key_value(key_value)
    return previous_value


def increment_by(database: Database, key: bytes, increment: int | float = 1) -> bytes | int:
    if isinstance(increment, int):
        return increment_by_int(database, key, increment)
    elif isinstance(increment, float):
        return increment_by_float(database, key, increment)
    else:
        raise ValueError()


@command(b"append", {b"write", b"string", b"fast"})
class Append(DatabaseCommand):
    key: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.bytes_database.get_or_none(self.key)
        if key_value is None:
            key_value = KeyValue(self.key, b"")
        key_value.value += self.value
        self.database.set_key_value(key_value)
        return len(key_value.value) - len(self.value)


@command(b"decr", {b"write", b"string", b"fast"})
class Decrement(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, -1)


@command(b"decrby", {b"write", b"string", b"fast"})
class DecrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    decrement: int = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.decrement * -1)


@command(b"get", {b"read", b"string", b"fast"})
class Get(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return self.database.string_database.get_value_or_none(self.key)


@command(b"getdel", {b"read", b"string", b"fast"})
class GetDelete(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        key_value = self.database.string_database.pop(self.key, None)
        if key_value is not None:
            return key_value.value
        return None


@command(b"getex", {b"write", b"string", b"fast"})
class GetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)
    persist: bool = keyword_parameter(flag=b"PERSIST")

    def execute(self) -> ValueType:
        key_value = self.database.string_database.get_or_none(self.key)
        if key_value is None:
            return None

        fields_names = ["ex", "px", "exat", "pxat", "persist"]

        filled = list(map(bool, [getattr(self, name) for name in fields_names]))
        if filled.count(True) > 1:
            raise ServerError(b"ERR syntax error")

        if True in filled:
            name = fields_names[filled.index(True)]
            value = getattr(self, name)

            if name in ["ex", "px"]:
                self.database.set_expiration(self.key, value * (1000 if name == "ex" else 1))
            if name in ["exat", "pxat"]:
                self.database.set_expiration_at(self.key, value * (1000 if name == "exat" else 1))
            if name == "persist":
                self.database.set_persist(self.key)

        return key_value.value


@command(b"getrange", {b"stream", b"write", b"fast"})
class StringGetRange(DatabaseCommand):
    key: bytes = positional_parameter()
    start: int = positional_parameter()
    end: int = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.bytes_database.get_or_none(self.key)
        if key_value is None:
            return b""

        return key_value.value[parse_range_parameters(self.start, self.end)]


@command(b"getset", {b"write", b"string", b"slow"})
class GetSet(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        old_value = self.database.string_database.get_value_or_create(self.key)
        self.database.string_database.set_value(self.key, self.value)
        return old_value


@command(b"incr", {b"write", b"string", b"fast"})
class Increment(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key)


@command(b"incrby", {b"write", b"string", b"fast"})
class IncrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: int = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.increment)


@command(b"incrbyfloat", {b"write", b"string", b"fast"})
class IncrementByFloat(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: float = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.increment)


@command(b"mget", {b"read", b"string", b"fast"})
class MultipleGet(DatabaseCommand):
    keys: list[bytes] = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        result: list[bytes | None] = []
        for key in self.keys:
            key_value = None
            try:
                key_value = self.database.bytes_database.get_or_none(key)
            except ServerWrongTypeError:
                pass

            if key_value is None:
                result.append(None)
            else:
                result.append(key_value.value)
        return result


@command(b"mset", {b"write", b"string", b"slow"})
class SetMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, value in self.key_value:
            self.database.upsert(key, value)
        return RESP_OK


@command(b"msetnx", {b"write", b"string", b"slow"})
class SetIfNotExistsMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, _ in self.key_value:
            if self.database.string_database.has_key(key):
                return False
        for key, value in self.key_value:
            self.database.upsert(key, value)
        return True


@command(b"set", {b"write", b"string", b"slow"})
class Set(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    ex: int | None = keyword_parameter(flag=b"EX", default=None)
    px: int | None = keyword_parameter(flag=b"PX", default=None)
    exat: int | None = keyword_parameter(flag=b"EXAT", default=None)
    pxat: int | None = keyword_parameter(flag=b"PXAT", default=None)

    def get_one_and_only_token(self) -> str | None:
        fields_names = ["ex", "px", "exat", "pxat"]

        filled = list(map(bool, [getattr(self, name) for name in fields_names]))
        if filled.count(True) > 1:
            raise ServerError(b"ERR syntax error")

        if True in filled:
            name = fields_names[filled.index(True)]

            return name

        return None

    def execute(self) -> ValueType:
        expiration = None
        if token_name := self.get_one_and_only_token():
            token_value = getattr(self, token_name)
            if token_name in ["ex", "px"]:
                expiration = int(time.time() * 1000) + token_value * (1000 if token_name == "ex" else 1)
            if token_name in ["exat", "pxat"]:
                expiration = token_value * (1000 if token_name == "exat" else 1)

        self.database.pop(self.key, None)
        self.database.set_key_value(KeyValue.of_string(self.key, self.value, expiration))
        return RESP_OK


@command(b"setex", {b"write", b"string", b"slow"})
class SetExpire(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.string_database.upsert(self.key, self.value)
        self.database.set_expiration(self.key, self.seconds)
        return RESP_OK


@command(b"setnx", {b"write", b"string", b"fast"})
class SetIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.database.string_database.has_key(self.key):
            return False
        self.database.string_database.upsert(self.key, self.value)
        return True


@command(b"setrange", {b"write", b"string", b"slow"})
class SetRange(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    offset: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.offset < 0:
            raise ServerError(b"ERR value is not an integer or out of range")

        if self.offset + len(self.value) > 512 * (1024**2):
            raise ServerError(b"ERR string exceeds maximum allowed size (proto-max-bulk-len)")

        string_value = self.database.bytes_database.get_value_or_empty(self.key)
        if not self.value:
            return len(string_value)

        if self.offset >= len(string_value):
            new_value = string_value + b"\x00" * (self.offset - len(string_value)) + self.value
        else:
            new_value = string_value[: self.offset] + self.value + string_value[self.offset + len(self.value) :]

        self.database.bytes_database.upsert(self.key, new_value)

        return len(new_value)


@command(b"strlen", {b"read", b"string", b"fast"})
class StringLength(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return len(self.database.bytes_database.get_value_or_empty(self.key))
