import fnmatch
import operator
import random
from enum import Enum
from functools import reduce
from typing import Any, ClassVar

from pyvalkey.commands.core import Command, DatabaseCommand
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database, StringType
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError, ValkeySyntaxError
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


@ServerCommandsRouter.command(b"setrange", [b"write", b"string", b"slow"])
class SetRange(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    offset: int = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.offset < 0:
            raise ServerError(b"ERR value is not an integer or out of range")

        if self.offset + len(self.value) > 512 * (1024**2):
            raise ServerError(b"ERR string exceeds maximum allowed size (proto-max-bulk-len)")

        if not self.value:
            s = self.database.get_string(self.key)
            return len(s)

        s = self.database.get_or_create_string(self.key)

        if self.offset >= len(s):
            new_value = s.value + b"\x00" * (self.offset - len(s)) + self.value
        else:
            new_value = s.value[: self.offset] + self.value + s.value[self.offset + len(self.value) :]

        s.value = new_value

        return len(s)


@ServerCommandsRouter.command(b"mset", [b"write", b"string", b"slow"])
class SetMultiple(DatabaseCommand):
    key_value: list[tuple[bytes, bytes]] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        for key, value in self.key_value:  # Todo: should be atomic (use update in database)
            s = self.database.get_or_create_string(key)
            s.value = value
        return RESP_OK


@ServerCommandsRouter.command(b"srandmember", [b"write", b"string", b"slow"])
class SetRandomMember(DatabaseCommand):
    key: bytes = positional_parameter()
    count: int = positional_parameter(default=1)

    def execute(self) -> ValueType:
        s = list(self.database.get_or_create_set(self.key))

        if self.count >= 0:
            result = []
            for _ in range(self.count):
                try:
                    result.append(s.pop(random.randrange(len(s))))
                except IndexError:
                    break
            return result

        return [random.choice(list(s)) for _ in range(abs(self.count))]


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


@ServerCommandsRouter.command(b"getbit", [b"read", b"bitmap", b"fast"])
class GetBit(DatabaseCommand):
    key: bytes = positional_parameter()
    offset: int = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)

        return s.get_bit(self.offset)


@ServerCommandsRouter.command(b"setbit", [b"write", b"bitmap", b"slow"])
class SetBit(DatabaseCommand):
    key: bytes = positional_parameter()
    offset: int = positional_parameter()
    value: int = positional_parameter()

    def execute(self) -> ValueType:
        if not (0 <= self.value <= 1):
            raise ServerError(b"ERR bit is not an integer or out of range")

        value = bool(self.value)

        s = self.database.get_or_create_string(self.key)

        previous_value = s.get_bit(self.offset)

        s.set_bit(self.offset, value)

        return previous_value


class BitOperationMode(Enum):
    AND = b"AND"
    OR = b"OR"
    XOR = b"XOR"
    NOT = b"NOT"


@ServerCommandsRouter.command(b"bitop", [b"write", b"bitmap", b"slow"])
class BitOperation(DatabaseCommand):
    OPERATION_TO_OPERATOR: ClassVar[dict[BitOperationMode, Any]] = {
        BitOperationMode.AND: operator.and_,
        BitOperationMode.OR: operator.or_,
        BitOperationMode.XOR: operator.xor,
    }

    operation: BitOperationMode = positional_parameter()
    destination_key: bytes = positional_parameter()
    source_keys: list[bytes] = positional_parameter()

    def handle(self) -> int:
        if self.operation in self.OPERATION_TO_OPERATOR:
            result = reduce(
                self.OPERATION_TO_OPERATOR[self.operation],
                (self.database.get_string(source_key).int_value for source_key in self.source_keys),
            )
            s = self.database.get_or_create_string(self.destination_key)
            s.int_value = result
            return len(s)

        (source_key,) = self.source_keys

        source_s = self.database.get_string(source_key)
        destination_s = self.database.get_or_create_string(self.destination_key)
        destination_s.int_value = ~source_s.int_value
        return len(destination_s)


@ServerCommandsRouter.command(b"bitcount", [b"read", b"bitmap", b"slow"])
class BitCount(DatabaseCommand):
    key: bytes = positional_parameter()
    count_range: tuple[int, int] | None = positional_parameter(default=None)
    bit_mode: bool = positional_parameter(default=False, values_mapping={b"BYTE": False, b"BIT": True})

    @classmethod
    def handle_byte_mode(cls, s: StringType, start: int, end: int) -> int:
        length = len(s.value)
        server_start = start
        server_stop = end

        if server_start >= 0:
            start = min(length, server_start)
        else:
            start = max(length + int(server_start), 0)

        if server_stop >= 0:
            stop = min(length, server_stop)
        else:
            stop = max(length + int(server_stop), 0)

        return s.count_bits_of_int(start, stop + 1)

    @classmethod
    def handle_bit_mode(cls, s: StringType, start: int, end: int) -> int:
        length = s.bit_length()

        if start < 0:
            start = length + start

        if end < 0:
            end = length + (end + 1)

        return s.count_bits_of_int(start, end)

    def execute(self) -> ValueType:
        s = self.database.get_string(self.key)
        if not self.count_range:
            return s.count_bits_of_bytes()

        start, end = self.count_range

        if self.bit_mode:
            return self.handle_bit_mode(s, start, end)
        return self.handle_byte_mode(s, start, end)


def increment_by(database: Database, key: bytes, increment: int | float = 1) -> bytes:
    s = database.get_or_create_string(key)
    s.numeric_value = s.numeric_value + increment
    return s.value


@ServerCommandsRouter.command(b"incr", [b"write", b"string", b"fast"])
class Increment(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key)


@ServerCommandsRouter.command(b"incrby", [b"write", b"string", b"fast"])
class IncrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: int = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.increment)


@ServerCommandsRouter.command(b"incrbyfloat", [b"write", b"string", b"fast"])
class IncrementByFloat(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: float = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.increment)


@ServerCommandsRouter.command(b"decr", [b"write", b"string", b"fast"])
class Decrement(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, -1)


@ServerCommandsRouter.command(b"decrby", [b"write", b"string", b"fast"])
class DecrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    decrement: float = positional_parameter()

    def execute(self) -> ValueType:
        return increment_by(self.database, self.key, self.decrement * -1)
