import operator
from dataclasses import dataclass
from functools import reduce
from typing import Any, ClassVar

from r3dis.commands.database.core import DatabaseCommand
from r3dis.commands.parsers import redis_positional_parameter
from r3dis.databases import Database
from r3dis.errors import RedisException


@dataclass
class GetBit(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    offset: int = redis_positional_parameter()

    def execute(self):
        s = self.database.get_or_create_string(self.key)

        bytes_offset = self.offset // 8
        byte_offset = self.offset - (bytes_offset * 8)

        return (s.bytes_value[bytes_offset] >> byte_offset) & 1


@dataclass
class SetBit(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    offset: int = redis_positional_parameter()
    value: bool = redis_positional_parameter()

    def execute(self):
        s = self.database.get_or_create_string(self.key)

        offset = int(self.offset)
        bytes_offset = offset // 8
        byte_offset = offset - (bytes_offset * 8)

        if len(s.bytes_value) <= bytes_offset:
            s.bytes_value = s.bytes_value.ljust(bytes_offset + 1, b"\0")
        previous_value = (s.bytes_value[bytes_offset] >> byte_offset) & 1

        if self.value:
            new_byte = s.bytes_value[bytes_offset] | 1 << byte_offset
        else:
            new_byte = s.bytes_value[bytes_offset] & ~(1 << byte_offset)

        s.update_with_bytes_value(s.bytes_value[:bytes_offset] + bytes([new_byte]) + s.bytes_value[bytes_offset + 1 :])

        return previous_value


@dataclass
class BitOperation(DatabaseCommand):
    OPERATION_TO_OPERATOR: ClassVar[dict[bytes, Any]] = {
        b"AND": operator.and_,
        b"OR": operator.or_,
        b"XOR": operator.xor,
    }

    operation: bytes = redis_positional_parameter(bytes_options=[b"AND", b"OR", b"XOR", b"NOT"])
    destination_key: bytes = redis_positional_parameter()
    source_keys: list[bytes] = redis_positional_parameter()

    def handle(self):
        if self.operation in self.OPERATION_TO_OPERATOR:
            result = reduce(
                self.OPERATION_TO_OPERATOR[self.operation],
                (self.database.get_string(source_key).int_value for source_key in self.source_keys),
            )
            s = self.database.get_or_create_string(self.destination_key)
            s.update_with_int_value(result)
            return len(s)

        (source_key,) = self.source_keys

        source_s = self.database.get_string(source_key)
        destination_s = self.database.get_or_create_string(self.destination_key)
        destination_s.update_with_int_value(~source_s.int_value)
        return len(destination_s)


@dataclass
class BitCount(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    count_range: tuple[int, int] | None = redis_positional_parameter(default=None)
    bit_mode: bool = redis_positional_parameter(default=False, bool_map={b"BYTE": False, b"BIT": True})

    def handle_byte_mode(self, key: bytes, start: int, end: int):
        s = self.database.get_string(key)

        length = len(s.bytes_value)
        redis_start = start
        redis_stop = end

        if redis_start >= 0:
            start = min(length, redis_start)
        else:
            start = max(length + int(redis_start), 0)

        if redis_stop >= 0:
            stop = min(length, redis_stop)
        else:
            stop = max(length + int(redis_stop), 0)

        return sum(map(int.bit_count, s.bytes_value[start : stop + 1]))

    def handle_bit_mode(self, key: bytes, start: int, end: int):
        s = self.database.get_string(key)
        value: int = s.int_value

        length = value.bit_length()

        if start < 0:
            start = length + start

        if end < 0:
            end = length + (end + 1)

        bit_count = ((value & ((2**end) - 1)) >> start).bit_count()
        return bit_count

    def execute(self):
        if not self.count_range:
            s = self.database.get_string(self.key)
            return sum(map(int.bit_count, s.bytes_value))

        start, end = self.count_range

        if self.bit_mode:
            self.handle_bit_mode(self.key, start, end)


def apply_increment(database: Database, key: bytes, increment: int | float = 1):
    s = database.get_or_create_string(key)
    if s.numeric_value is None:
        raise RedisException(b"ERR value is not an integer or out of range")
    s.update_with_numeric_value(s.numeric_value + increment)
    return s.bytes_value


@dataclass
class Increment(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return apply_increment(self.database, self.key)


@dataclass
class IncrementBy(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    increment: int = redis_positional_parameter()

    def execute(self):
        return apply_increment(self.database, self.key, self.increment)


@dataclass
class IncrementByFloat(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    increment: float = redis_positional_parameter()

    def execute(self):
        return apply_increment(self.database, self.key, self.increment)


@dataclass
class Decrement(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return apply_increment(self.database, self.key, -1)


@dataclass
class DecrementBy(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    decrement: float = redis_positional_parameter()

    def execute(self):
        return apply_increment(self.database, self.key, self.decrement * -1)
