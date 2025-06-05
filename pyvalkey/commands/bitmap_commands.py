import operator
from enum import Enum
from functools import reduce
from typing import Any, ClassVar

from pyvalkey.commands.core import DatabaseCommand
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import (
    convert_bytes_value_to_int,
    convert_int_value_to_bytes,
    count_bits_by_bits_range,
    count_bits_by_bytes_range,
    get_bit_from_bytes,
    set_bit_to_bytes,
)
from pyvalkey.database_objects.databases import KeyValue
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType


class BitOperationMode(Enum):
    AND = b"AND"
    OR = b"OR"
    XOR = b"XOR"
    NOT = b"NOT"


@command(b"bitcount", {b"read", b"bitmap", b"slow"})
class BitCount(DatabaseCommand):
    key: bytes = positional_parameter()
    count_range: tuple[int, int] | None = positional_parameter(default=None)
    bit_mode: bool = positional_parameter(default=False, values_mapping={b"BYTE": False, b"BIT": True})

    @classmethod
    def handle_byte_mode(cls, value: bytes, start: int, end: int) -> int:
        length = len(value)
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

        return count_bits_by_bytes_range(value, start, stop + 1)

    @classmethod
    def handle_bit_mode(cls, value: bytes, start: int, end: int) -> int:
        length = convert_bytes_value_to_int(value).bit_length()

        if start < 0:
            start = length + start

        if end < 0:
            end = length + (end + 1)

        return count_bits_by_bits_range(value, start, end)

    def execute(self) -> ValueType:
        string_value = self.database.bytes_database.get_value(self.key)
        if not self.count_range:
            return count_bits_by_bytes_range(string_value)

        start, end = self.count_range

        if self.bit_mode:
            return self.handle_bit_mode(string_value, start, end)
        return self.handle_byte_mode(string_value, start, end)


@command(b"bitfield", {b"read", b"bitmap", b"slow"})
class BitField(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


@command(b"bitfield_ro", {b"read", b"bitmap", b"slow"})
class BitFieldReadOnly(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


@command(b"bitop", {b"write", b"bitmap", b"slow"})
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
                (
                    convert_bytes_value_to_int(self.database.bytes_database.get_value(source_key))
                    for source_key in self.source_keys
                ),
            )
            string_value = convert_int_value_to_bytes(result)
            self.database.upsert(self.destination_key, string_value)
            return len(string_value)

        (source_key,) = self.source_keys

        new_value = convert_int_value_to_bytes(
            ~convert_bytes_value_to_int(self.database.bytes_database.get_value(source_key))
        )
        self.database.upsert(self.destination_key, new_value)
        return len(new_value)


@command(b"bitpos", {b"read", b"bitmap", b"slow"})
class BitPosition(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


@command(b"getbit", {b"read", b"bitmap", b"fast"})
class GetBit(DatabaseCommand):
    key: bytes = positional_parameter()
    offset: int = positional_parameter()

    def execute(self) -> ValueType:
        return get_bit_from_bytes(self.database.bytes_database.get_value_or_empty(self.key), self.offset)


@command(b"setbit", {b"write", b"bitmap", b"slow"})
class SetBit(DatabaseCommand):
    key: bytes = positional_parameter()
    offset: int = positional_parameter()
    value: int = positional_parameter()

    def execute(self) -> ValueType:
        if not (0 <= self.value <= 1):
            raise ServerError(b"ERR bit is not an integer or out of range")

        bit_bool_value = bool(self.value)

        value = self.database.bytes_database.get_value_or_empty(self.key)

        self.database.string_database.set_key_value(
            KeyValue.of_string(self.key, set_bit_to_bytes(value, self.offset, bit_bool_value)),
        )

        return get_bit_from_bytes(value, self.offset)
