import operator
from enum import Enum
from functools import reduce
from typing import Any, ClassVar

from pyvalkey.commands.core import Command
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
from pyvalkey.database_objects.databases import Database, KeyValue
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType
from pyvalkey.utils.dependencies import dependency


class BitOperationMode(Enum):
    AND = b"AND"
    OR = b"OR"
    XOR = b"XOR"
    NOT = b"NOT"


@command(b"bitcount", {b"bitmap", b"read", b"slow"}, flags={b"readonly"})
class BitCount(Command):
    """
    summary: Counts the number of set bits (population counting) in a string.
    complexity: O(N)
    since: 2.6.0
    function: bitcountCommand
    reply_schema:
      description: The number of bits set to 1.
      type: integer
      minimum: 0
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
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


@command(b"bitfield", {b"bitmap", b"slow"}, flags={b"denyoom", b"write"})
class BitField(Command):
    """
    summary: Performs arbitrary bitfield integer operations on strings.
    complexity: O(1) for each subcommand specified
    since: 3.2.0
    function: bitfieldCommand
    reply_schema:
      type: array
      items:
        oneOf:
        - description: The result of the subcommand at the same position
          type: integer
        - description: In case OVERFLOW FAIL was given and overflows or underflows detected
          type: 'null'
    """

    database: Database = dependency()

    def execute(self) -> ValueType:
        return None


@command(b"bitfield_ro", {b"bitmap", b"read"}, flags={b"fast", b"readonly"})
class BitFieldReadOnly(Command):
    """
    summary: Performs arbitrary read-only bitfield integer operations on strings.
    complexity: O(1) for each subcommand specified
    since: 6.0.0
    function: bitfieldroCommand
    reply_schema:
      type: array
      items:
        description: The result of the subcommand at the same position
        type: integer
    """

    database: Database = dependency()

    def execute(self) -> ValueType:
        return None


@command(b"bitop", {b"bitmap", b"slow"}, flags={b"denyoom", b"write"})
class BitOperation(Command):
    """
    summary: Performs bitwise operations on multiple strings, and stores the result.
    complexity: O(N)
    since: 2.6.0
    function: bitopCommand
    reply_schema:
      description: >-
        The size of the string stored in the destination key, that is equal to the size of the longest input string.
      type: integer
      minimum: 0
    """

    database: Database = dependency()

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


@command(b"bitpos", {b"bitmap", b"read", b"slow"}, flags={b"readonly"})
class BitPosition(Command):
    """
    summary: Finds the first set (1) or clear (0) bit in a string.
    complexity: O(N)
    since: 2.8.7
    function: bitposCommand
    reply_schema:
      oneOf:
      - description: The position of the first bit set to 1 or 0 according to the request.
        type: integer
        minimum: 0
      - description: >-
          In case the `bit` argument is 1 and the string is empty or composed of just zero bytes.
        const: -1
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    bit: int = positional_parameter()
    start: int | None = positional_parameter(default=None)
    end: int | None = positional_parameter(default=None)
    bit_mode: bool = positional_parameter(default=False, values_mapping={b"BYTE": False, b"BIT": True})

    def execute(self) -> ValueType:
        if self.bit not in (0, 1):
            raise ServerError(b"ERR The bit argument must be 1 or 0.")

        value = self.database.bytes_database.get_value_or_empty(self.key)
        end_given = self.end is not None
        length = len(value)

        if self.bit_mode:
            total = length * 8
            start = self.start if self.start is not None else 0
            end = self.end if self.end is not None else total - 1
            if start < 0:
                start = max(0, total + start)
            if end < 0:
                end = total + end
            end = min(end, total - 1)
            if total == 0 or start > end:
                return -1
            for pos in range(start, end + 1):
                if (value[pos // 8] >> (7 - (pos % 8))) & 1 == self.bit:
                    return pos
            return -1

        start = self.start if self.start is not None else 0
        end = self.end if self.end is not None else length - 1
        if start < 0:
            start = max(0, length + start)
        if end < 0:
            end = length + end
        end = min(end, length - 1)
        if length == 0 or start > end:
            return -1

        skip = 0x00 if self.bit else 0xFF
        for byte_idx in range(start, end + 1):
            byte = value[byte_idx]
            if byte == skip:
                continue
            for bit_idx in range(8):
                if (byte >> (7 - bit_idx)) & 1 == self.bit:
                    return byte_idx * 8 + bit_idx

        if self.bit == 0 and not end_given:
            return (end + 1) * 8
        return -1


@command(b"getbit", {b"bitmap", b"read"}, flags={b"fast", b"readonly"})
class GetBit(Command):
    """
    summary: Returns a bit value by offset.
    complexity: O(1)
    since: 2.2.0
    function: getbitCommand
    reply_schema:
      description: The bit value stored at offset.
      oneOf:
      - const: 0
      - const: 1
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    offset: int = positional_parameter()

    def execute(self) -> ValueType:
        return get_bit_from_bytes(self.database.bytes_database.get_value_or_empty(self.key), self.offset)


@command(b"setbit", {b"bitmap", b"slow"}, flags={b"denyoom", b"write"})
class SetBit(Command):
    """
    summary: >-
      Sets or clears the bit at offset of the string value. Creates the key if it doesn't exist.
    complexity: O(1)
    since: 2.2.0
    function: setbitCommand
    reply_schema:
      description: The original bit value stored at offset.
      oneOf:
      - const: 0
      - const: 1
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
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
