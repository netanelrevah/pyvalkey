import operator
from enum import Enum
from functools import reduce
from typing import Any, ClassVar

from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.commands.strings_commands import DatabaseCommand
from pyvalkey.database_objects.databases import Database, StringType
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"getbit", [b"read", b"bitmap", b"fast"])
class GetBit(DatabaseCommand):
    key: bytes = positional_parameter()
    offset: int = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)

        return s.get_set(self.offset)


@ServerCommandsRouter.command(b"setbit", [b"write", b"bitmap", b"slow"])
class SetBit(DatabaseCommand):
    key: bytes = positional_parameter()
    offset: int = positional_parameter()
    value: bool = positional_parameter()

    def execute(self) -> ValueType:
        s = self.database.get_or_create_string(self.key)

        previous_value = s.get_set(self.offset)

        s.set_bit(self.offset, self.value)

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
