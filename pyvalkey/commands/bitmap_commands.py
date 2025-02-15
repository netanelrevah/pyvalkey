import operator
from enum import Enum
from functools import reduce
from typing import Any, ClassVar

from pyvalkey.commands.core import DatabaseCommand
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import StringType
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import ValueType


class BitOperationMode(Enum):
    AND = b"AND"
    OR = b"OR"
    XOR = b"XOR"
    NOT = b"NOT"


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


@ServerCommandsRouter.command(b"bitfield", [b"read", b"bitmap", b"slow"])
class BitField(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"bitfield_ro", [b"read", b"bitmap", b"slow"])
class BitFieldReadOnly(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


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
                (self.database.get_string(source_key).bytes_value for source_key in self.source_keys),
            )
            s = self.database.get_or_create_string(self.destination_key)
            s.bytes_value = result
            return len(s)

        (source_key,) = self.source_keys

        source_s = self.database.get_string(source_key)
        destination_s = self.database.get_or_create_string(self.destination_key)
        destination_s.bytes_value = ~source_s.bytes_value
        return len(destination_s)


@ServerCommandsRouter.command(b"bitpos", [b"read", b"bitmap", b"slow"])
class BitPosition(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


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
