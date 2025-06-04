import decimal
import re
from math import inf

from pyvalkey.database_objects.errors import ServerError

NUMERIC_REGEX = re.compile(b"^-?\d+(\.\d*)?$")
INTEGER_REGEX = re.compile(b"^-?\d+$")
FLOATING_POINT_REGEX = NUMERIC_REGEX


def is_numeric(value: bytes) -> bool:
    return NUMERIC_REGEX.match(value) is not None


def is_integer(value: bytes) -> bool:
    return INTEGER_REGEX.match(value) is not None


def is_floating_point(value: bytes) -> bool:
    if value in [b"+inf", b"-inf", b"inf"]:
        return True
    return FLOATING_POINT_REGEX.match(value) is not None


def parse_range_parameters(start: int, stop: int, is_reversed: bool = False) -> slice:
    if not is_reversed:
        python_start = start
        if stop == -1:
            python_stop = None
        else:
            python_stop = stop + 1
        return slice(python_start, python_stop)

    python_reversed_start = -(start + 1)
    if stop == -1:
        python_reversed_stop = None
    else:
        python_reversed_stop = -(stop + 2)
    return slice(python_reversed_start, python_reversed_stop, -1)


def convert_bytes_value_to_float(value: bytes) -> float:
    if not is_floating_point(value):
        raise ServerError(b"ERR value is not a valid float")
    return float(value)


decimal_context = decimal.Context()
decimal_context.prec = 20


def float_to_str(value: float) -> str:
    """
    Convert the given float to a string,
    without resorting to scientific notation
    """
    d1 = decimal_context.create_decimal(repr(value))
    return format(d1, "f")


def convert_float_value_to_bytes(value: float) -> bytes:
    return float_to_str(value).rstrip("0").rstrip(".").encode()


def convert_bytes_value_as_int(value: bytes) -> int:
    if not is_integer(value):
        raise ServerError(b"ERR value is not an integer or out of range")
    return int(value)


def convert_bytes_value_to_int(value: bytes) -> int:
    return int.from_bytes(value, byteorder="big", signed=True)


def convert_int_value_to_bytes(value: int) -> bytes:
    return value.to_bytes(length=(8 + (value + (value < 0)).bit_length()) // 8, byteorder="big", signed=True)


def increment_bytes_value_as_float(value: bytes, increment: float) -> bytes:
    if abs(increment) == inf:
        raise ServerError(b"ERR value is NaN or Infinity")
    float_value = convert_bytes_value_to_float(value)
    if abs(float_value + increment) == inf:
        raise ServerError(b"ERR increment would produce NaN or Infinity")
    return convert_float_value_to_bytes(convert_bytes_value_to_float(value) + increment)


def count_bits_by_bytes_range(value: bytes, start: int | None = None, stop: int | None = None) -> int:
    return sum(map(lambda x: x.bit_count(), value[slice(start, stop)]))


def count_bits_by_bits_range(value: bytes, start: int, stop: int) -> int:
    int_value = convert_bytes_value_to_int(value)
    return ((int_value & ((2**stop) - 1)) >> start).bit_count()


def get_bit_from_bytes(value: bytes, offset: int) -> int:
    if offset >= 2**32 or offset < 0:
        raise ServerError(b"ERR bit offset is not an integer or out of range")

    bytes_offset = offset // 8
    byte_offset = offset - (bytes_offset * 8)

    adjusted_value = value
    if len(value) <= bytes_offset:
        adjusted_value = value.ljust(bytes_offset + 1, b"\0")

    return (adjusted_value[bytes_offset] >> (7 - byte_offset)) & 1


def set_bit_to_bytes(value: bytes, offset: int, bit_value: bool) -> bytes:
    if offset >= 2**32 or offset < 0:
        raise ServerError(b"ERR bit offset is not an integer or out of range")

    bytes_offset = offset // 8
    byte_offset = offset - (bytes_offset * 8)

    new_value = value

    if len(value) <= bytes_offset:
        new_value = value.ljust(bytes_offset + 1, b"\0")

    if bit_value:
        new_byte = new_value[bytes_offset] | (128 >> byte_offset)
    else:
        new_byte = new_value[bytes_offset] & ~(128 >> byte_offset)

    return new_value[:bytes_offset] + bytes([new_byte]) + new_value[bytes_offset + 1 :]
