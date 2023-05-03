from enum import ReprEnum
from typing import Iterable, Sequence


def to_bytes(value: bytes | int | str) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def flatten(value: Iterable[Sequence], reverse_sub_lists=False):
    for item in value:
        for sub_item in item if not reverse_sub_lists else reversed(item):
            yield sub_item


class BytesEnum(bytes, ReprEnum):
    """
    Enum where members are also (and must be) strings
    """

    def __new__(cls, *values):
        "values must already be of type `str`"
        if len(values) > 1:
            raise TypeError("too many arguments for bytes(): %r" % (values,))
        if len(values) == 1:
            # it must be a bytes
            if not isinstance(values[0], bytes):
                raise TypeError("%r is not a bytes" % (values[0],))
        value = bytes(*values)
        member = bytes.__new__(cls, value)
        member._value_ = value
        return member

    def _generate_next_value_(name: bytes, start, count, last_values):
        """
        Return the lower-cased version of the member name.
        """
        return name.lower()
