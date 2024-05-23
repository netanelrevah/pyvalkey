from collections.abc import Iterable, Sequence
from typing import TypeVar


def to_bytes(value: bytes | int | str) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


T = TypeVar("T")


def flatten(value: Iterable[Sequence[T]], reverse_sub_lists: bool = False) -> Iterable[T]:
    for item in value:
        yield from (item if not reverse_sub_lists else reversed(item))
