from typing import Iterable, Sequence


def to_bytes(value: bytes | int | str) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def flatten(value: Iterable[Sequence], reverse_sub_lists: bool = False) -> Iterable[Sequence]:
    for item in value:
        yield from (item if not reverse_sub_lists else reversed(item))
