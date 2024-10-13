from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Self, TextIO


@dataclass
class CharsIterator:
    _iterator: Iterator[str]

    _accumulator: list[str] = field(default_factory=list, init=False)

    @property
    def origin(self) -> str:
        return "".join(self._accumulator)

    def __next__(self) -> str:
        char = next(self._iterator)
        self._accumulator.append(char)
        return char

    def __iter__(self) -> Self:
        return self

    def drop_last(self) -> None:
        self._accumulator.pop(-1)

    @classmethod
    def of(cls, chars: Iterator[str] | str) -> Self:
        if isinstance(chars, str):
            chars = iter(chars)
        return cls(chars)


def read_text_io_by_characters(text_io: TextIO) -> Iterator[str]:
    while c := text_io.read(1):
        yield c
