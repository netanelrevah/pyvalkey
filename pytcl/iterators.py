from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Self, TextIO


@dataclass
class CharsIterator:
    _iterator: Iterator[str]

    _leftovers: list[str] = field(default_factory=list, init=False)
    _accumulator_stack: list[list[str]] = field(default_factory=list, init=False)

    def push(self) -> None:
        self._accumulator_stack.append([])

    def pop(self) -> str:
        return "".join(self._accumulator_stack[-1])

    def __next__(self) -> str:
        if self._leftovers:
            return self._leftovers.pop(0)
        char = next(self._iterator)
        self._accumulator_stack[-1].append(char)
        return char

    def __iter__(self) -> Self:
        return self

    def push_back(self) -> None:
        self._leftovers.append(self._accumulator_stack[-1][-1])

    def drop_last(self) -> None:
        self._accumulator_stack[-1].pop(-1)

    @classmethod
    def of(cls, chars: Iterator[str] | str) -> Self:
        if isinstance(chars, str):
            chars = iter(chars)
        return cls(chars)


def read_text_io_by_characters(text_io: TextIO) -> Iterator[str]:
    while c := text_io.read(1):
        yield c
