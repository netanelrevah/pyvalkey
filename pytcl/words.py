from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, ClassVar, Literal, Self, TextIO, TypeVar, dataclass_transform, overload

from pytcl.errors import TCLSubstituteError
from pytcl.iterators import CharsIterator, read_text_io_by_characters


class TCLWordBase:
    origin: str

    def substitute(self, namespace: dict[str, Any]) -> str:
        return "".join(self.substitute_iterator(namespace))

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        raise NotImplementedError()

    @classmethod
    def read(cls, chars: Iterator[str] | str) -> Self:
        iterator = chars
        if not isinstance(iterator, CharsIterator):
            iterator = CharsIterator.of(chars)
        iterator.push()
        tcl_word_base = cls._read(iterator)
        tcl_word_base.origin = iterator.pop()
        return tcl_word_base

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.origin


T = TypeVar("T", bound=TCLWordBase)


@dataclass_transform()
def tcl_word_wrapper(cls: type[T]) -> type[T]:
    assert issubclass(cls, TCLWordBase)

    cls.__annotations__["origin"] = str
    setattr(cls, "origin", field(default="", init=False))

    return dataclass(cls)


@overload
@dataclass_transform()
def tcl_word(command_cls: type[T], /) -> type[T]: ...


@overload
@dataclass_transform()
def tcl_word(*args: type[T]) -> type[T]: ...


def tcl_word(*args: Any) -> Any:
    if len(args) == 0:
        return tcl_word_wrapper
    return tcl_word_wrapper(args[0])


@tcl_word
class VariableSubstitution(TCLWordBase):
    variable_name: str

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        if self.variable_name not in namespace:
            raise TCLSubstituteError(f'can\'t read "{self.variable_name}": no such variable')
        return iter(str(namespace[self.variable_name]))

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        variable_name = ""
        while char := next(chars, None):
            match char:
                case " " | "\n":
                    chars.push_back()
                    chars.drop_last()
                    break
                case _:
                    variable_name += char
        return cls(variable_name)


@tcl_word
class TCLWord(TCLWordBase):
    value: str

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        return iter(self.value)

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        name = ""
        while char := next(chars, None):
            match char:
                case " " | "\t" | "\n":
                    chars.push_back()
                    chars.drop_last()
                    break
                case _:
                    name += char
        return cls(name)


@tcl_word
class TCLBracketWord(TCLWordBase):
    script: TCLScript

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        return self.script.substitute_iterator(namespace)

    @classmethod
    def _iterate_in_bracket(cls, chars: CharsIterator) -> Iterator[str]:
        chars_collected = False
        depth = 0
        while char := next(chars, None):
            match char:
                case "[":
                    depth += 1
                    chars_collected = True
                    yield char
                case "]":
                    if depth == 0:
                        chars.drop_last()
                        break
                    depth -= 1
                    chars_collected = True
                    yield char
                case "\n":
                    if chars_collected:
                        yield char
                case _:
                    yield char

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        return cls(TCLScript.read(cls._iterate_in_bracket(chars)))


@tcl_word
class TCLDoubleQuotedWord(TCLWordBase):
    value: str

    ESCAPING_CHARS: ClassVar[dict] = {"a": "\a", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}
    HEX_DIGITS: ClassVar[str] = "0123456789abcdef"
    MAX_NUM_OF_DIGITS_BY_ESCAPE_CHAR: ClassVar[dict] = {"x": 2, "u": 4, "U": 8}

    @classmethod
    def _read_unicode_value(
        cls, first_digit: str, chars: CharsIterator, max_num_of_digits: int, base: Literal[8, 16]
    ) -> str:
        number_of_chars = 1
        digits = first_digit
        while number_of_chars < max_num_of_digits:
            char = next(chars, None)
            if char is None:
                break
            if char.lower() not in cls.HEX_DIGITS[:base]:
                chars.push_back()
                break
            digits += char

        return chr(int(digits, base))

    @classmethod
    def _substitute_backslash(cls, chars: CharsIterator) -> str:
        while char := next(chars, None):
            if char in "abfnrtv":
                return cls.ESCAPING_CHARS[char]
            if char.isdigit():
                return cls._read_unicode_value(char, chars, 3, 8)
            if char in "xuU":
                first_digit = next(chars, None)
                if first_digit is None:
                    return char
                if first_digit.lower() not in cls.HEX_DIGITS:
                    chars.push_back()
                    return char
                return cls._read_unicode_value(first_digit, chars, cls.MAX_NUM_OF_DIGITS_BY_ESCAPE_CHAR[char], 16)
            return char
        raise TCLSubstituteError()

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        chars = CharsIterator.of(self.value)
        chars.push()
        while char := next(chars, None):
            match char:
                case "\\":
                    yield from self._substitute_backslash(chars)
                case "$":
                    next_char = next(chars, None)
                    if not next_char:
                        yield "$"
                        break
                    if next_char in [" ", "\n", "$"]:
                        yield from ["$", next_char]
                        continue
                    chars.push_back()
                    yield from VariableSubstitution.read(chars).substitute_iterator(namespace)
                case _:
                    yield char

        # parts = self.value.split(" ")
        # for part in parts[:-1]:
        #     yield from self._substitute_sub_word(part, namespace)
        #     yield from " "
        # yield from self._substitute_sub_word(parts[-1], namespace)

    @classmethod
    def read_backslash(cls, chars: Iterator[str]) -> str:
        char = next(chars, None)
        match char:
            case None:
                raise StopIteration()
            case "\n":
                return " "
            case _:
                return "\\" + char

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        value: list[str] = []
        while char := next(chars, None):
            match char:
                case "\\":
                    value.append(cls.read_backslash(chars))
                case '"':
                    chars.drop_last()
                    break
                case _:
                    value.append(char)

        return cls("".join(value))


@tcl_word
class TCLBracesWord(TCLWordBase):
    value: str

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        yield from self.value

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        collected_chars: list[str] = []

        depth = 0
        while char := next(chars, None):
            match char:
                case "{":
                    depth += 1
                    collected_chars.append(char)
                case "}":
                    if depth == 0:
                        chars.drop_last()
                        break
                    depth -= 1
                    collected_chars.append(char)
                case "\n":
                    if collected_chars:
                        collected_chars.append(char)
                case _:
                    collected_chars.append(char)

        return cls("".join(collected_chars))


TCLCommandArguments = TCLBracesWord | TCLDoubleQuotedWord | TCLBracketWord | VariableSubstitution | TCLWord


@tcl_word
class TCLCommandWord(TCLWordBase):
    name: str
    args: list[TCLCommandArguments]

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        if self.name not in namespace:
            raise TCLSubstituteError(f'invalid command name "{self.name}"')
        yield from namespace[self.name].substitute_iterator(namespace)

    @classmethod
    def read_name(cls, chars: Iterator[str]) -> str:
        name = ""
        while char := next(chars, None):
            match char:
                case " " | "\t":
                    if name == "":
                        continue
                    break
                case "\n":
                    raise ValueError()
                case _:
                    name += char
        return name

    @classmethod
    def _read(cls, chars: CharsIterator) -> Self:
        name = cls.read_name(chars)

        arguments: list[TCLCommandArguments] = []
        while char := next(chars, None):
            match char:
                case "\n" | ";":
                    chars.drop_last()
                    break
                case '"':
                    arguments.append(TCLDoubleQuotedWord.read(chars))
                case "{":
                    arguments.append(TCLBracesWord.read(chars))
                case "[":
                    arguments.append(TCLBracketWord.read(chars))
                case "}" | "]":
                    raise ValueError()
                case " " | "\t":
                    continue
                case _:
                    arguments.append(TCLWord.read(chain(char, chars)))
        return cls(name, arguments)


EMPTY_COMMAND = TCLCommandWord(name="", args=[])


@dataclass
class TCLScript(TCLWordBase):
    commands: list[TCLCommandWord]

    def substitute_iterator(self, namespace: dict[str, Any]) -> Iterator[str]:
        for command in self.commands:
            command.substitute_iterator(namespace)
        yield ""

    @classmethod
    def handle_comment(cls, chars: Iterator[str]) -> None:
        while char := next(chars, None):
            if char == "\n":
                break

    @classmethod
    def _read(cls, chars: CharsIterator, in_bracket: bool = False) -> Self:
        commands: list[TCLCommandWord] = []
        while char := next(chars, None):
            match char:
                case "\n" | ";" | " ":
                    continue
                case "#":
                    cls.handle_comment(chars)
                case _:
                    commands.append(TCLCommandWord.read(chain([char], chars)))

        return cls(commands)

    @classmethod
    def read_text_io(cls, text_io: TextIO) -> Self:
        return cls.read(read_text_io_by_characters(text_io))


EMTPY_SCRIPT = TCLScript([EMPTY_COMMAND])
