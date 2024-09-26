from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Self, TextIO


def read_text_io_by_characters(text_io: TextIO) -> Iterator[str]:
    while c := text_io.read(1):
        yield c


class TCLTokenizer:
    @classmethod
    def read_source(cls, source_chars: Iterator[str]) -> Iterator[str]:
        while char := next(source_chars, None):
            match char:
                case "{":
                    pass
                case "[":
                    pass
                case '"':
                    pass
                case _:
                    yield char

    @classmethod
    def read_io(cls, source: TextIO) -> Iterator[str]:
        yield from cls.read_source(read_text_io_by_characters(source))


@dataclass
class VariableSubstitution:
    variable_name: str

    def substitute(self) -> Iterator[str]:
        raise NotImplementedError()


class TCLWordBase:
    def substitute(self) -> Iterator[str]:
        raise NotImplementedError()

    @classmethod
    def read(cls, chars: Iterator[str]) -> TCLWordBase:
        raise NotImplementedError()


@dataclass
class TCLWord(TCLWordBase):
    value: str
    # ESCAPING_CHARS: ClassVar[dict] = {"a": "\a", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "v": "\v"}
    # HEX_DIGITS: ClassVar[str] = "0123456789abcdef"
    # MAX_NUM_OF_DIGITS_BY_ESCAPE_CHAR: ClassVar[dict] = {"x": 2, "u": 4, "U": 8}
    #
    # @classmethod
    # def _read_unicode_value(
    #     cls, first_digit: str, chars: Iterator[str], max_num_of_digits: int, base: Literal[8, 16]
    # ) -> tuple[str, Iterator[str]]:
    #     number_of_chars = 1
    #     digits = first_digit
    #     leftovers = None
    #     while number_of_chars < max_num_of_digits:
    #         char = next(chars, None)
    #         if char is None:
    #             leftovers = char
    #             break
    #         if char not in cls.HEX_DIGITS[:base]:
    #             leftovers = char
    #             break
    #         digits += char
    #
    #     value = chr(int(digits, base))
    #     iterator = chain(leftovers, chars) if leftovers else chars
    #     return value, iterator
    #
    # @classmethod
    # def read_backslash(cls, chars: Iterator[str], in_double_quote: bool = True) -> tuple[str, Iterator[str]]:
    #     while char := next(chars, None):
    #         if char in "abfnrtv":
    #             return cls.ESCAPING_CHARS[char], chars
    #         if char.isdigit():
    #             return cls._read_unicode_value(char, chars, 3, 8)
    #         if char in "xuU":
    #             first_digit = next(chars, None)
    #             if not first_digit or first_digit not in cls.HEX_DIGITS:
    #                 return char, chain(first_digit, chars) if first_digit is not None else chars
    #             return cls._read_unicode_value(char, chars, cls.MAX_NUM_OF_DIGITS_BY_ESCAPE_CHAR[char], 16)
    #         return char, chars
    #     if in_double_quote:
    #         raise ValueError()
    #     return "\\", chars

    def substitute(self) -> Iterator[str]:
        return iter(self.value)

    @classmethod
    def read(cls, chars: Iterator[str]) -> Self:
        name = ""
        while char := next(chars, None):
            match char:
                case " " | "\t":
                    break
                # case "\n":
                #     raise ValueError()
                case _:
                    name += char
        return cls(name)


@dataclass
class TCLBracketWord(TCLWordBase):
    script: TCLScript

    def substitute(self) -> Iterator[str]:
        raise NotImplementedError()

    @classmethod
    def _iterate_in_bracket(cls, chars: Iterator[str]) -> Iterator[str]:
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
    def read(cls, chars: Iterator[str]) -> Self:
        return cls(TCLScript.read(cls._iterate_in_bracket(chars)))


@dataclass
class TCLDoubleQuotedWord(TCLWordBase):
    value: str

    def substitute(self) -> Iterator[str]:
        return iter(self.value)

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
    def read(cls, chars: Iterator[str]) -> Self:
        value: list[str] = []
        while char := next(chars, None):
            match char:
                case "\\":
                    value.append(cls.read_backslash(chars))
                case '"':
                    break
                case _:
                    value.append(char)

        return cls("".join(value))


@dataclass
class TCLBracesWord(TCLWordBase):
    value: str

    def substitute(self) -> Iterator[str]:
        return iter(self.value)

    @classmethod
    def read(cls, chars: Iterator[str]) -> Self:
        collected_chars: list[str] = []

        depth = 0
        while char := next(chars, None):
            match char:
                case "{":
                    depth += 1
                    collected_chars.append(char)
                case "}":
                    if depth == 0:
                        break
                    depth -= 1
                    collected_chars.append(char)
                case "\n":
                    if collected_chars:
                        collected_chars.append(char)
                case _:
                    collected_chars.append(char)

        return cls("".join(collected_chars))

    def __str__(self) -> str:
        return self.value


TCLCommandArguments = TCLBracesWord | TCLDoubleQuotedWord | TCLBracketWord | VariableSubstitution | TCLWord


@dataclass
class TCLCommand(TCLWordBase):
    name: str
    args: list[TCLCommandArguments]

    def substitute(self) -> Iterator[str]:
        raise NotImplementedError()

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
    def read(cls, chars: Iterator[str]) -> Self:
        name = cls.read_name(chars)

        arguments: list[TCLCommandArguments] = []
        while char := next(chars, None):
            match char:
                case "\n" | ";":
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
                    arguments.append(TCLWord.read(chars))
        return cls(name, arguments)

    def __str__(self) -> str:
        representation = [self.name]
        for arg in self.args:
            representation.append(str(arg))

        return "\n".join(representation)


EMPTY_COMMAND = TCLCommand(name="", args=[])


@dataclass
class TCLScript(TCLWordBase):
    commands: list[TCLCommand]

    def substitute(self) -> Iterator[str]:
        raise NotImplementedError()

    @classmethod
    def read(cls, chars: Iterator[str], in_bracket: bool = False) -> Self:
        commands: list[TCLCommand] = []
        while char := next(chars, None):
            match char:
                case "\n" | ";" | " ":
                    continue
                case _:
                    commands.append(TCLCommand.read(chain([char], chars)))

        return cls(commands)

    @classmethod
    def read_text_io(cls, text_io: TextIO) -> Self:
        return cls.read(read_text_io_by_characters(text_io))

    def __str__(self) -> str:
        representation = []
        for expression in self.commands:
            representation.append(str(expression))

        return "\n".join(representation)


@dataclass
class TCLList:
    words: list[TCLWord | TCLDoubleQuotedWord | TCLBracesWord | TCLBracketWord]

    @classmethod
    def words_iterator(
        cls, chars: Iterator[str]
    ) -> Iterator[TCLWord | TCLDoubleQuotedWord | TCLBracesWord | TCLBracketWord]:
        while char := next(chars, None):
            match char:
                case '"':
                    yield TCLDoubleQuotedWord.read(chars)
                case "{":
                    yield TCLBracesWord.read(chars)
                case "[":
                    yield TCLBracketWord.read(chars)
                case "}" | "]":
                    raise ValueError()
                case " " | "\t" | "\n":
                    continue
                case _:
                    yield TCLWord.read(chain([char], chars))

    @classmethod
    def interpertize(
        cls, list_word: TCLWord | TCLDoubleQuotedWord | TCLBracesWord | TCLBracketWord | VariableSubstitution
    ) -> Self:
        if isinstance(list_word, TCLWord | TCLBracesWord):
            return cls(list(cls.words_iterator(iter(list_word.value))))
        if isinstance(list_word, TCLBracketWord | TCLDoubleQuotedWord | VariableSubstitution):
            return cls(list(cls.words_iterator(iter(list_word.substitute()))))
        raise TypeError()


@dataclass
class TCLExpression:
    word: TCLCommandArguments

    def substitute(self) -> Iterator[str]:
        return iter("0")

    @classmethod
    def interpertize(cls, word: TCLCommandArguments) -> Self:
        return cls(word)


@dataclass
class TCLCommandIf:
    if_part: tuple[TCLExpression, TCLScript]
    elseif_parts: list[tuple[TCLExpression, TCLScript]]
    else_part: TCLScript | None

    @classmethod
    def _read_if(
        cls,
        args_iterator: Iterator[TCLCommandArguments],
    ) -> tuple[TCLExpression, TCLScript]:
        expression = TCLExpression.interpertize(next(args_iterator))
        body = next(args_iterator)
        if body.substitute() == "then":
            body = next(args_iterator)
        return expression, TCLScript.read(body.substitute())

    @classmethod
    def interpertize(cls, tcl_command: TCLCommand) -> Self:
        args_iterator = chain([TCLWord("if")], tcl_command.args)

        if_part: tuple[TCLExpression, TCLScript] | None = None
        elseif_parts: list[tuple[TCLExpression, TCLScript]] = []
        else_part: TCLScript | None = None

        while argument := next(args_iterator, None):
            match "".join(argument.substitute()):
                case "if":
                    if_part = cls._read_if(args_iterator)
                case "elseif":
                    elseif_parts.append(cls._read_if(args_iterator))
                case "else":
                    else_part = TCLScript.read(next(args_iterator).substitute())
                case _:
                    raise ValueError()

        if if_part is None:
            raise ValueError()

        return cls(if_part, elseif_parts, else_part)


EMTPY_SCRIPT = TCLScript([EMPTY_COMMAND])


# def tokenize_tcl_particle(value: str) -> list[str]:
#     tokens = []
#
#     chars = iter(value)
#     current_token = ""
#     while char := next(chars, None):
#         match char:
#             case "\\":
#                 next_char = next(chars, None)
#                 if not next_char:
#                     if current_token:
#                         tokens.append(current_token)
#                         current_token = ""
#                     tokens.append("\n")
#                     break
#                 current_token += char + next(chars)
#             case "{" | "}" | '"' | "[" | "]":
#                 if current_token:
#                     tokens.append(current_token)
#                     current_token = ""
#                 tokens.append(char)
#             case _:
#                 current_token += char
#
#     if current_token:
#         tokens.append(current_token)
#
#     return tokens


# def read_tcl_words(source_code: TextIO) -> Iterator[str]:
#     for full_line in source_code:
#         line = full_line.strip()
#         if line.startswith("#"):
#             continue
#         continue_to_next_line = False
#         for particle in line.split():
#             tokens = tokenize_tcl_particle(particle)
#             for token in tokens:
#                 if token == "\n":
#                     continue_to_next_line = True
#                 else:
#                     yield token
#         if not continue_to_next_line:
#             yield "\n"


def read_tcl_file(source_file_path: Path) -> TCLScript:
    with open(source_file_path) as source_file:
        return TCLScript.read_text_io(source_file)
