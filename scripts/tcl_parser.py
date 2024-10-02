from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Self, TextIO


def read_text_io_by_characters(text_io: TextIO) -> Iterator[str]:
    while c := text_io.read(1):
        yield c


class TCLWordBase:
    def substitute(self) -> str:
        return "".join(self.substitute_iterator())

    def substitute_iterator(self) -> Iterator[str]:
        raise NotImplementedError()

    @classmethod
    def read(cls, chars: Iterator[str]) -> TCLWordBase:
        raise NotImplementedError()


@dataclass
class VariableSubstitution(TCLWordBase):
    variable_name: str

    def __str__(self) -> str:
        return "$" + self.variable_name

    def substitute_iterator(self) -> Iterator[str]:
        raise NotImplementedError()


@dataclass
class TCLWord(TCLWordBase):
    value: str

    def __str__(self) -> str:
        return self.value

    def substitute_iterator(self) -> Iterator[str]:
        return iter(self.value)

    @classmethod
    def read(cls, chars: Iterator[str]) -> Self:
        name = ""
        while char := next(chars, None):
            match char:
                case " " | "\t":
                    break
                case _:
                    name += char
        return cls(name)


@dataclass
class TCLBracketWord(TCLWordBase):
    script: TCLScript

    def substitute_iterator(self) -> Iterator[str]:
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

    def substitute_iterator(self) -> Iterator[str]:
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

    def __str__(self) -> str:
        if "\n" in self.value:
            return "{\n" + self.value.rstrip() + "\n}"
        else:
            return "{" + self.value + "}"

    def substitute_iterator(self) -> Iterator[str]:
        yield from self.value

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


TCLCommandArguments = TCLBracesWord | TCLDoubleQuotedWord | TCLBracketWord | VariableSubstitution | TCLWord


@dataclass
class TCLCommand(TCLWordBase):
    name: str
    args: list[TCLCommandArguments]

    def substitute_iterator(self) -> Iterator[str]:
        raise NotImplementedError()

    def __str__(self) -> str:
        string_value = [self.name, " "]

        for arg in self.args[:-1]:
            string_value.append(str(arg))
            string_value.append(" ")
        string_value.append(str(self.args[-1]))
        return "".join(string_value)

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
                    arguments.append(TCLWord.read(chain(char, chars)))
        return cls(name, arguments)


EMPTY_COMMAND = TCLCommand(name="", args=[])


@dataclass
class TCLScript(TCLWordBase):
    commands: list[TCLCommand]

    def substitute_iterator(self) -> Iterator[str]:
        raise NotImplementedError()

    @classmethod
    def handle_comment(cls, chars: Iterator[str]) -> None:
        while char := next(chars, None):
            if char == "\n":
                break

    @classmethod
    def read(cls, chars: Iterator[str], in_bracket: bool = False) -> Self:
        commands: list[TCLCommand] = []
        while char := next(chars, None):
            match char:
                case "\n" | ";" | " ":
                    continue
                case "#":
                    cls.handle_comment(chars)
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
            return cls(list(cls.words_iterator(list_word.substitute_iterator())))
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
class TCLVariableName:
    name: TCLWord

    def substitute(self) -> Iterator[str]:
        return self.name.substitute_iterator()

    @classmethod
    def interpertize(cls, word: TCLWord) -> Self:
        return cls(word)


@dataclass
class TCLCommandForEach:
    variables_names_and_values_lists: list[tuple[tuple[TCLWord, ...], TCLList]]
    body: TCLScript

    @classmethod
    def interpertize(cls, tcl_command: TCLCommand) -> Self:
        args_iterator = iter(tcl_command.args)

        variables_names_and_lists = []
        body: TCLScript | None = None
        while argument := next(args_iterator, None):
            values_list = next(args_iterator, None)

            if values_list is None:
                if not isinstance(argument, TCLBracesWord):
                    raise ValueError()
                body = TCLScript.read(argument.substitute_iterator())
                break

            names_list: tuple[TCLWord, ...]
            if isinstance(argument, TCLWord):
                names_list = (argument,)
            elif isinstance(argument, TCLBracesWord):
                names_list_words = []
                for word in TCLList.interpertize(argument).words:
                    if not isinstance(word, TCLWord):
                        raise ValueError()
                    names_list_words.append(word)
                names_list = tuple(names_list_words)
            else:
                raise ValueError()

            variables_names_and_lists.append((names_list, TCLList.interpertize(values_list)))

        if not variables_names_and_lists or not body:
            raise ValueError()

        return cls(variables_names_and_lists, body)


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
        if "".join(body.substitute_iterator()) == "then":
            body = next(args_iterator)
        return expression, TCLScript.read(body.substitute_iterator())

    @classmethod
    def interpertize(cls, tcl_command: TCLCommand) -> Self:
        args_iterator = chain([TCLWord("if")], tcl_command.args)

        if_part: tuple[TCLExpression, TCLScript] | None = None
        elseif_parts: list[tuple[TCLExpression, TCLScript]] = []
        else_part: TCLScript | None = None

        while argument := next(args_iterator, None):
            match "".join(argument.substitute_iterator()):
                case "if":
                    if_part = cls._read_if(args_iterator)
                case "elseif":
                    elseif_parts.append(cls._read_if(args_iterator))
                case "else":
                    else_part = TCLScript.read(next(args_iterator).substitute_iterator())
                case _:
                    raise ValueError()

        if if_part is None:
            raise ValueError()

        return cls(if_part, elseif_parts, else_part)


EMTPY_SCRIPT = TCLScript([EMPTY_COMMAND])


def read_tcl_file(source_file_path: Path) -> TCLScript:
    with open(source_file_path) as source_file:
        return TCLScript.read_text_io(source_file)
