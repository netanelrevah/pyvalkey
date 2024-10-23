from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from itertools import chain
from typing import Any, Self

from pytcl.types import TCLList
from pytcl.words import TCLBracesWord, TCLCommand, TCLCommandArguments, TCLScript, TCLWord


@dataclass
class TCLCommandForEach:
    variables_names_and_values_lists: list[tuple[tuple[str, ...], TCLList]]
    body: TCLScript

    @classmethod
    def interpertize(cls, tcl_command: TCLCommand, namespace: dict[str, Any]) -> Self:
        args_iterator = iter(tcl_command.args)

        variables_names_and_lists = []
        body: TCLScript | None = None
        while argument := next(args_iterator, None):
            values_list = next(args_iterator, None)

            if values_list is None:
                if not isinstance(argument, TCLBracesWord):
                    raise ValueError()
                body = TCLScript.read(argument.substitute_iterator(namespace))
                break

            names_list: tuple[str, ...]
            if isinstance(argument, TCLWord):
                names_list = (argument.substitute(namespace),)
            elif isinstance(argument, TCLBracesWord):
                names_list_words = []
                for word in TCLList.interpertize(argument, namespace).words:
                    names_list_words.append(word)
                names_list = tuple(names_list_words)
            else:
                raise ValueError()

            variables_names_and_lists.append((names_list, TCLList.interpertize(values_list, namespace)))

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
        cls, args_iterator: Iterator[TCLCommandArguments], namespace: dict[str, Any]
    ) -> tuple[TCLExpression, TCLScript]:
        expression = TCLExpression.interpertize(next(args_iterator), namespace)
        body = next(args_iterator)
        if body.substitute(namespace) == "then":
            body = next(args_iterator)
        return expression, TCLScript.read(body.substitute_iterator(namespace))

    @classmethod
    def interpertize(cls, tcl_command: TCLCommand, namespace: dict[str, Any]) -> Self:
        args_iterator = chain([TCLWord("if")], tcl_command.args)

        if_part: tuple[TCLExpression, TCLScript] | None = None
        elseif_parts: list[tuple[TCLExpression, TCLScript]] = []
        else_part: TCLScript | None = None

        while argument := next(args_iterator, None):
            match argument.substitute(namespace):
                case "if":
                    if_part = cls._read_if(args_iterator, namespace)
                case "elseif":
                    elseif_parts.append(cls._read_if(args_iterator, namespace))
                case "else":
                    else_part = TCLScript.read(next(args_iterator).substitute_iterator(namespace))
                case _:
                    raise ValueError()

        if if_part is None:
            raise ValueError()

        return cls(if_part, elseif_parts, else_part)


@dataclass
class TCLExpression:
    word: TCLCommandArguments

    @classmethod
    def interpertize(cls, word: TCLCommandArguments, namespace: dict[str, Any]) -> Self:
        return cls(word)
