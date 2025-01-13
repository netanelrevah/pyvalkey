from __future__ import annotations

import functools
import operator
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from functools import reduce
from itertools import chain
from typing import Any, ClassVar, Literal, Self, TypeVar, dataclass_transform

from pytcl.errors import TCLInterpretationError
from pytcl.iterators import CharsIterator
from pytcl.types import TCLList
from pytcl.words import (
    TCLBracesWord,
    TCLCommandArguments,
    TCLDoubleQuotedWord,
    TCLScript,
    TCLVariableSubstitutionWord,
    TCLWord,
)


class TCLCommandBase:
    def execute(self, namespace: dict[str, Any]) -> str:
        raise NotImplementedError()

    @classmethod
    def interpertize(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Self:
        raise NotImplementedError()


@dataclass
class TCLCommandForEach(TCLCommandBase):
    variables_names_and_values_lists: list[tuple[tuple[str, ...], TCLList]]
    body: TCLScript

    @classmethod
    def interpertize(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Self:
        args_iterator = iter(arguments)

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
class TCLCommandIf(TCLCommandBase):
    if_part: tuple[TCLExpression, TCLScript]
    elseif_parts: list[tuple[TCLExpression, TCLScript]]
    else_part: TCLScript | None

    @classmethod
    def _read_if(
        cls, args_iterator: Iterator[TCLCommandArguments], namespace: dict[str, Any]
    ) -> tuple[TCLExpression, TCLScript]:
        expression_word = next(args_iterator)
        expression = TCLExpression.interpertize([expression_word], namespace)
        body = next(args_iterator)
        if body.substitute(namespace) == "then":
            body = next(args_iterator)
        return expression, TCLScript.read(body.substitute_iterator(namespace))

    @classmethod
    def interpertize(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Self:
        args_iterator: Iterator[TCLCommandArguments] = chain([TCLWord("if")], arguments)

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
class TCLStringIs(TCLCommandBase):
    CLASSES: ClassVar[dict[str, TCLCommandBase]] = {}

    class_: str
    string: str
    strict: bool = False
    fail_index: str = ""

    def execute(self, namespace: dict[str, Any]) -> str:
        match self.class_:
            case "true":
                return "1" if self.string.lower() in ["yes", "true", "1", "on"] else "0"
            case "false":
                return "1" if self.string.lower() in ["no", "false", "0", "off"] else "0"
            case "boolean":
                return "1" if self.string.lower() in ["yes", "true", "1", "on", "no", "false", "0", "off"] else "0"
            case _:
                raise TCLInterpretationError()

    @classmethod
    def interpertize(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Self:
        class_ = arguments[0].substitute(namespace)

        if class_ not in cls.CLASSES:
            raise TCLInterpretationError()

        string = arguments[-1].substitute(namespace)

        strict = False
        fail_index = ""
        arguments_iterator: Iterator[TCLCommandArguments] = iter(arguments[1:-1])
        while argument := next(arguments_iterator, None):
            match argument.substitute(namespace):
                case "-strict":
                    strict = True
                case "-failindex":
                    fail_index = next(arguments_iterator).substitute(namespace)
                case _:
                    raise TCLInterpretationError()

        return cls(class_, string, strict, fail_index)


TCLMathOperandType = str | TCLCommandArguments


@functools.total_ordering
class TCLMathOperator(TCLCommandBase):
    PRECEDENCE: ClassVar[int]
    NUMBER_OF_OPERANDS: ClassVar[int | None] = None
    ASSOCIATIVITY: ClassVar[Literal["L", "R"]] = "L"

    def __init__(self, *args: TCLMathOperandType | Sequence[TCLMathOperandType]) -> None: ...

    @classmethod
    def from_args(cls, *args: TCLMathOperandType) -> Self:
        raise NotImplementedError()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TCLMathOperator):
            raise ValueError()
        return self.PRECEDENCE < other.PRECEDENCE

    @classmethod
    def _parse_operand(cls, operand: TCLMathOperandType, namespace: dict[str, Any]) -> int | float | str:
        if isinstance(operand, TCLMathOperator):
            operand = operand.execute(namespace)
        if isinstance(operand, TCLCommandArguments):
            operand = operand.substitute(namespace)
        try:
            return int(operand)
        except ValueError:
            try:
                return float(operand)
            except ValueError:
                return operand

    @classmethod
    def _operator_execute(cls, operator: Callable[..., int | float], *operands: str, namespace: dict[str, Any]) -> str:
        result = reduce(operator, map(lambda x: cls._parse_operand(x, namespace), operands))
        if isinstance(result, bool):
            return "1" if result else "0"
        return str(result)


T = TypeVar("T", bound=TCLMathOperator)


def tcl_math_operator(
    operator: str, precedence: int, number_of_operands: int | None = None, associativity: Literal["L", "R"] = "L"
) -> Callable[[type[T]], type[T]]:
    @dataclass_transform()
    def wrapper(cls: type[T]) -> type[T]:
        assert issubclass(cls, TCLMathOperator)

        cls.PRECEDENCE = precedence
        cls.NUMBER_OF_OPERANDS = number_of_operands
        cls.ASSOCIATIVITY = associativity

        if number_of_operands is None:

            def from_args(a_cls: type[T], *args: TCLMathOperandType) -> T:
                return a_cls(args)
        else:

            def from_args(a_cls: type[T], *args: TCLMathOperandType) -> T:
                return a_cls(*args)

        setattr(cls, "from_args", classmethod(from_args))

        operator_class = dataclass(cls)

        return operator_class

    return wrapper


class TCLMathOperatorBooleanNegation(TCLMathOperator):
    operand: str

    def execute(self, namespace: dict[str, Any]) -> str:
        return "1" if TCLStringIs("false", self.operand).execute(namespace) == "0" else "0"


class TCLMathOperatorBitWiseNegation(TCLMathOperator):
    operand: str

    def execute(self, namespace: dict[str, Any]) -> str:
        return str(~int(self.operand))


class TCLMathOperatorSubtraction(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorSummation(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorPower(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorShiftLeft(TCLMathOperator):
    left_operand: str
    right_operand: str


class TCLMathOperatorShiftRight(TCLMathOperator):
    left_operand: str
    right_operand: str


class TCLMathOperatorBitWiseOr(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorBitWiseAnd(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorBitWiseXor(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorEqual(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorNotEqual(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorLess(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorGreater(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorLessOrEqual(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorGreaterOrEqual(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorMultiply(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorDivision(TCLMathOperator):
    operands: list[str]


class TCLMathOperatorModule(TCLMathOperator):
    operands: list[str]


@dataclass
@functools.total_ordering
class ExpressionOperator:
    operator_func: Callable[..., int | float | bool | tuple]
    representation: str
    precedence: int
    number_of_operands: int | None = None
    associativity: Literal["R", "L"] = "L"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ExpressionOperator):
            raise ValueError()
        return self.precedence < other.precedence

    @classmethod
    def _parse_operand(cls, operand: TCLMathOperandType, namespace: dict[str, Any]) -> int | float | str:
        if isinstance(operand, TCLCommandArguments):
            operand = operand.substitute(namespace)
        try:
            return int(operand)
        except ValueError:
            try:
                return float(operand)
            except ValueError:
                return operand

    def apply(self, *operands: str, namespace: dict[str, Any]) -> str:
        result = reduce(self.operator_func, map(lambda x: self._parse_operand(x, namespace), operands))
        if isinstance(result, bool):
            return "1" if result else "0"
        return str(result)


EXPRESSION_OPERATOR = {
    expression_operator.representation: expression_operator
    for expression_operator in [
        ExpressionOperator(operator.not_, "!", 6, 1, "R"),
        ExpressionOperator(operator.inv, "~", 6, 1, "R"),
        ExpressionOperator(operator.sub, "-", 1),
        ExpressionOperator(operator.add, "+", 1),
        ExpressionOperator(operator.pow, "**", 3, associativity="R"),
        ExpressionOperator(operator.lshift, "<<", 5, number_of_operands=2),
        ExpressionOperator(operator.rshift, ">>", 5, number_of_operands=2),
        ExpressionOperator(operator.or_, "|", 4),
        ExpressionOperator(operator.and_, "&", 4),
        ExpressionOperator(operator.xor, "^", 4),
        ExpressionOperator(operator.eq, "==", 0),
        ExpressionOperator(operator.ne, "!=", 0),
        ExpressionOperator(operator.lt, "<", 0),
        ExpressionOperator(operator.gt, ">", 0),
        ExpressionOperator(operator.le, "<=", 0),
        ExpressionOperator(operator.ge, ">=", 0),
        ExpressionOperator(operator.mul, "*", 2),
        ExpressionOperator(operator.truediv, "/", 2),
        ExpressionOperator(operator.mod, "%", 2),
        ExpressionOperator(lambda *operands: any(operands), "||", -1),
        ExpressionOperator(lambda *operands: all(operands), "&&", -2),
        ExpressionOperator(lambda x, y: (x, y), ":", -3),
        ExpressionOperator(lambda x, y: y[0] if x else y[1], "?", -4),
    ]
}


@dataclass
class TCLExpression(TCLCommandBase):
    postfix: list[str | TCLCommandArguments]

    @classmethod
    def _iterate_expression(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Iterator[str]:
        for argument in arguments[:-1]:
            yield from argument.substitute_iterator(namespace)
            yield from " "
        yield from arguments[-1].substitute_iterator(namespace)

    @classmethod
    def _read_operator(cls, chars: CharsIterator) -> str:
        chars.push()

        value = ""
        while char := next(chars, None):
            if value + char not in EXPRESSION_OPERATOR:
                chars.push_back()
                break
            value += char

        chars.pop()
        return value

    @classmethod
    def _read_value(cls, chars: CharsIterator) -> str:
        chars.push()

        value = ""
        while char := next(chars, None):
            match char:
                case " " | "\t" | "\n":
                    break
                case "*" | "<":
                    chars.push_back()
                    break
                case _:
                    value += char

        chars.pop()
        return value

    @classmethod
    def _iterate_tokens(cls, chars: Iterator[str]) -> Iterator[str | TCLCommandArguments]:
        iterator = CharsIterator.of(chars)
        iterator.push()

        while char := next(iterator, None):
            match char:
                case " " | "\t" | "\n":
                    continue
                case '"':
                    yield TCLDoubleQuotedWord.read(iterator)
                case "$":
                    yield TCLVariableSubstitutionWord.read(iterator)
                case "{":
                    yield TCLBracesWord.read(iterator)
                case "*" | "<":
                    iterator.push_back()
                    yield cls._read_operator(iterator)
                case _:
                    iterator.push_back()
                    yield cls._read_value(iterator)

    @classmethod
    def interpertize(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Self:
        expression_iterator = cls._iterate_expression(arguments, namespace)
        tokens_iterator = cls._iterate_tokens(expression_iterator)

        operators_stack: list[str] = []
        postfix: list[str | TCLCommandArguments] = []
        while token := next(tokens_iterator, None):
            if isinstance(token, str) and (expression_operator := EXPRESSION_OPERATOR.get(token)):
                while operators_stack:
                    if not (top_operator := EXPRESSION_OPERATOR.get(operators_stack[-1])):
                        break
                    if (
                        expression_operator.associativity == "L"
                        and expression_operator.precedence > top_operator.precedence
                    ):
                        break
                    if (
                        expression_operator.associativity == "R"
                        and expression_operator.precedence >= top_operator.precedence
                    ):
                        break
                    postfix.append(operators_stack.pop())
                operators_stack.append(token)
            elif isinstance(token, str) and token == "(":
                operators_stack.append(token)
            elif isinstance(token, str) and token == ")":
                while operators_stack:
                    operator_string = operators_stack.pop()
                    if operator_string == "(":
                        break
                    postfix.append(operator_string)
            else:
                postfix.append(token)

        while operators_stack:
            postfix.append(operators_stack.pop())

        return cls(postfix)

    def execute(self, namespace: dict[str, Any]) -> str:
        postfix = list(self.postfix)
        commands_stack: list[TCLMathOperandType] = []
        while postfix:
            token = postfix.pop(0)

            if isinstance(token, str) and (expression_operator := EXPRESSION_OPERATOR.get(token, None)) is not None:
                operands = []
                for _ in range(expression_operator.number_of_operands if expression_operator.number_of_operands else 2):
                    operands.append(commands_stack.pop())
                commands_stack.append(expression_operator.from_args(*reversed(operands)))
                continue

            commands_stack.append(token)

        if len(commands_stack) != 1 or not isinstance(commands_stack[0], TCLMathOperator):
            raise TCLInterpretationError()

        return commands_stack[0]
