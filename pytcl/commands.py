from __future__ import annotations

import functools
import operator
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from functools import reduce
from itertools import chain
from typing import Any, ClassVar, Literal, Self, TypeVar, dataclass_transform

from pytcl.errors import TCLInterpretationError
from pytcl.types import TCLList
from pytcl.words import TCLBracesWord, TCLCommandArguments, TCLScript, TCLWord


class TCLCommandBase:
    def execute(self) -> str:
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

    def execute(self) -> str:
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


@functools.total_ordering
class TCLMathOperator(TCLCommandBase):
    PRECEDENCE: ClassVar[int]
    NUMBER_OF_OPERANDS: ClassVar[int | None] = None
    ASSOCIATIVITY: ClassVar[Literal["L", "R"]] = "L"

    def __init__(self, *args: str | TCLMathOperator | Sequence[str | TCLMathOperator]) -> None: ...

    @classmethod
    def from_args(cls, *args: str | TCLMathOperator) -> Self:
        raise NotImplementedError()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TCLMathOperator):
            raise ValueError()
        return self.PRECEDENCE < other.PRECEDENCE


class Operators:
    OPERATOR_CLASSES: ClassVar[dict[str, type[TCLMathOperator]]] = {}


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

            def from_args(a_cls: type[T], *args: str | TCLMathOperator) -> T:
                return a_cls(args)
        else:

            def from_args(a_cls: type[T], *args: str | TCLMathOperator) -> T:
                return a_cls(*args)

        setattr(cls, "from_args", classmethod(from_args))

        operator_class = dataclass(cls)

        Operators.OPERATOR_CLASSES[operator] = operator_class

        return operator_class

    return wrapper


@tcl_math_operator("!", 6, 1, "R")
class TCLMathOperatorBooleanNegation(TCLMathOperator):
    operand: str


@tcl_math_operator("~", 6, 1, "R")
class TCLMathOperatorBitWiseNegation(TCLMathOperator):
    operand: str


@tcl_math_operator("-", 1)
class TCLMathOperatorSubtraction(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("+", 1)
class TCLMathOperatorSummation(TCLMathOperator):
    operands: list[str]

    def execute(self) -> str:
        return str(reduce(operator.add, map(float, self.operands)))


@tcl_math_operator("**", 3, associativity="R")
class TCLMathOperatorPower(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("<<", 5, number_of_operands=2)
class TCLMathOperatorShiftLeft(TCLMathOperator):
    left_operand: str
    right_operand: str


@tcl_math_operator(">>", 5, number_of_operands=2)
class TCLMathOperatorShiftRight(TCLMathOperator):
    left_operand: str
    right_operand: str


@tcl_math_operator("|", 4)
class TCLMathOperatorBitWiseOr(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("&", 4)
class TCLMathOperatorBitWiseAnd(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("^", 4)
class TCLMathOperatorBitWiseXor(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("==", 0)
class TCLMathOperatorEqual(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("!=", 0)
class TCLMathOperatorNotEqual(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("<", 0)
class TCLMathOperatorLess(TCLMathOperator):
    operands: list[str]


@tcl_math_operator(">", 0)
class TCLMathOperatorGreater(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("<=", 0)
class TCLMathOperatorLessOrEqual(TCLMathOperator):
    operands: list[str]


@tcl_math_operator(">=", 0)
class TCLMathOperatorGreaterOrEqual(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("*", 2)
class TCLMathOperatorMultiply(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("/", 2)
class TCLMathOperatorDivision(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("%", 2)
class TCLMathOperatorModule(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("||", -2)
class TCLMathOperatorLogicalOr(TCLMathOperator):
    operands: list[str]


@tcl_math_operator("&&", -1)
class TCLMathOperatorLogicalAnd(TCLMathOperator):
    operands: list[str]


@dataclass
class TCLExpression(TCLCommandBase):
    evaluator: TCLMathOperator

    @classmethod
    def _iterate_expression(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Iterator[str]:
        for argument in arguments[:-1]:
            yield from argument.substitute_iterator(namespace)
            yield from " "
        yield from arguments[-1].substitute_iterator(namespace)

    @classmethod
    def _read_value(cls, chars: Iterator[str]) -> str:
        value = ""
        while char := next(chars, None):
            match char:
                case " " | "\t" | "\n":
                    break
                case _:
                    value += char
        return value

    @classmethod
    def _iterate_tokens(cls, chars: Iterator[str]) -> Iterator[str]:
        while char := next(chars, None):
            match char:
                case " " | "\t" | "\n":
                    continue
                case _:
                    yield cls._read_value(chain([char], chars))

    @classmethod
    def interpertize(cls, arguments: list[TCLCommandArguments], namespace: dict[str, Any]) -> Self:
        expression_iterator = cls._iterate_expression(arguments, namespace)
        tokens_iterator = cls._iterate_tokens(expression_iterator)

        operators_stack: list[str] = []
        postfix: list[str] = []
        while token := next(tokens_iterator, None):
            if operator := Operators.OPERATOR_CLASSES.get(token):
                while operators_stack:
                    if not (top_operator := Operators.OPERATOR_CLASSES.get(operators_stack[-1])):
                        break
                    if operator.ASSOCIATIVITY == "L" and operator.PRECEDENCE > top_operator.PRECEDENCE:
                        break
                    if operator.ASSOCIATIVITY == "R" and operator.PRECEDENCE >= top_operator.PRECEDENCE:
                        break
                    postfix.append(operators_stack.pop())
                operators_stack.append(token)
            elif token == "(":
                operators_stack.append(token)
            elif token == ")":
                while operators_stack:
                    operator_string = operators_stack.pop()
                    if operator_string == "(":
                        break
                    postfix.append(operator_string)
            else:
                postfix.append(token)

        while operators_stack:
            postfix.append(operators_stack.pop())

        commands_stack: list[str | TCLMathOperator] = []
        while postfix:
            token = postfix.pop(0)

            if (operator := Operators.OPERATOR_CLASSES.get(token, None)) is not None:
                operands = []
                for _ in range(operator.NUMBER_OF_OPERANDS if operator.NUMBER_OF_OPERANDS else 2):
                    operands.append(commands_stack.pop())
                commands_stack.append(operator.from_args(*operands))
                continue

            commands_stack.append(token)

        if len(commands_stack) != 1 or not isinstance(commands_stack[0], TCLMathOperator):
            raise TCLInterpretationError()

        return cls(commands_stack[0])

    def execute(self) -> str:
        return self.evaluator.execute()