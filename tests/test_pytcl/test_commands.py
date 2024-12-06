import pytest
from parametrization import Parametrization

from pytcl.commands import TCLExpression
from pytcl.errors import TCLSubstituteError
from pytcl.words import TCLBracesWord, TCLWord


@Parametrization.parameters("word", "expected_interpretation", "should_raise")
@Parametrization.default_parameters(should_raise=False)
@Parametrization.case("6.1 full", [TCLBracesWord("3.1 + $a")], "6.1")
@Parametrization.case("6.1 in parts", [TCLWord("3.1"), TCLWord("+"), TCLWord("3")], "6.1")
@Parametrization.case("5.6 in parts", [TCLBracesWord('2 + "$a.$b"')], "5.6")
@Parametrization.case("5.6 full", [TCLWord("2"), TCLWord("+"), TCLWord("3.6")], "5.6")
@Parametrization.case("32 in parts", [TCLWord("4"), TCLWord("*"), TCLWord("8")], "32")
@Parametrization.case("string compare", [TCLBracesWord('{word one} < "word $a"')], "0")
@Parametrization.case("8<7", [TCLBracesWord("4*2 < 7")], "0")
@Parametrization.case("512", [TCLBracesWord("2**3**2")], "512")
def test_tcl_expression__substitute(word, expected_interpretation, should_raise):
    namespace = {"a": "3", "b": "6"}

    if should_raise:
        with pytest.raises(TCLSubstituteError, match=expected_interpretation):
            TCLExpression.interpertize(word, namespace=namespace)
    else:
        instance = TCLExpression.interpertize(word, namespace=namespace)

        assert instance.execute(namespace) == expected_interpretation
