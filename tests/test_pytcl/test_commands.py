import pytest
from parametrization import Parametrization

from pytcl.commands import TCLExpression
from pytcl.errors import TCLSubstituteError
from pytcl.words import TCLWord


@Parametrization.parameters("word", "expected_interpretation", "should_raise")
@Parametrization.default_parameters(should_raise=False)
@Parametrization.case("", [TCLWord("8.2"), TCLWord("+"), TCLWord("6")], "14.2")
def test_tcl_expression__substitute(word, expected_interpretation, should_raise):
    if should_raise:
        with pytest.raises(TCLSubstituteError, match=expected_interpretation):
            TCLExpression.interpertize(word, namespace={"abc": 22})
    else:
        instance = TCLExpression.interpertize(word, namespace={"abc": 22})

        assert instance.execute() == expected_interpretation
