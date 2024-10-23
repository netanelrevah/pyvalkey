import pytest
from parametrization import Parametrization

from pytcl.commands import TCLExpression
from pytcl.errors import TCLSubstituteError


@Parametrization.parameters("chars", "expected_interpretation", "should_raise")
@Parametrization.default_parameters(should_raise=False)
@Parametrization.case("got dollar", "$", "$")
def test_tcl_expression__substitute(chars, expected_interpretation, should_raise):
    if should_raise:
        with pytest.raises(TCLSubstituteError, match=expected_interpretation):
            TCLExpression.interpertize(chars, namespace={"abc": 22})
    else:
        instance = TCLExpression.interpertize(chars, namespace={"abc": 22})

        assert instance.word == expected_interpretation
