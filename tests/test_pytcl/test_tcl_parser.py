import pytest

from pytcl.errors import TCLSubstituteError
from pytcl.words import TCLDoubleQuotedWord, VariableSubstitution


def test_variable_substitution_word__get_empty__fail():
    instance = VariableSubstitution.read("abc")

    assert instance.variable_name == "abc"
    assert instance.origin == "abc"


def test_double_quoted_word__substitute():
    instance = TCLDoubleQuotedWord.read("$")

    assert instance.substitute(namespace={}) == "$"

    # ---

    instance = TCLDoubleQuotedWord.read("$ ")

    assert instance.substitute(namespace={}) == "$ "

    # ---

    instance = TCLDoubleQuotedWord.read("$abc")
    with pytest.raises(TCLSubstituteError, match='can\'t read "abc": no such variable'):
        instance.substitute(namespace={})

    # ---

    instance = TCLDoubleQuotedWord.read("$abc")

    assert instance.substitute(namespace={"abc": "22"}) == "22"

    # ---

    instance = TCLDoubleQuotedWord.read("aaa $abc bbb")

    assert instance.substitute(namespace={"abc": "22"}) == "aaa 22 bbb"

    # ---

    instance = TCLDoubleQuotedWord.read("aaa $abcd bbb")
    with pytest.raises(TCLSubstituteError, match='can\'t read "abcd": no such variable'):
        instance.substitute(namespace={"abc": "22"})
