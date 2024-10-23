import pytest
from parametrization import Parametrization

from pytcl.errors import TCLSubstituteError
from pytcl.words import TCLDoubleQuotedWord, VariableSubstitution


def test_variable_substitution_word__get_empty__fail():
    instance = VariableSubstitution.read("abc")

    assert instance.variable_name == "abc"
    assert instance.origin == "abc"


@Parametrization.parameters("chars", "expected_value", "expected_substitute", "should_raise")
@Parametrization.default_parameters(should_raise=False)
@Parametrization.case("got dollar", "$", "$", "$")
@Parametrization.case("got two dollars", "$$", "$$", "$$")
@Parametrization.case("got dollar with space", "$ ", "$ ", "$ ")
@Parametrization.case("backslash newline", "a\\\nb", "a b", "a b")
@Parametrization.case("hex backslash", " $abc\n\\x20", " $abc\n\\x20", " 22\n ")
@Parametrization.case("complicated hex backslash", " $abc\n\\x2q", " $abc\n\\x2q", " 22\n\x02q")
@Parametrization.case("go two lines with subs", " $abc\nccc", " $abc\nccc", " 22\nccc")
@Parametrization.case("backslash cancel subs", "\\$abc", "\\$abc", "$abc")
@Parametrization.case("simple subs", "$abc", "$abc", "22")
@Parametrization.case("subs with prefix", "a$abc", "a$abc", "a22")
@Parametrization.case("subs with backslash", " $abc\n\\x20", " $abc\n\\x20", " 22\n ")
@Parametrization.case("wrapped subs", "aaa $abc bbb", "aaa $abc bbb", "aaa 22 bbb")
@Parametrization.case("simple error", "$abcd", "$abcd", 'can\'t read "abcd": no such variable', True)
@Parametrization.case("wrapped error", "aaa $abcd bbb", "aaa $abcd bbb", 'can\'t read "abcd": no such variable', True)
def test_double_quoted_word__substitute(chars, expected_value, expected_substitute, should_raise):
    instance = TCLDoubleQuotedWord.read(chars)

    assert instance.value == expected_value
    assert instance.origin == chars
    if should_raise:
        with pytest.raises(TCLSubstituteError, match=expected_substitute):
            instance.substitute(namespace={})
    else:
        assert instance.substitute(namespace={"abc": 22}) == expected_substitute
