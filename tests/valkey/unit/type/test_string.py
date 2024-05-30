import time
from random import randint
from typing import TypeVar

import pytest
import redis
from _pytest.python_api import raises

from tests.utils import assert_raises

pytestmark = pytest.mark.string


def test_set_and_get_an_item(s: redis.Redis):
    s.set("x", "foobar")
    assert s.get("x") == b"foobar"


def test_set_and_get_an_empty_item(s: redis.Redis):
    s.set("x", "")
    assert s.get("x") == b""


def test_very_big_payload_in_get_or_set(s: redis.Redis):
    buffer = "abcd" * 1000000

    s.set("foo", buffer)
    assert s.get("foo") == buffer.encode()


@pytest.mark.slow
def test_very_big_payload_random_access(s: redis.Redis):
    payload = []
    for index in range(100):
        size = 1 + randint(0, 100000)
        buffer = f"pl-{index}" * size
        payload.append(buffer)
        s.set(f"bigpayload_{index}", buffer)

    for _ in range(1000):
        index = randint(0, 99)
        buffer = s.get(f"bigpayload_{index}").decode()
        assert buffer == payload[index]


@pytest.mark.slow
def test_set_10000_numeric_keys_and_access_all_them_in_reverse_order(s: redis.Redis):
    for number in range(10000):
        s.set(str(number), number)

    for number in range(9999, -1):
        value = s.get(str(number))
        assert int(value) == number

    assert s.dbsize() == 10000


@pytest.mark.skip("previously included")
def test_dbsize_should_be_10000_now(s: redis.Redis):
    pass


def test_setnx_target_key_missing(s: redis.Redis):
    s.delete("novar")
    assert s.setnx("novar", "foobared") is True
    assert s.get("novar") == b"foobared"


def test_setnx_target_key_exists(s: redis.Redis):
    s.set("novar", "foobared")
    assert s.setnx("novar", "blabla") is False
    assert s.get("novar") == b"foobared"


def test_setnx_against_not_expired_volatile_key(s: redis.Redis):
    s.set("x", 10)
    s.expire("x", 10000)
    assert s.setnx("x", 20) is False
    assert s.get("x") == b"10"


def test_setnx_against_expired_volatile_key(s: redis.Redis):
    for index in range(9999):
        s.setex(f"key-{index}", 3600, "value")

    s.set("x", 10)
    s.expire("x", 1)

    time.sleep(2)

    assert s.setnx("x", 20) is True
    assert s.get("x") == b"20"


def test_getex_ex_option(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", ex=10)
    assert 5 <= s.ttl("foo") <= 10


def test_getex_px_option(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", px=10000)
    assert 5000 <= s.pttl("foo") <= 10000


def test_getex_exat_option(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", exat=int(time.time() + 10))
    assert 5 <= s.ttl("foo") <= 10


def test_getex_pxat_option(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", pxat=int(time.time() * 1000 + 10000))
    assert 5000 <= s.pttl("foo") <= 10000


def test_getex_persist_option(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar", ex=10)
    s.getex("foo", persist=True)
    assert s.ttl("foo") == -1


def test_getex_no_option(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo")
    assert s.getex("foo") == b"bar"


def test_getex_syntax_errors(s: redis.Redis):
    with raises(redis.RedisError, match=""):
        s.execute_command("getex", "foo", "non-existent-option")


def test_getex_and_get_expired_key_or_not_exist(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar", px=1)
    time.sleep(0.002)
    assert s.getex("foo") is None
    assert s.get("foo") is None


def test_getex_no_arguments(s: redis.Redis):
    with raises(redis.RedisError, match=""):
        s.execute_command("getex")


def test_getdel_command(s: redis.Redis):
    s.delete("foo")
    s.set("foo", "bar")
    assert s.getdel("foo") == b"bar"
    assert s.getdel("foo") is None


@pytest.mark.xfail("not implemented")
def test_getdel_propagate_as_del_command_to_replica(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getex_without_argument_does_not_propagate_to_replica(s: redis.Redis):
    assert False


def test_mget(s: redis.Redis):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    assert s.mget("foo", "bar") == [b"BAR", b"FOO"]


def test_mget_against_non_existing_key(s: redis.Redis):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    assert s.mget("foo", "baazz", "bar") == [b"BAR", None, b"FOO"]


def test_mget_against_non_string_key(s: redis.Redis):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    s.sadd("myset", "ciao")
    s.sadd("myset", "bau")
    assert s.mget("foo", "baazz", "bar", "myset") == [b"BAR", None, b"FOO", None]


def test_getset_set_new_value(s: redis.Redis):
    assert s.getset("foo", "xyz") == b""
    assert s.get("foo") == b"xyz"


def test_getset_replace_old_value(s: redis.Redis):
    s.set("foo", "bar")
    assert s.getset("foo", "xyz") == b"bar"
    assert s.get("foo") == b"xyz"


def test_mset_base_case(s: redis.Redis):
    s.mset({"x": 10, "y": "foo bar", "z": "x x x x x x x\n\n\r\n"})
    assert s.mget("x", "y", "z") == [b"10", b"foo bar", b"x x x x x x x\n\n\r\n"]


E = TypeVar("E", bound=BaseException)


def test_mset_or_msetnx_wrong_number_of_args(s: redis.Redis):
    with assert_raises(redis.RedisError, "wrong number of arguments for 'mset' command"):
        s.execute_command("mset", "x", 10, "y", "foo bar", "z")
    with assert_raises(redis.RedisError, "wrong number of arguments for 'msetnx' command"):
        s.execute_command("msetnx", "x", 20, "y", "foo bar", "z")


def test_mset_with_already_existing_same_key_twice(s: redis.Redis):
    s.set("x", "x")
    s.execute_command("mset", "x", "xxx", "x", "yyy")
    assert s.get("x") == b"yyy"


def test_msetnx_with_already_existent_key(s: redis.Redis):
    s.set("x", "x")
    assert s.msetnx({"x1": "xxx", "y2": "yyy", "x": 20}) is False
    assert s.exists("x1") == 0
    assert s.exists("y2") == 0


def test_msetnx_with_not_existing_keys(s: redis.Redis):
    assert s.msetnx({"x1": "xxx", "y2": "yyy"}) is True
    assert s.get("x1") == b"xxx"
    assert s.get("y2") == b"yyy"


def test_msetnx_with_not_existing_keys_same_key_twice(s: redis.Redis):
    assert s.execute_command("msetnx", "x1", "xxx", "x1", "yyy") is True
    assert s.get("x1") == b"yyy"


def test_msetnx_with_already_existing_keys_same_key_twice(s: redis.Redis):
    assert s.set("x1", b"yyy")
    assert s.execute_command("msetnx", "x1", "xxx", "x1", "zzz") is False
    assert s.get("x1") == b"yyy"


def test_strlen_against_non_existing_key(s: redis.Redis):
    assert s.strlen("notakey") == 0


def test_strlen_against_integer_encoded_value(s: redis.Redis):
    s.set("myinteger", -555)
    assert s.strlen("myinteger") == 4


def test_strlen_against_plain_string(s: redis.Redis):
    s.set("mystring", "foozzz0123456789 baz")
    assert s.strlen("mystring") == 20


@pytest.mark.xfail("not implemented")
def test_setbit_against_non_existing_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setbit_against_string_encoded_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setbit_against_integer_encoded_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setbit_against_key_with_wrong_type(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setbit_with_out_of_range_bit_offset(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setbit_with_non_bit_argument(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setbit_fuzzing(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getbit_against_non_existing_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getbit_against_string_encoded_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getbit_against_integer_encoded_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setrange_against_non_existing_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setrange_against_string_encoded_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setrange_against_integer_encoded_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setrange_against_key_with_wrong_type(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setrange_with_out_of_range_offset(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getrange_against_non_existing_key(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getrange_against_wrong_key_type(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getrange_against_string_value(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getrange_against_integer_encoded_value(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getrange_fuzzing(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_coverage_substr(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_trim_on_set_with_big_value(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_can_detect_syntax_errors(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_nx_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_xx_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_option_with_no_previous_value(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_option_with_xx(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_option_with_xx_and_no_previous_value(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_option_with_nx(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_option_with_nx_and_previous_value(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_get_with_incorrect_type_should_result_in_wrong_type_error(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_ex_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_px_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_exat_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_pxat_option(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_extended_set_using_multiple_options_at_once(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_getrange_with_huge_ranges_github_issue_1844(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_lcs_basic(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_lcs_len(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_lcs_indexes(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_lcs_indexes_with_match_len(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_lcs_indexes_with_match_len_and_minimum_match_len(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_setrange_with_huge_offset(s: redis.Redis):
    assert False


@pytest.mark.xfail("not implemented")
def test_append_modifies_the_encoding_from_int_to_raw(s: redis.Redis):
    assert False
