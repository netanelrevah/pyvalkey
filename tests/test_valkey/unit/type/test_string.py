import time
from random import randint

import pytest

from tests.utils import assert_raises, bits_to_bytes
from tests.valkey_test_client import ValkeyError, ValkeyTestClient

pytestmark = pytest.mark.string


def test_set_and_get_an_item(r: ValkeyTestClient):
    r.run("set", "x", "foobar")
    assert r.run("get", "x") == b"foobar"


def test_set_and_get_an_empty_item(r: ValkeyTestClient):
    r.run("set", "x", "")
    assert r.run("get", "x") is None


def test_very_big_payload_in_get_or_set(r: ValkeyTestClient):
    buffer = "abcd" * 1000000

    r.run("set", "foo", buffer)
    assert r.run("get", "foo") == buffer.encode()


@pytest.mark.slow
def test_very_big_payload_random_access(r: ValkeyTestClient):
    payload = []
    for index in range(100):
        size = 1 + randint(0, 100000)
        buffer = f"pl-{index}" * size
        payload.append(buffer)
        r.run("set", f"bigpayload_{index}", buffer)

    for _ in range(1000):
        index = randint(0, 99)
        buffer = r.run("get", f"bigpayload_{index}").decode()
        assert buffer == payload[index]


@pytest.mark.slow
def test_set_10000_numeric_keys_and_access_all_them_in_reverse_order(r: ValkeyTestClient):
    for number in range(10000):
        r.run("set", str(number), number)

    for number in range(9999, -1):
        value = r.run("get", str(number))
        assert int(value) == number

    assert r.run("dbsize") == 10000


def test_setnx_target_key_missing(r: ValkeyTestClient):
    r.run("del", "novar")
    assert r.run("setnx", "novar", "foobared") == 1
    assert r.run("get", "novar") == b"foobared"


def test_setnx_target_key_exists(r: ValkeyTestClient):
    r.run("set", "novar", "foobared")
    assert r.run("setnx", "novar", "blabla") == 0
    assert r.run("get", "novar") == b"foobared"


def test_setnx_against_not_expired_volatile_key(r: ValkeyTestClient):
    r.run("set", "x", 10)
    r.run("expire", "x", 10000)
    assert r.run("setnx", "x", 20) == 0
    assert r.run("get", "x") == b"10"


def test_setnx_against_expired_volatile_key(r: ValkeyTestClient):
    for index in range(9999):
        r.run("setex", f"key-{index}", 3600, "value")

    r.run("set", "x", 10)
    r.run("expire", "x", 1)

    time.sleep(2)

    assert r.run("setnx", "x", 20) == 1
    assert r.run("get", "x") == b"20"


def test_getex_ex_option(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar")
    r.run("getex", "foo", "ex", 10)
    assert 5 <= r.run("ttl", "foo") <= 10


def test_getex_px_option(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar")
    r.run("getex", "foo", "px", 10000)
    assert 5000 <= r.run("pttl", "foo") <= 10000


def test_getex_exat_option(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar")
    r.run("getex", "foo", "exat", int(time.time() + 10))
    assert 5 <= r.run("ttl", "foo") <= 10


def test_getex_pxat_option(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar")
    r.run("getex", "foo", "pxat", int(time.time() * 1000 + 10000))
    assert 5000 <= r.run("pttl", "foo") <= 10000


def test_getex_persist_option(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar", "ex", 10)
    r.run("getex", "foo", "persist")
    assert r.run("ttl", "foo") == -1


def test_getex_no_option(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar")
    r.run("getex", "foo")
    assert r.run("getex", "foo") == b"bar"


def test_getex_syntax_errors(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "syntax error"):
        r.run("getex", "foo", "non-existent-option")


def test_getex_and_get_expired_key_or_not_exist(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar", "px", 1)
    time.sleep(0.002)
    assert r.run("getex", "foo") is None
    assert r.run("get", "foo") is None


def test_getex_no_arguments(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "wrong number of arguments for 'getex' command"):
        r.run("getex")


def test_getdel_command(r: ValkeyTestClient):
    r.run("del", "foo")
    r.run("set", "foo", "bar")
    assert r.run("getdel", "foo") == b"bar"
    assert r.run("getdel", "foo") is None


def test_mget(r: ValkeyTestClient):
    r.run("set", "foo", "BAR")
    r.run("set", "bar", "FOO")
    assert r.run("mget", "foo", "bar") == [b"BAR", b"FOO"]


def test_mget_against_non_existing_key(r: ValkeyTestClient):
    r.run("set", "foo", "BAR")
    r.run("set", "bar", "FOO")
    assert r.run("mget", "foo", "baazz", "bar") == [b"BAR", None, b"FOO"]


def test_mget_against_non_string_key(r: ValkeyTestClient):
    r.run("set", "foo", "BAR")
    r.run("set", "bar", "FOO")
    r.run("sadd", "myset", "ciao")
    r.run("sadd", "myset", "bau")
    assert r.run("mget", "foo", "baazz", "bar", "myset") == [b"BAR", None, b"FOO", None]


def test_getset_set_new_value(r: ValkeyTestClient):
    assert r.run("getset", "foo", "xyz") == b""
    assert r.run("get", "foo") == b"xyz"


def test_getset_replace_old_value(r: ValkeyTestClient):
    r.run("set", "foo", "bar")
    assert r.run("getset", "foo", "xyz") == b"bar"
    assert r.run("get", "foo") == b"xyz"


def test_mset_base_case(r: ValkeyTestClient):
    r.run("mset", "x", 10, "y", "foo bar", "z", "x x x x x x x\n\n\r\n")
    assert r.run("mget", "x", "y", "z") == [b"10", b"foo bar", b"x x x x x x x\n\n\r\n"]


def test_mset_or_msetnx_wrong_number_of_args(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "wrong number of arguments for 'mset' command"):
        r.run("mset", "x", 10, "y", "foo bar", "z")
    with assert_raises(ValkeyError, "wrong number of arguments for 'msetnx' command"):
        r.run("msetnx", "x", 20, "y", "foo bar", "z")


def test_mset_with_already_existing_same_key_twice(r: ValkeyTestClient):
    r.run("set", "x", "x")
    r.run("mset", "x", "xxx", "x", "yyy")
    assert r.run("get", "x") == b"yyy"


def test_msetnx_with_already_existent_key(r: ValkeyTestClient):
    r.run("set", "x", "x")
    assert r.run("msetnx", "x1", "xxx", "y2", "yyy", "x", 20) == 0
    assert r.run("exists", "x1") == 0
    assert r.run("exists", "y2") == 0


def test_msetnx_with_not_existing_keys(r: ValkeyTestClient):
    assert r.run("msetnx", "x1", "xxx", "y2", "yyy") == 1
    assert r.run("get", "x1") == b"xxx"
    assert r.run("get", "y2") == b"yyy"


def test_msetnx_with_not_existing_keys_same_key_twice(r: ValkeyTestClient):
    assert r.run("msetnx", "x1", "xxx", "x1", "yyy") == 1
    assert r.run("get", "x1") == b"yyy"


def test_msetnx_with_already_existing_keys_same_key_twice(r: ValkeyTestClient):
    assert r.run("set", "x1", b"yyy")
    assert r.run("msetnx", "x1", "xxx", "x1", "zzz") == 0
    assert r.run("get", "x1") == b"yyy"


def test_strlen_against_non_existing_key(r: ValkeyTestClient):
    assert r.run("strlen", "notakey") == 0


def test_strlen_against_integer_encoded_value(r: ValkeyTestClient):
    r.run("set", "myinteger", -555)
    assert r.run("strlen", "myinteger") == 4


def test_strlen_against_plain_string(r: ValkeyTestClient):
    r.run("set", "mystring", "foozzz0123456789 baz")
    assert r.run("strlen", "mystring") == 20


def test_setbit_against_non_existing_key(r: ValkeyTestClient):
    assert r.run("setbit", "mykey", 1, 1) == 0
    assert r.run("get", "mykey") == bits_to_bytes("01000000")


def test_setbit_against_string_encoded_key(r: ValkeyTestClient):
    r.run("set", "mykey", "@")
    assert r.run("setbit", "mykey", 2, 1) == 0
    assert r.run("get", "mykey") == bits_to_bytes("01100000")
    assert r.run("setbit", "mykey", 1, 0) == 1
    assert r.run("get", "mykey") == bits_to_bytes("00100000")


def test_setbit_against_key_with_wrong_type(r: ValkeyTestClient):
    r.run("lpush", "mykey", "foo")
    with assert_raises(ValkeyError, "WRONGTYPE Operation against a key holding the wrong kind of value"):
        r.run("setbit", "mykey", 0, 1)


def test_setbit_with_out_of_range_bit_offset(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "bit offset is not an integer or out of range"):
        r.run("setbit", "mykey", 4 * (1024**3), 1)
    with assert_raises(ValkeyError, "bit offset is not an integer or out of range"):
        r.run("setbit", "mykey", -1, 1)


def test_setbit_with_non_bit_argument(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "bit is not an integer or out of range"):
        r.run("setbit", "mykey", 0, -1)
    with assert_raises(ValkeyError, "bit is not an integer or out of range"):
        r.run("setbit", "mykey", 0, 2)
    with assert_raises(ValkeyError, "bit is not an integer or out of range"):
        r.run("setbit", "mykey", 0, 10)
    with assert_raises(ValkeyError, "bit is not an integer or out of range"):
        r.run("setbit", "mykey", 0, 20)


def test_setbit_fuzzing(r: ValkeyTestClient):
    length = 256 * 8
    expected = ""

    for index in range(0, 2000):
        bit_number = randint(0, length - 1)
        bit_value = randint(0, 1)
        if len(expected) < bit_number:
            expected += "0" * (bit_number - len(expected))
        head = expected[:bit_number]
        tail = expected[bit_number + 1 :]
        expected = f"{head}{bit_value}{tail}"

        r.run("setbit", "mykey", bit_number, bit_value)
        actual = r.run("get", "mykey")
        assert actual == bits_to_bytes(expected)


def test_getbit_against_non_existing_key(r: ValkeyTestClient):
    assert r.run("getbit", "mykey", 0) == 0


def test_getbit_against_string_encoded_key(r: ValkeyTestClient):
    r.run("set", "mykey", "`")

    assert r.run("getbit", "mykey", 0) == 0
    assert r.run("getbit", "mykey", 1) == 1
    assert r.run("getbit", "mykey", 2) == 1
    assert r.run("getbit", "mykey", 3) == 0

    assert r.run("getbit", "mykey", 8) == 0
    assert r.run("getbit", "mykey", 100) == 0
    assert r.run("getbit", "mykey", 10000) == 0


def test_setrange_against_non_existing_key(r: ValkeyTestClient):
    r.run("del", "mykey")
    assert r.run("setrange", "mykey", 0, "foo") == 3
    assert r.run("get", "mykey") == b"foo"

    r.run("del", "mykey")
    assert r.run("setrange", "mykey", 0, "") == 0
    assert r.run("exists", "mykey") == 0

    r.run("del", "mykey")
    assert r.run("setrange", "mykey", 1, "foo") == 4
    assert r.run("get", "mykey") == b"\x00foo"


def test_setrange_against_string_encoded_key(r: ValkeyTestClient):
    r.run("set", "mykey", "foo")
    assert r.run("setrange", "mykey", 0, "b") == 3
    assert r.run("get", "mykey") == b"boo"

    r.run("set", "mykey", "foo")
    assert r.run("setrange", "mykey", 0, "") == 3
    assert r.run("get", "mykey") == b"foo"

    r.run("set", "mykey", "foo")
    assert r.run("setrange", "mykey", 1, "b") == 3
    assert r.run("get", "mykey") == b"fbo"

    r.run("set", "mykey", "foo")
    assert r.run("setrange", "mykey", 4, "bar") == 7
    assert r.run("get", "mykey") == b"foo\x00bar"


def test_setrange_against_key_with_wrong_type(r: ValkeyTestClient):
    assert r.run("lpush", "mykey", "foo")
    with assert_raises(ValkeyError, "WRONGTYPE Operation against a key holding the wrong kind of value"):
        r.run("setrange", "mykey", 0, "bar")


def test_setrange_with_out_of_range_offset(r: ValkeyTestClient):
    with assert_raises(ValkeyError, "string exceeds maximum allowed size (proto-max-bulk-len)"):
        r.run("setrange", "mykey", 512 * 1024 * 1024 - 4, "world")

    r.run("set", "mykey", "hello")
    with assert_raises(ValkeyError, "value is not an integer or out of range"):
        r.run("setrange", "mykey", -1, "world")

    with assert_raises(ValkeyError, "string exceeds maximum allowed size (proto-max-bulk-len)"):
        r.run("setrange", "mykey", 512 * 1024 * 1024 - 4, "world")
