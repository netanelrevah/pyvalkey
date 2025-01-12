import time
from random import randint

import pytest
import valkey

from tests.utils import assert_raises, bits_to_bytes

pytestmark = pytest.mark.string


def test_set_and_get_an_item(s: valkey.Valkey):
    s.set("x", "foobar")
    assert s.get("x") == b"foobar"


def test_set_and_get_an_empty_item(s: valkey.Valkey):
    s.set("x", "")
    assert s.get("x") == b""


def test_very_big_payload_in_get_or_set(s: valkey.Valkey):
    buffer = "abcd" * 1000000

    s.set("foo", buffer)
    assert s.get("foo") == buffer.encode()


@pytest.mark.slow
def test_very_big_payload_random_access(s: valkey.Valkey):
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
def test_set_10000_numeric_keys_and_access_all_them_in_reverse_order(s: valkey.Valkey):
    for number in range(10000):
        s.set(str(number), number)

    for number in range(9999, -1):
        value = s.get(str(number))
        assert int(value) == number

    assert s.dbsize() == 10000


def test_setnx_target_key_missing(s: valkey.Valkey):
    s.delete("novar")
    assert s.setnx("novar", "foobared") is True
    assert s.get("novar") == b"foobared"


def test_setnx_target_key_exists(s: valkey.Valkey):
    s.set("novar", "foobared")
    assert s.setnx("novar", "blabla") is False
    assert s.get("novar") == b"foobared"


def test_setnx_against_not_expired_volatile_key(s: valkey.Valkey):
    s.set("x", 10)
    s.expire("x", 10000)
    assert s.setnx("x", 20) is False
    assert s.get("x") == b"10"


def test_setnx_against_expired_volatile_key(s: valkey.Valkey):
    for index in range(9999):
        s.setex(f"key-{index}", 3600, "value")

    s.set("x", 10)
    s.expire("x", 1)

    time.sleep(2)

    assert s.setnx("x", 20) is True
    assert s.get("x") == b"20"


def test_getex_ex_option(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", ex=10)
    assert 5 <= s.ttl("foo") <= 10


def test_getex_px_option(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", px=10000)
    assert 5000 <= s.pttl("foo") <= 10000


def test_getex_exat_option(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", exat=int(time.time() + 10))
    assert 5 <= s.ttl("foo") <= 10


def test_getex_pxat_option(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo", pxat=int(time.time() * 1000 + 10000))
    assert 5000 <= s.pttl("foo") <= 10000


def test_getex_persist_option(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar", ex=10)
    s.getex("foo", persist=True)
    assert s.ttl("foo") == -1


def test_getex_no_option(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar")
    s.getex("foo")
    assert s.getex("foo") == b"bar"


def test_getex_syntax_errors(s: valkey.Valkey):
    with assert_raises(valkey.ValkeyError, "syntax error"):
        s.execute_command("getex", "foo", "non-existent-option")


def test_getex_and_get_expired_key_or_not_exist(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar", px=1)
    time.sleep(0.002)
    assert s.getex("foo") is None
    assert s.get("foo") is None


def test_getex_no_arguments(s: valkey.Valkey):
    with assert_raises(valkey.ValkeyError, "wrong number of arguments for 'getex' command"):
        s.execute_command("getex")


def test_getdel_command(s: valkey.Valkey):
    s.delete("foo")
    s.set("foo", "bar")
    assert s.getdel("foo") == b"bar"
    assert s.getdel("foo") is None


def test_mget(s: valkey.Valkey):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    assert s.mget("foo", "bar") == [b"BAR", b"FOO"]


def test_mget_against_non_existing_key(s: valkey.Valkey):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    assert s.mget("foo", "baazz", "bar") == [b"BAR", None, b"FOO"]


def test_mget_against_non_string_key(s: valkey.Valkey):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    s.sadd("myset", "ciao")
    s.sadd("myset", "bau")
    assert s.mget("foo", "baazz", "bar", "myset") == [b"BAR", None, b"FOO", None]


def test_getset_set_new_value(s: valkey.Valkey):
    assert s.getset("foo", "xyz") == b""
    assert s.get("foo") == b"xyz"


def test_getset_replace_old_value(s: valkey.Valkey):
    s.set("foo", "bar")
    assert s.getset("foo", "xyz") == b"bar"
    assert s.get("foo") == b"xyz"


def test_mset_base_case(s: valkey.Valkey):
    s.mset({"x": 10, "y": "foo bar", "z": "x x x x x x x\n\n\r\n"})
    assert s.mget("x", "y", "z") == [b"10", b"foo bar", b"x x x x x x x\n\n\r\n"]


def test_mset_or_msetnx_wrong_number_of_args(s: valkey.Valkey):
    with assert_raises(valkey.ValkeyError, "wrong number of arguments for 'mset' command"):
        s.execute_command("mset", "x", 10, "y", "foo bar", "z")
    with assert_raises(valkey.ValkeyError, "wrong number of arguments for 'msetnx' command"):
        s.execute_command("msetnx", "x", 20, "y", "foo bar", "z")


def test_mset_with_already_existing_same_key_twice(s: valkey.Valkey):
    s.set("x", "x")
    s.execute_command("mset", "x", "xxx", "x", "yyy")
    assert s.get("x") == b"yyy"


def test_msetnx_with_already_existent_key(s: valkey.Valkey):
    s.set("x", "x")
    assert s.msetnx({"x1": "xxx", "y2": "yyy", "x": 20}) is False
    assert s.exists("x1") == 0
    assert s.exists("y2") == 0


def test_msetnx_with_not_existing_keys(s: valkey.Valkey):
    assert s.msetnx({"x1": "xxx", "y2": "yyy"}) is True
    assert s.get("x1") == b"xxx"
    assert s.get("y2") == b"yyy"


def test_msetnx_with_not_existing_keys_same_key_twice(s: valkey.Valkey):
    assert s.execute_command("msetnx", "x1", "xxx", "x1", "yyy") is True
    assert s.get("x1") == b"yyy"


def test_msetnx_with_already_existing_keys_same_key_twice(s: valkey.Valkey):
    assert s.set("x1", b"yyy")
    assert s.execute_command("msetnx", "x1", "xxx", "x1", "zzz") is False
    assert s.get("x1") == b"yyy"


def test_strlen_against_non_existing_key(s: valkey.Valkey):
    assert s.strlen("notakey") == 0


def test_strlen_against_integer_encoded_value(s: valkey.Valkey):
    s.set("myinteger", -555)
    assert s.strlen("myinteger") == 4


def test_strlen_against_plain_string(s: valkey.Valkey):
    s.set("mystring", "foozzz0123456789 baz")
    assert s.strlen("mystring") == 20


def test_setbit_against_non_existing_key(s: valkey.Valkey):
    assert s.setbit("mykey", 1, 1) == 0
    assert s.get("mykey") == bits_to_bytes("01000000")


def test_setbit_against_string_encoded_key(s: valkey.Valkey):
    s.set("mykey", "@")
    assert s.setbit("mykey", 2, 1) == 0
    assert s.get("mykey") == bits_to_bytes("01100000")
    assert s.setbit("mykey", 1, 0) == 1
    assert s.get("mykey") == bits_to_bytes("00100000")


def test_setbit_against_key_with_wrong_type(s: valkey.Valkey):
    s.lpush("mykey", "foo")
    with assert_raises(valkey.ValkeyError, "WRONGTYPE Operation against a key holding the wrong kind of value"):
        s.setbit("mykey", 0, 1)


def test_setbit_with_out_of_range_bit_offset(s: valkey.Valkey):
    with assert_raises(valkey.ValkeyError, "bit offset is not an integer or out of range"):
        s.setbit("mykey", 4 * (1024**3), 1)
    with assert_raises(valkey.ValkeyError, "bit offset is not an integer or out of range"):
        s.setbit("mykey", -1, 1)


def test_setbit_with_non_bit_argument(s: valkey.Valkey):
    with assert_raises(valkey.ValkeyError, "bit is not an integer or out of range"):
        s.execute_command("setbit", "mykey", 0, -1)
    with assert_raises(valkey.ValkeyError, "bit is not an integer or out of range"):
        s.execute_command("setbit", "mykey", 0, 2)
    with assert_raises(valkey.ValkeyError, "bit is not an integer or out of range"):
        s.execute_command("setbit", "mykey", 0, 10)
    with assert_raises(valkey.ValkeyError, "bit is not an integer or out of range"):
        s.execute_command("setbit", "mykey", 0, 20)


def test_setbit_fuzzing(s: valkey.Valkey):
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

        s.setbit("mykey", bit_number, bit_value)
        actual = s.get("mykey")
        assert actual == bits_to_bytes(expected)


def test_getbit_against_non_existing_key(s: valkey.Valkey):
    assert s.getbit("mykey", 0) == 0


def test_getbit_against_string_encoded_key(s: valkey.Valkey):
    s.set("mykey", "`")

    assert s.getbit("mykey", 0) == 0
    assert s.getbit("mykey", 1) == 1
    assert s.getbit("mykey", 2) == 1
    assert s.getbit("mykey", 3) == 0

    assert s.getbit("mykey", 8) == 0
    assert s.getbit("mykey", 100) == 0
    assert s.getbit("mykey", 10000) == 0


def test_setrange_against_non_existing_key(s: valkey.Valkey):
    s.delete("mykey")
    assert s.setrange("mykey", 0, "foo") == 3
    assert s.get("mykey") == b"foo"

    s.delete("mykey")
    assert s.setrange("mykey", 0, "") == 0
    assert s.exists("mykey") == 0

    s.delete("mykey")
    assert s.setrange("mykey", 1, "foo") == 4
    assert s.get("mykey") == b"\x00foo"


def test_setrange_against_string_encoded_key(s: valkey.Valkey):
    s.set("mykey", "foo")
    assert s.setrange("mykey", 0, "b") == 3
    assert s.get("mykey") == b"boo"

    s.set("mykey", "foo")
    assert s.setrange("mykey", 0, "") == 3
    assert s.get("mykey") == b"foo"

    s.set("mykey", "foo")
    assert s.setrange("mykey", 1, "b") == 3
    assert s.get("mykey") == b"fbo"

    s.set("mykey", "foo")
    assert s.setrange("mykey", 4, "bar") == 7
    assert s.get("mykey") == b"foo\x00bar"


def test_setrange_against_key_with_wrong_type(s: valkey.Valkey):
    assert s.lpush("mykey", "foo")
    with assert_raises(valkey.ValkeyError, "WRONGTYPE Operation against a key holding the wrong kind of value"):
        s.setrange("mykey", 0, "bar")


def test_setrange_with_out_of_range_offset(s: valkey.Valkey):
    with assert_raises(valkey.ValkeyError, "string exceeds maximum allowed size (proto-max-bulk-len)"):
        s.setrange("mykey", 512 * 1024 * 1024 - 4, "world")

    s.set("mykey", "hello")
    with assert_raises(valkey.ValkeyError, "value is not an integer or out of range"):
        s.setrange("mykey", -1, "world")

    with assert_raises(valkey.ValkeyError, "string exceeds maximum allowed size (proto-max-bulk-len)"):
        s.setrange("mykey", 512 * 1024 * 1024 - 4, "world")
