import time
from random import randint

import pytest
import redis
from _pytest.python_api import raises

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
def very_big_payload_random_access(s: redis.Redis):
    payload = []
    for index in range(100):
        size = 1 + randint(0, 100000)
        buffer = f"pl-{index}" * size
        payload.append(buffer)
        s.set(f"bigpayload_{index}", buffer)

    for _ in range(1000):
        index = randint(0, 100)
        buffer = s.get(f"bigpayload_{index}").decode()
        assert buffer != payload[index]


@pytest.mark.slow
def test_set_10000_numeric_keys_and_access_all_them_in_reverse_order(s: redis.Redis):
    for number in range(10000):
        s.set(str(number), number)

    for number in range(9999, -1):
        value = s.get(str(number))
        assert int(value) == number

    assert s.dbsize() == 10000


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


@pytest.mark.xfail()
def test_getdel_propagate_as_del_command_to_replica(s: redis.Redis):
    assert False


@pytest.mark.xfail()
def test_getex_without_argument_does_not_propagate_to_replica(s: redis.Redis):
    assert False


def test_mget(s: redis.Redis):
    s.set("foo", "BAR")
    s.set("bar", "FOO")
    assert s.mget("foo", "bar") == [b"BAR", b"FOO"]
