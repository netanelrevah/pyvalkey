import time
from random import randint

import pytest
import redis

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
