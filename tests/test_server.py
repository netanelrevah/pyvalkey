from threading import Thread

import redis
from _pytest.fixtures import yield_fixture
from pytest import fixture

from r3dis import RedisServer


@yield_fixture
def s():
    server = RedisServer(("127.0.0.1", 6379))
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield t


@fixture
def c():
    return redis.Redis()


def test_simple(s, c):
    c.set("b", 1)
    assert c.get("b") == b"1"
    c.set("a", "bla")
    assert c.get("a") == b"bla"
    c.hset("c", "d", 2)
    assert c.hgetall("c") == {b"d": b"2"}
    c.delete("a", "b")
    assert c.keys() == [b"c"]
    c.set("f", "abc")
    c.append("f", "abc")
    assert c.get("f") == b"abcabc"
