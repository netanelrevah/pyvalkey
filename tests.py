import redis
from pytest import fixture


@fixture
def c():
    return redis.Redis()


def test_simple(c):
    c.set("b", 1)
    assert c.get("b") == b"1"
    c.set("a", "bla")
    assert c.get("a") == b"bla"
    c.hset("c", "d", 2)
    assert c.hgetall("c") == {b"d": b"2"}
    c.delete("a", "b")
    assert c.keys() == [b"c"]
