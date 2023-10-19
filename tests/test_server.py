from threading import Thread

import redis
from _pytest.fixtures import yield_fixture
from parametrization import Parametrization
from pytest import fixture

from r3dis.commands.utils import parse_range_parameters
from r3dis.databases import MAX_STRING
from r3dis.server import RedisServer


@yield_fixture
def s():
    server = RedisServer(("127.0.0.1", 6379))
    t = Thread(target=server.serve_forever)
    t.start()
    yield t
    server.shutdown()


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


def test_redis_max_str():
    assert "a" < MAX_STRING
    assert "a" <= MAX_STRING
    assert not "\xff" > MAX_STRING
    assert not "\xff" >= MAX_STRING
    assert "a" != MAX_STRING

    assert MAX_STRING > "a"
    assert MAX_STRING >= "a"
    assert not MAX_STRING < "\xff"
    assert not MAX_STRING <= "\xff"
    assert MAX_STRING != "a"


N0 = []
N1 = [8]
N3 = [7, 2, 5]
N10 = [7, 2, 5, 8, 4, 9, 1, 3, 10, 6]


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="n3_first_two_items",
    redis_start=0,
    redis_stop=1,
    actual_list=N3,
    expected_slice=slice(0, 2),
    expected_list=[7, 2],
    expected_reversed_slice=slice(-1, -3, -1),
    expected_reversed_list=[5, 2],
)
@Parametrization.case(
    name="out_of_range_of_empty",
    redis_start=0,
    redis_stop=0,
    actual_list=N0,
    expected_slice=slice(0, 1),
    expected_list=[],
    expected_reversed_slice=slice(-1, -2, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="out_of_range_of_empty_by_tail",
    redis_start=-1,
    redis_stop=-1,
    actual_list=N0,
    expected_slice=slice(-1, None, None),
    expected_list=[],
    expected_reversed_slice=slice(0, None, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="out_of_range",
    redis_start=1,
    redis_stop=1,
    actual_list=N1,
    expected_slice=slice(1, 2),
    expected_list=[],
    expected_reversed_slice=slice(-2, -3, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="out_of_range_by_tail",
    redis_start=-2,
    redis_stop=-2,
    actual_list=N1,
    expected_slice=slice(-2, -1),
    expected_list=[],
    expected_reversed_slice=slice(1, 0, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="one_item",
    redis_start=0,
    redis_stop=0,
    actual_list=N1,
    expected_slice=slice(0, 1),
    expected_list=N1,
    expected_reversed_slice=slice(-1, -2, -1),
    expected_reversed_list=N1,
)
@Parametrization.case(
    name="one_item_by_tail",
    redis_start=-1,
    redis_stop=-1,
    actual_list=N1,
    expected_slice=slice(-1, None),
    expected_list=N1,
    expected_reversed_slice=slice(0, None, -1),
    expected_reversed_list=N1,
)
@Parametrization.case(
    name="first_item",
    redis_start=0,
    redis_stop=0,
    actual_list=N10,
    expected_slice=slice(0, 1),
    expected_list=[7],
    expected_reversed_slice=slice(-1, -2, -1),
    expected_reversed_list=[6],
)
@Parametrization.case(
    name="first_two_items",
    redis_start=0,
    redis_stop=1,
    actual_list=N10,
    expected_slice=slice(0, 2),
    expected_list=[7, 2],
    expected_reversed_slice=slice(-1, -3, -1),
    expected_reversed_list=[6, 10],
)
@Parametrization.case(
    name="last_item",
    redis_start=9,
    redis_stop=9,
    actual_list=N10,
    expected_slice=slice(9, 10),
    expected_list=[6],
    expected_reversed_slice=slice(-10, -11, -1),
    expected_reversed_list=[7],
)
@Parametrization.case(
    name="last_two_items",
    redis_start=8,
    redis_stop=9,
    actual_list=N10,
    expected_slice=slice(8, 10),
    expected_list=[10, 6],
    expected_reversed_slice=slice(-9, -11, -1),
    expected_reversed_list=[2, 7],
)
@Parametrization.case(
    name="last_item_by_tail",
    redis_start=-1,
    redis_stop=-1,
    actual_list=N10,
    expected_slice=slice(-1, None),
    expected_list=[6],
    expected_reversed_slice=slice(0, None, -1),
    expected_reversed_list=[7],
)
@Parametrization.case(
    name="last_two_items_by_tail",
    redis_start=-2,
    redis_stop=-1,
    actual_list=N10,
    expected_slice=slice(-2, None),
    expected_list=[10, 6],
    expected_reversed_slice=slice(1, None, -1),
    expected_reversed_list=[2, 7],
)
@Parametrization.case(
    name="first_item_by_tail",
    redis_start=-10,
    redis_stop=-10,
    actual_list=N10,
    expected_slice=slice(-10, -9),
    expected_list=[7],
    expected_reversed_slice=slice(9, 8, -1),
    expected_reversed_list=[6],
)
@Parametrization.case(
    name="first_two_items_by_tail",
    redis_start=-10,
    redis_stop=-9,
    actual_list=N10,
    expected_slice=slice(-10, -8),
    expected_list=[7, 2],
    expected_reversed_slice=slice(9, 7, -1),
    expected_reversed_list=[6, 10],
)
@Parametrization.case(
    name="all_items",
    redis_start=0,
    redis_stop=9,
    actual_list=N10,
    expected_slice=slice(0, 10),
    expected_list=N10,
    expected_reversed_slice=slice(-1, -11, -1),
    expected_reversed_list=list(reversed(N10)),
)
@Parametrization.case(
    name="all_items_by_tail",
    redis_start=-10,
    redis_stop=-1,
    actual_list=N10,
    expected_slice=slice(-10, None),
    expected_list=N10,
    expected_reversed_slice=slice(9, None, -1),
    expected_reversed_list=list(reversed(N10)),
)
@Parametrization.case(
    name="middle_items",
    redis_start=4,
    redis_stop=5,
    actual_list=N10,
    expected_slice=slice(4, 6),
    expected_list=[4, 9],
    expected_reversed_slice=slice(-5, -7, -1),
    expected_reversed_list=[9, 4],
)
@Parametrization.case(
    name="middle_items_by_tail",
    redis_start=-6,
    redis_stop=-5,
    actual_list=N10,
    expected_slice=slice(-6, -4),
    expected_list=[4, 9],
    expected_reversed_slice=slice(5, 3, -1),
    expected_reversed_list=[9, 4],
)
def test_redis_parse_range_parameters(
    redis_start,
    redis_stop,
    actual_list,
    expected_slice,
    expected_list,
    expected_reversed_slice,
    expected_reversed_list,
):
    actual_slice = parse_range_parameters(redis_start, redis_stop)
    assert actual_slice == expected_slice
    assert actual_list[actual_slice] == expected_list

    actual_reversed_slice = parse_range_parameters(redis_start, redis_stop, is_reversed=True)
    assert actual_reversed_slice == expected_reversed_slice
    assert actual_list[actual_reversed_slice] == expected_reversed_list
