from threading import Thread

import redis
from parametrization import Parametrization
from pytest import fixture

from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.database_objects.databases import MAX_BYTES
from pyvalkey.server import ValkeyServer


@fixture
def s():
    server = ValkeyServer(("127.0.0.1", 6379))
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


def test_server_max_str():
    assert "a" < MAX_BYTES
    assert "a" <= MAX_BYTES
    assert not "\xff" > MAX_BYTES
    assert not "\xff" >= MAX_BYTES
    assert "a" != MAX_BYTES

    assert MAX_BYTES > "a"
    assert MAX_BYTES >= "a"
    assert not MAX_BYTES < "\xff"
    assert not MAX_BYTES <= "\xff"
    assert MAX_BYTES != "a"


N0 = []
N1 = [8]
N3 = [7, 2, 5]
N10 = [7, 2, 5, 8, 4, 9, 1, 3, 10, 6]


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="n3_first_two_items",
    server_start=0,
    server_stop=1,
    actual_list=N3,
    expected_slice=slice(0, 2),
    expected_list=[7, 2],
    expected_reversed_slice=slice(-1, -3, -1),
    expected_reversed_list=[5, 2],
)
@Parametrization.case(
    name="out_of_range_of_empty",
    server_start=0,
    server_stop=0,
    actual_list=N0,
    expected_slice=slice(0, 1),
    expected_list=[],
    expected_reversed_slice=slice(-1, -2, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="out_of_range_of_empty_by_tail",
    server_start=-1,
    server_stop=-1,
    actual_list=N0,
    expected_slice=slice(-1, None, None),
    expected_list=[],
    expected_reversed_slice=slice(0, None, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="out_of_range",
    server_start=1,
    server_stop=1,
    actual_list=N1,
    expected_slice=slice(1, 2),
    expected_list=[],
    expected_reversed_slice=slice(-2, -3, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="out_of_range_by_tail",
    server_start=-2,
    server_stop=-2,
    actual_list=N1,
    expected_slice=slice(-2, -1),
    expected_list=[],
    expected_reversed_slice=slice(1, 0, -1),
    expected_reversed_list=[],
)
@Parametrization.case(
    name="one_item",
    server_start=0,
    server_stop=0,
    actual_list=N1,
    expected_slice=slice(0, 1),
    expected_list=N1,
    expected_reversed_slice=slice(-1, -2, -1),
    expected_reversed_list=N1,
)
@Parametrization.case(
    name="one_item_by_tail",
    server_start=-1,
    server_stop=-1,
    actual_list=N1,
    expected_slice=slice(-1, None),
    expected_list=N1,
    expected_reversed_slice=slice(0, None, -1),
    expected_reversed_list=N1,
)
@Parametrization.case(
    name="first_item",
    server_start=0,
    server_stop=0,
    actual_list=N10,
    expected_slice=slice(0, 1),
    expected_list=[7],
    expected_reversed_slice=slice(-1, -2, -1),
    expected_reversed_list=[6],
)
@Parametrization.case(
    name="first_two_items",
    server_start=0,
    server_stop=1,
    actual_list=N10,
    expected_slice=slice(0, 2),
    expected_list=[7, 2],
    expected_reversed_slice=slice(-1, -3, -1),
    expected_reversed_list=[6, 10],
)
@Parametrization.case(
    name="last_item",
    server_start=9,
    server_stop=9,
    actual_list=N10,
    expected_slice=slice(9, 10),
    expected_list=[6],
    expected_reversed_slice=slice(-10, -11, -1),
    expected_reversed_list=[7],
)
@Parametrization.case(
    name="last_two_items",
    server_start=8,
    server_stop=9,
    actual_list=N10,
    expected_slice=slice(8, 10),
    expected_list=[10, 6],
    expected_reversed_slice=slice(-9, -11, -1),
    expected_reversed_list=[2, 7],
)
@Parametrization.case(
    name="last_item_by_tail",
    server_start=-1,
    server_stop=-1,
    actual_list=N10,
    expected_slice=slice(-1, None),
    expected_list=[6],
    expected_reversed_slice=slice(0, None, -1),
    expected_reversed_list=[7],
)
@Parametrization.case(
    name="last_two_items_by_tail",
    server_start=-2,
    server_stop=-1,
    actual_list=N10,
    expected_slice=slice(-2, None),
    expected_list=[10, 6],
    expected_reversed_slice=slice(1, None, -1),
    expected_reversed_list=[2, 7],
)
@Parametrization.case(
    name="first_item_by_tail",
    server_start=-10,
    server_stop=-10,
    actual_list=N10,
    expected_slice=slice(-10, -9),
    expected_list=[7],
    expected_reversed_slice=slice(9, 8, -1),
    expected_reversed_list=[6],
)
@Parametrization.case(
    name="first_two_items_by_tail",
    server_start=-10,
    server_stop=-9,
    actual_list=N10,
    expected_slice=slice(-10, -8),
    expected_list=[7, 2],
    expected_reversed_slice=slice(9, 7, -1),
    expected_reversed_list=[6, 10],
)
@Parametrization.case(
    name="all_items",
    server_start=0,
    server_stop=9,
    actual_list=N10,
    expected_slice=slice(0, 10),
    expected_list=N10,
    expected_reversed_slice=slice(-1, -11, -1),
    expected_reversed_list=list(reversed(N10)),
)
@Parametrization.case(
    name="all_items_by_tail",
    server_start=-10,
    server_stop=-1,
    actual_list=N10,
    expected_slice=slice(-10, None),
    expected_list=N10,
    expected_reversed_slice=slice(9, None, -1),
    expected_reversed_list=list(reversed(N10)),
)
@Parametrization.case(
    name="middle_items",
    server_start=4,
    server_stop=5,
    actual_list=N10,
    expected_slice=slice(4, 6),
    expected_list=[4, 9],
    expected_reversed_slice=slice(-5, -7, -1),
    expected_reversed_list=[9, 4],
)
@Parametrization.case(
    name="middle_items_by_tail",
    server_start=-6,
    server_stop=-5,
    actual_list=N10,
    expected_slice=slice(-6, -4),
    expected_list=[4, 9],
    expected_reversed_slice=slice(5, 3, -1),
    expected_reversed_list=[9, 4],
)
def test_server_parse_range_parameters(
    server_start,
    server_stop,
    actual_list,
    expected_slice,
    expected_list,
    expected_reversed_slice,
    expected_reversed_list,
):
    actual_slice = parse_range_parameters(server_start, server_stop)
    assert actual_slice == expected_slice
    assert actual_list[actual_slice] == expected_list

    actual_reversed_slice = parse_range_parameters(server_start, server_stop, is_reversed=True)
    assert actual_reversed_slice == expected_reversed_slice
    assert actual_list[actual_reversed_slice] == expected_reversed_list
