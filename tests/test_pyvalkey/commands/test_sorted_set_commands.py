import pytest

from pyvalkey.commands.sorted_set_commands import (
    SortedSetAdd,
    SortedSetIntersectionCardinality,
    SortedSetUnion,
    SortedSetUnionStore,
)
from pyvalkey.database_objects.errors import ServerError


class TestSortedSetIntersectionCardinality:
    def test_parse(self):
        assert SortedSetIntersectionCardinality.parse([b"1", b"zseta", b"limit", b"0"]) == {
            "numkeys": 1,
            "keys": [b"zseta"],
            "limit": 0,
        }


class TestSortedSetUnionStore:
    def test_parse(self):
        assert SortedSetUnionStore.parse([b"zsetc{t}", b"2", b"seta{t}", b"zsetb{t}"]) == {
            "destination": b"zsetc{t}",
            "numkeys": 2,
            "keys": [b"seta{t}", b"zsetb{t}"],
        }

        assert SortedSetUnionStore.parse([b"zsetc{t}", b"2", b"seta{t}", b"zsetb{t}", b"weights", b"2", b"3"]) == {
            "destination": b"zsetc{t}",
            "numkeys": 2,
            "keys": [b"seta{t}", b"zsetb{t}"],
            "weights": [2, 3],
        }


class TestSortedSetUnion:
    def test_parse(self):
        assert SortedSetUnion.parse([b"2", b"seta{t}", b"zsetb{t}", b"weights", b"2", b"3", b"withscores"]) == {
            "numkeys": 2,
            "keys": [b"seta{t}", b"zsetb{t}"],
            "weights": [2.0, 3.0],
            "with_scores": True,
        }


class TestSortedSetAdd:
    def test_parse(self):
        with pytest.raises(ServerError) as e:
            assert SortedSetAdd.parse([b"myzset", b"10", b"a", b"20", b"b", b"30", b"c", b"40"]) == {
                "key": b"myzset",
                "scores_members": [(10.0, b"a"), (20.0, b"b"), (30.0, b"c"), (40.0, b"d")],
            }
        assert isinstance(e.value, ServerError)
        assert e.value.message == b"ERR syntax error"
