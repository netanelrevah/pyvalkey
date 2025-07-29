from pyvalkey.commands.sorted_set_commands import SortedSetIntersectionCardinality, SortedSetUnion, SortedSetUnionStore


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
