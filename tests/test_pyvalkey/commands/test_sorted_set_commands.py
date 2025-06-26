from pyvalkey.commands.sorted_set_commands import SortedSetUnion, SortedSetUnionStore


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
            "weights": [2, 3],
            "withscores": True,
        }
