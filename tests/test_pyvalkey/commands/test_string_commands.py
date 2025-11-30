from unittest.mock import Mock

from pytest import raises

from pyvalkey.commands.list_commands import ListBlockingLeftPop
from pyvalkey.commands.string_commands import ExistenceMode, Get, GetExpire, LongestCommonSubsequence, Set
from pyvalkey.database_objects.databases import Database, DatabaseContent, KeyValue
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.utils.times import now_ms


class TestListBlockingLeftPop:
    def test_parse(self):
        assert ListBlockingLeftPop.parse([b"abc", b"1"]) == {"keys": [b"abc"], "timeout": 1}


class TestSet:
    def test_parse(self):
        assert Set.parse([b"k", b"v"]) == {
            "key": b"k",
            "value": b"v",
        }
        assert Set.parse([b"k", b"v", b"nx"]) == {
            "key": b"k",
            "value": b"v",
            "existence_mode": ExistenceMode.OnlyIfNotExist,
        }
        assert Set.parse([b"k", b"v", b"xx"]) == {
            "key": b"k",
            "value": b"v",
            "existence_mode": ExistenceMode.OnlyIfExist,
        }
        with raises(ServerError, match="ERR syntax error"):
            Set.parse([b"k", b"v", b"nx", b"xx"])

        assert Set.parse([b"k", b"v", b"ex", b"10"]) == {
            "key": b"k",
            "value": b"v",
            "ex": 10,
        }
        assert Set.parse([b"k", b"v", b"GET"]) == {
            "key": b"k",
            "value": b"v",
            "get": True,
        }
        assert Set.parse([b"k", b"v", b"GET", b"NX"]) == {
            "key": b"k",
            "value": b"v",
            "existence_mode": ExistenceMode.OnlyIfNotExist,
            "get": True,
        }

    def test_execute(self):
        blocking_manager_mock = Mock()

        ###

        database = Database(0)

        assert Set(database, blocking_manager_mock, b"0", b"0").execute() == b"OK"

        assert database.content.data[b"0"] == KeyValue(b"0", 0)

        ###

        database = Database(0, DatabaseContent({b"foo": KeyValue(b"foo", b"initial_value")}))

        assert Set(database, blocking_manager_mock, b"foo", b"new_value", condition=b"initial_value").execute() == b"OK"

        assert database.content.data[b"foo"] == KeyValue(b"foo", b"new_value")

        ###

        database = Database(0)

        assert (
            Set(database, blocking_manager_mock, b"foo", b"new_value", condition=b"initial_value", get=True).execute()
            is None
        )

        assert database.content.data == {}

        ###

        database = Database(0, DatabaseContent({b"foo": KeyValue(b"foo", b"initial_value")}))

        assert (
            Set(database, blocking_manager_mock, b"foo", b"new_value", condition=b"initial_value", get=True).execute()
            == b"initial_value"
        )

        assert database.content.data[b"foo"].value == KeyValue(b"foo", b"new_value").value
        assert database.content.data[b"foo"].key == KeyValue(b"foo", b"new_value").key


class TestGet:
    def test_execute(self):
        database = Database(0, DatabaseContent({b"0": KeyValue(b"0", 0)}))

        assert Get(database, b"0").execute() == 0


class TestGetExpire:
    def test_execute(self):
        database = Database(0, DatabaseContent({b"foo": KeyValue(b"foo", 1)}))

        now_milliseconds = now_ms()

        assert GetExpire(database, b"foo", pxat=now_milliseconds + 10000).execute() == 1

        assert database.content.data.get(b"foo").expiration == now_milliseconds + 10000


class TestLongestCommonSubsequence:
    def test_execute(self):
        database = Database(0)

        database.string_database.upsert(b"key1", b"ohmytext")
        database.string_database.upsert(b"key2", b"mynewtext")

        assert LongestCommonSubsequence(database, b"key1", b"key2").execute() == b"mytext"
        assert LongestCommonSubsequence(database, b"key1", b"key2", length=True).execute() == 6
        assert LongestCommonSubsequence(database, b"key1", b"key2", index=True).execute() == {
            b"matches": [
                [
                    [4, 7],
                    [5, 8],
                ],
                [
                    [2, 3],
                    [0, 1],
                ],
            ],
            b"len": 6,
        }
        assert LongestCommonSubsequence(database, b"key1", b"key2", index=True, min_match_length=4).execute() == {
            b"matches": [
                [
                    [4, 7],
                    [5, 8],
                ]
            ],
            b"len": 6,
        }
        assert LongestCommonSubsequence(
            database, b"key1", b"key2", index=True, min_match_length=4, with_match_length=True
        ).execute() == {
            b"matches": [
                [
                    [4, 7],
                    [5, 8],
                    4,
                ]
            ],
            b"len": 6,
        }

    def test_execute_rna(self):
        database = Database(0)

        database.bytes_database.upsert(
            b"rna1",
            b"CACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACT"
            b"TTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAAC"
            b"TAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCG"
            b"TCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTTCGTCCGGGTGTG",
        )
        database.bytes_database.upsert(
            b"rna2",
            b"ATTAAAGGTTTATACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTC"
            b"TCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGT"
            b"ATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTG"
            b"CTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT",
        )

        assert (
            LongestCommonSubsequence(database, b"rna1", b"rna2").execute()
            == b"ACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTT"
            b"TAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACT"
            b"AATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGT"
            b"CCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT"
        )
