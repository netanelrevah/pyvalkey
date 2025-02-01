from pyvalkey.commands.sort_commands import Sort
from pyvalkey.database_objects.databases import Database, KeyValue, StringType


class TestSetRandomMember:
    def test_parse(self):
        assert Sort.parse([b"a", b"by", b"ww*", b"limit", b"1", b"2"]) == {"key": b"a", "by": b"ww*", "limit": (1, 2)}
        assert Sort.parse([b"tosort", b"get", b"#"]) == {"key": b"tosort", "get_values": [b"#"]}
        assert Sort.parse([b"tosort", b"get", b"#", b"get", b"22"]) == {"key": b"tosort", "get_values": [b"#", b"22"]}
        assert Sort.parse([b"tosort", b"desc"]) == {"key": b"tosort", "descending": True}

    # def test_create(self):
    #     client_context = Mock(spec_set=["database"])
    #
    #     command = Hello.create([b"ss", b"100"], client_context)
    #     assert command.protocol_version == b"ss"
    #

    def test_execute_get(self):
        database = Database(
            data={
                kv.key: kv
                for kv in [
                    KeyValue(b"a_1", StringType(b"a")),
                    KeyValue(b"a_2", StringType(b"b")),
                    KeyValue(b"a_3", StringType(b"c")),
                    KeyValue(b"to_sort", [b"3", b"2", b"1"]),
                ]
            }
        )

        command = Sort(database=database, key=b"to_sort", get_values=[b"a_*"])

        assert command.execute() == [b"a", b"b", b"c"]

    def test_execute_get_hash(self):
        database = Database(
            data={
                kv.key: kv
                for kv in [
                    KeyValue(b"a", StringType(b"aa")),
                    KeyValue(b"b", StringType(b"bb")),
                    KeyValue(b"c", StringType(b"cc")),
                    KeyValue(b"to_sort", [b"a", b"b", b"c"]),
                ]
            }
        )

        command = Sort(database=database, key=b"to_sort", get_values=[b"#"], alpha=True)

        assert command.execute() == [b"a", b"b", b"c"]

    def test_execute_get_hash_with_numbers(self):
        database = Database(
            data={
                kv.key: kv
                for kv in [
                    KeyValue(b"to_sort", [b"5", b"0", b"1", b"2", b"3", b"4"]),
                ]
            }
        )

        command = Sort(database=database, key=b"to_sort", get_values=[b"#"])

        assert command.execute() == [b"0", b"1", b"2", b"3", b"4", b"5"]

    def test_execute_get_foo(self):
        database = Database(
            data={
                kv.key: kv
                for kv in [
                    KeyValue(b"a", StringType(b"aa")),
                    KeyValue(b"b", StringType(b"bb")),
                    KeyValue(b"c", StringType(b"cc")),
                    KeyValue(b"to_sort", [b"a", b"b", b"c"]),
                ]
            }
        )

        command = Sort(database=database, key=b"to_sort", get_values=[b"foo"], alpha=True)

        assert command.execute() == [None, None, None]
