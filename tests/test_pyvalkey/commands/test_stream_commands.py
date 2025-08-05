from pyvalkey.commands.stream_commands import StreamGroupRead


class TestStreamGroupRead:
    def test_parse(self):
        assert StreamGroupRead.parse([b"g1", b"Alice", b"COUNT", b"1", b"STREAMS", b"x{t}", b">"]) == {
            "group": b"g1",
            "consumer": b"Alice",
            "count": 1,
            "keys_and_ids": [b"x{t}", b">"],
        }

    # def test_create(self):
    #     client_context = Mock(spec_set=["database"])
    #
    #     command = Hello.create([b"ss", b"100"], client_context)
    #     assert command.protocol_version == b"ss"
    #
    # def test_execute(self):
    #     database = Mock(spec_set=["get_set_or_none"])
    #     database.get_set_or_none.return_value = {"a"}
    #
    #     command = Hello(database=database, key=b"ss", count=100)
    #
    #     assert command.execute() == ["a"]
    #
    #     assert database.mock_calls == [call.get_set_or_none(b"ss")]
