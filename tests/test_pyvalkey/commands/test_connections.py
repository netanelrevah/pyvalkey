import pytest

from pyvalkey.commands.connection_commands import Hello
from pyvalkey.commands.server_commands import CommandGetKeys
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RespProtocolVersion


class TestHello:
    def test_parse(self):
        assert Hello.parse([]) == {}
        assert Hello.parse([b"2"]) == {"protocol_version": RespProtocolVersion.RESP2}
        assert Hello.parse([b"3"]) == {"protocol_version": RespProtocolVersion.RESP3}
        with pytest.raises(ServerError, match="NOPROTO unsupported protocol version"):
            assert Hello.parse([b"4"])

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


class TestCommandGetKeys:
    # def test_parse(self):
    #     assert Hello.parse([]) == {}
    #     assert Hello.parse([b"2"]) == {"protocol_version": RespProtocolVersion.RESP2}
    #     assert Hello.parse([b"3"]) == {"protocol_version": RespProtocolVersion.RESP3}
    #     with pytest.raises(ServerError, match="NOPROTO unsupported protocol version"):
    #         assert Hello.parse([b"4"])

    # def test_create(self):
    #     client_context = Mock(spec_set=["database"])
    #
    #     command = Hello.create([b"ss", b"100"], client_context)
    #     assert command.protocol_version == b"ss"
    #
    def test_sort_with_one_store(self):
        command = CommandGetKeys(b"sort", [b"abc", b"store", b"def"])

        assert command.execute() == [b"abc", b"def"]

    def test_sort_with_multi_store(self):
        command = CommandGetKeys(b"sort", [b"abc", b"store", b"def", b"store", b"def2"])

        assert command.execute() == [b"abc", b"def2"]
