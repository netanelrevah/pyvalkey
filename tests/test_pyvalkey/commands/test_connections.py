import pytest

from pyvalkey.commands.connections import Hello
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RespProtocolVersion


class TestSetRandomMember:
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
