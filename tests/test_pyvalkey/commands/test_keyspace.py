from pyvalkey.commands.generic_commands import Expire, Keys, TimeToLive
from pyvalkey.commands.string_commands import Set
from pyvalkey.database_objects.databases import Database


class TestTimeToLive:
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
    def test_ttl_with_expire(self):
        database = Database()
        Set(database, b"mykey{t}", b"foo").execute()
        Expire(database, b"mykey{t}", 100).execute()

        command = TimeToLive(database, b"mykey{t}")

        assert command.execute() == 100


class TestKeys:
    def test_validate_pattern(self):
        assert Keys.nesting(b"*?" * 1000) == 1000
        assert Keys.nesting(b"*?" * 1001) == 1001
        assert Keys.nesting(b"?*" * 1001) == 1000
        assert Keys.nesting(b"?*" * 1002) == 1001
