from unittest.mock import Mock, call

from pyvalkey.commands.set_commands import SetRandomMember


class TestSetRandomMember:
    def test_parse(self):
        assert SetRandomMember.parse([b"ss", b"100"]) == {"key": b"ss", "count": 100}

    def test_create(self):
        client_context = Mock(spec_set=["database"])

        command = SetRandomMember.create([b"ss", b"100"], client_context)
        assert command.key == b"ss"
        assert command.count == 100

    def test_execute(self):
        database = Mock(spec_set=["get_set_or_none"])
        database.get_set_or_none.return_value = {"a"}

        command = SetRandomMember(database=database, key=b"ss", count=100)

        assert command.execute() == ["a"]

        assert database.mock_calls == [call.get_set_or_none(b"ss")]
