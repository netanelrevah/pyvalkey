from unittest.mock import Mock, call

from pyvalkey.commands.list_commands import ListLength


class TestListLength:
    def test_parse(self):
        assert ListLength.parse([b"abc"]) == {"key": b"abc"}

    def test_create(self):
        client_context = Mock(spec_set=["database"])

        command = ListLength.create([b"abc"], client_context)
        assert command.key == b"abc"
        assert command.database == client_context.database

    def test_execute(self):
        database = Mock(spec_set=["get_list"])
        database.get_list.return_value = [1, 2, 3]

        command = ListLength(database=database, key=b"abc")

        assert command.execute() == 3

        assert database.mock_calls == [call.get_list(b"abc")]
