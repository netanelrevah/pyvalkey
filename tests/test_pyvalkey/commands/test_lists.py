from unittest.mock import Mock

from pyvalkey.commands.list_commands import ListLength
from pyvalkey.database_objects.databases import Database, KeyValue


class TestListLength:
    def test_parse(self):
        assert ListLength.parse([b"abc"]) == {"key": b"abc"}

    def test_create(self):
        client_context = Mock(spec_set=["database"])

        command = ListLength.create([b"abc"], client_context)
        assert command.key == b"abc"
        assert command.database == client_context.database

    def test_execute(self):
        database = Database()
        database.list_database.set_key_value(KeyValue(b"abc", [1, 2, 3]))

        command = ListLength(database=database, key=b"abc")

        assert command.execute() == 3
