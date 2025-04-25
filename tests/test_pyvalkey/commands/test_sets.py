from unittest.mock import Mock

from pyvalkey.commands.set_commands import SetRandomMember
from pyvalkey.database_objects.databases import Database, KeyValue


class TestSetRandomMember:
    def test_parse(self):
        assert SetRandomMember.parse([b"ss", b"100"]) == {"key": b"ss", "count": 100}

    def test_create(self):
        client_context = Mock(spec_set=["database"])

        command = SetRandomMember.create([b"ss", b"100"], client_context)
        assert command.key == b"ss"
        assert command.count == 100

    def test_execute(self):
        database = Database(0)
        database.set_database.set_key_value(KeyValue(b"ss", {b"a"}))

        command = SetRandomMember(database=database, key=b"ss", count=100)

        assert command.execute() == [b"a"]
