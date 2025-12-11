from unittest.mock import Mock

import pytest

from pyvalkey.commands.set_commands import SetAreMembers, SetIntersectionCardinality, SetRandomMember
from pyvalkey.commands.string_commands import SetMultiple
from pyvalkey.database_objects.databases import Database, KeyValue
from pyvalkey.database_objects.errors import ServerError, ServerWrongNumberOfArgumentsError


class TestSetIntersectionCardinality:
    def test_parse(self):
        """
        assert_error "ERR wrong number of arguments for 'sintercard' command" {r sintercard}
        assert_error "ERR wrong number of arguments for 'sintercard' command" {r sintercard 1}

        assert_error "ERR numkeys*" {r sintercard 0 myset{t}}
        assert_error "ERR numkeys*" {r sintercard a myset{t}}

        assert_error "ERR Number of keys*" {r sintercard 2 myset{t}}
        assert_error "ERR Number of keys*" {r sintercard 3 myset{t} myset2{t}}

        assert_error "ERR syntax error*" {r sintercard 1 myset{t} myset2{t}}
        assert_error "ERR syntax error*" {r sintercard 1 myset{t} bar_arg}
        assert_error "ERR syntax error*" {r sintercard 1 myset{t} LIMIT}

        assert_error "ERR LIMIT*" {r sintercard 1 myset{t} LIMIT -1}
        assert_error "ERR LIMIT*" {r sintercard 1 myset{t} LIMIT a}
        """
        with pytest.raises(ServerWrongNumberOfArgumentsError):
            SetIntersectionCardinality.parse([])

        with pytest.raises(ServerWrongNumberOfArgumentsError):
            SetIntersectionCardinality.parse([b"1"])

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"0", b"s"])
        assert e.value.message == b"ERR numkeys should be greater than 0"

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"a", b"s"])
        assert e.value.message == b"ERR numkeys should be greater than 0"

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"2", b"s"])
        assert e.value.message == b"ERR Number of keys can't be greater than number of args"

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"3", b"s", b"s2"])
        assert e.value.message == b"ERR Number of keys can't be greater than number of args"

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"1", b"s", b"s2"])
        assert e.value.message == b"ERR syntax error"

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"1", b"s", b"bla"])
        assert e.value.message == b"ERR syntax error"

        with pytest.raises(ServerError) as e:
            SetIntersectionCardinality.parse([b"1", b"s", b"LIMIT"])
        assert type(e.value) is ServerError
        assert e.value.message == b"ERR syntax error"


class TestSetRandomMember:
    def test_parse(self):
        assert SetRandomMember.parse([b"ss", b"100"]) == {"key": b"ss", "count": 100}

    def test_create(self):
        client_context = Mock(spec_set=["database"])

        command = SetRandomMember.create([b"ss", b"100"], client_context)
        assert command.key == b"ss"
        assert command.count == 100

    def test_execute(self):
        database = Database(0, None, None)
        database.set_database.set_key_value(KeyValue(b"ss", {b"a"}))

        command = SetRandomMember(database=database, key=b"ss", count=100)

        assert command.execute() == [b"a"]


class TestSetMultiple:
    def test_parse(self):
        with pytest.raises(expected_exception=ServerWrongNumberOfArgumentsError):
            assert SetMultiple.parse([b"a", b"1", b"b"])


class TestSetAreMembers:
    def test_parse(self):
        with pytest.raises(expected_exception=ServerWrongNumberOfArgumentsError):
            assert SetAreMembers.parse([b"zmscoretest"])
