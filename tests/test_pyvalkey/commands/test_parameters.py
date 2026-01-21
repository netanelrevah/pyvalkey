from pyvalkey.commands.core import DatabaseCommand
from pyvalkey.commands.parameters import (
    ParameterMetadata,
    keyword_boolean_flag,
    keyword_numeric_option,
    keyword_string_option,
    positional_boolean_flag,
    positional_parameter,
)
from pyvalkey.commands.parsers import ObjectParametersParser
from pyvalkey.commands.router import command
from pyvalkey.resp import ValueType


def test_positional_boolean_flag_default_false():
    param = positional_boolean_flag(default=False)
    assert param.default is False
    assert param.metadata is not None


def test_positional_boolean_flag_default_true():
    param = positional_boolean_flag(default=True)
    assert param.default is True


def test_positional_boolean_flag_custom_mapping():
    custom_mapping = {b"YES": True, b"NO": False}
    param = positional_boolean_flag(values_mapping=custom_mapping)
    assert param.metadata is not None


def test_keyword_boolean_flag():
    param = keyword_boolean_flag(flag=b"REPLACE", default=False)
    assert param.metadata is not None
    assert param.metadata[ParameterMetadata.TOKEN] == b"REPLACE"


def test_keyword_numeric_option():
    param = keyword_numeric_option(flag=b"EX", default=None)
    assert param.metadata is not None
    assert param.metadata[ParameterMetadata.TOKEN] == b"EX"


def test_keyword_string_option():
    param = keyword_string_option(flag=b"DB", default=None)
    assert param.metadata is not None
    assert param.metadata[ParameterMetadata.TOKEN] == b"DB"


def test_all_parameter_types_have_metadata():
    param1 = positional_boolean_flag(default=False)
    assert ParameterMetadata.COMMAND_PARAMETER in param1.metadata

    param2 = keyword_boolean_flag(flag=b"REPLACE", default=False)
    assert ParameterMetadata.COMMAND_PARAMETER in param2.metadata
    assert ParameterMetadata.TOKEN in param2.metadata

    param3 = keyword_numeric_option(flag=b"EX", default=None)
    assert ParameterMetadata.COMMAND_PARAMETER in param3.metadata
    assert ParameterMetadata.TOKEN in param3.metadata

    param4 = keyword_string_option(flag=b"DB", default=None)
    assert ParameterMetadata.COMMAND_PARAMETER in param4.metadata
    assert ParameterMetadata.TOKEN in param4.metadata


@command(b"getset", {b"test"})
class GetSetTestCommand(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    get_flag: bool = positional_boolean_flag(default=False)

    def execute(self) -> ValueType:
        """Return the old value if GET flag is set, otherwise return OK."""
        old_value = self.database.string_database.get_value_or_none(self.key)
        self.database.string_database.upsert(self.key, self.value)
        return old_value if self.get_flag else b"OK"


@command(b"set", {b"test"})
class SetTestCommand(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    ex_seconds: int | None = keyword_numeric_option(flag=b"EX", default=None)
    px_milliseconds: int | None = keyword_numeric_option(flag=b"PX", default=None)
    nx_flag: bool = keyword_boolean_flag(flag=b"NX", default=False)

    def execute(self) -> bytes:
        if self.nx_flag and self.database.string_database.has_key(self.key):
            return b"NOT_SET"

        self.database.string_database.upsert(self.key, self.value)

        if self.ex_seconds is not None:
            self.database.set_expiration_in(self.key, 1000 * self.ex_seconds)
        elif self.px_milliseconds is not None:
            self.database.set_expiration_in(self.key, self.px_milliseconds)

        return b"OK"


@command(b"select", {b"test"})
class SelectTestCommand(DatabaseCommand):
    db_index: int = positional_parameter()

    def execute(self) -> bytes:
        return f"Selected DB: {self.db_index}".encode()


@command(b"client", {b"test"})
class ClientTestCOmmand(DatabaseCommand):
    client_type: bytes | None = keyword_string_option(flag=b"TYPE", default=None)
    client_id: int | None = keyword_numeric_option(flag=b"ID", default=None)

    def execute(self) -> dict[bytes, bytes]:
        result = {}
        if self.client_type is not None:
            result[b"type"] = self.client_type
        if self.client_id is not None:
            result[b"id"] = str(self.client_id).encode()
        return result


@command(b"zadd", {b"test"})
class ZAddTestCommand(DatabaseCommand):
    key: bytes = positional_parameter()
    score_member: list[tuple[float, bytes]] = positional_parameter()
    changed_flag: bool = keyword_boolean_flag(flag=b"CH", default=False)

    def execute(self) -> int:
        # In a real implementation, this would actually modify the sorted set
        num_changes = len(self.score_member)
        return num_changes if self.changed_flag else 0


def test_getset_without_get():
    parser = ObjectParametersParser.create(GetSetTestCommand)
    params = [b"mykey", b"myvalue"]
    parser.parse(params)


def test_getset_with_get():
    parser = ObjectParametersParser.create(GetSetTestCommand)
    params = [b"mykey", b"myvalue", b"1"]  # GET flag as "1"
    result = parser.parse(params)
    assert result["get_flag"] is True


def test_set_with_ex():
    parser = ObjectParametersParser.create(SetTestCommand)
    params = [b"mykey", b"myvalue", b"EX", b"10"]
    result = parser.parse(params)
    assert result["ex_seconds"] == 10
    # nx_flag should not be in result when not provided (it's optional)


def test_set_with_nx():
    parser = ObjectParametersParser.create(SetTestCommand)
    params = [b"mykey", b"myvalue", b"NX"]
    result = parser.parse(params)
    assert result["nx_flag"] is True
    # ex_seconds should not be in result when not provided (it's optional)


def test_client_with_type():
    parser = ObjectParametersParser.create(ClientTestCOmmand)
    params = [b"TYPE", b"normal"]
    result = parser.parse(params)
    assert result["client_type"] == b"normal"
    # client_id should not be in result when not provided (it's optional)


def test_client_with_id():
    parser = ObjectParametersParser.create(ClientTestCOmmand)
    params = [b"ID", b"123"]
    result = parser.parse(params)
    assert result["client_id"] == 123
    # client_type should not be in result when not provided (it's optional)


def test_zadd_with_ch():
    parser = ObjectParametersParser.create(ZAddTestCommand)
    params = [b"myset", b"1.5", b"member1", b"CH"]
    result = parser.parse(params)
    assert result["changed_flag"] is True
