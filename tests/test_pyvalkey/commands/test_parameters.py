from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import (
    ParameterMetadata,
    flag_parameter,
    keyword_numeric_option,
    keyword_string_option,
    positional_boolean_flag,
    positional_parameter,
)
from pyvalkey.commands.parsers import ObjectParametersParser
from pyvalkey.commands.router import transform_command
from pyvalkey.database_objects.databases import Database
from pyvalkey.resp import ValueType
from pyvalkey.utils.dependencies import dependency


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
    param = flag_parameter(token=b"REPLACE", default=False)
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

    param2 = flag_parameter(token=b"REPLACE", default=False)
    assert ParameterMetadata.COMMAND_PARAMETER in param2.metadata
    assert ParameterMetadata.TOKEN in param2.metadata

    param3 = keyword_numeric_option(flag=b"EX", default=None)
    assert ParameterMetadata.COMMAND_PARAMETER in param3.metadata
    assert ParameterMetadata.TOKEN in param3.metadata

    param4 = keyword_string_option(flag=b"DB", default=None)
    assert ParameterMetadata.COMMAND_PARAMETER in param4.metadata
    assert ParameterMetadata.TOKEN in param4.metadata


@transform_command
class GetSetTestCommand(Command):
    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    get_flag: bool = positional_boolean_flag(default=False)

    def execute(self) -> ValueType:
        pass


@transform_command
class SetTestCommand(Command):
    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    value: bytes = positional_parameter()
    ex_seconds: int | None = keyword_numeric_option(flag=b"EX", default=None)
    px_milliseconds: int | None = keyword_numeric_option(flag=b"PX", default=None)
    nx_flag: bool = flag_parameter(token=b"NX", default=False)

    def execute(self) -> ValueType:
        pass


@transform_command
class SelectTestCommand(Command):
    database: Database = dependency()

    db_index: int = positional_parameter()

    def execute(self) -> ValueType:
        pass


@transform_command
class ClientTestCommand(Command):
    database: Database = dependency()

    client_type: bytes | None = keyword_string_option(flag=b"TYPE", default=None)
    client_id: int | None = keyword_numeric_option(flag=b"ID", default=None)

    def execute(self) -> ValueType:
        pass


@transform_command
class ZAddTestCommand(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
    score_member: list[tuple[float, bytes]] = positional_parameter()
    changed_flag: bool = flag_parameter(token=b"CH", default=False)

    def execute(self) -> ValueType:
        pass


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
    parser = ObjectParametersParser.create(ClientTestCommand)
    params = [b"TYPE", b"normal"]
    result = parser.parse(params)
    assert result["client_type"] == b"normal"
    # client_id should not be in result when not provided (it's optional)


def test_client_with_id():
    parser = ObjectParametersParser.create(ClientTestCommand)
    params = [b"ID", b"123"]
    result = parser.parse(params)
    assert result["client_id"] == 123
    # client_type should not be in result when not provided (it's optional)


def test_zadd_with_ch():
    parser = ObjectParametersParser.create(ZAddTestCommand)
    params = [b"myset", b"1.5", b"member1", b"CH"]
    result = parser.parse(params)
    assert result["changed_flag"] is True
