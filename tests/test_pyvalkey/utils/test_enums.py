import pytest

from pyvalkey.utils.enums import BytesEnum


class Protocol(BytesEnum):
    START = b"\x02"
    STOP = "STOP", "utf-8"
    ACK = [65, 67, 75]  # noqa:RUF012
    SIZE_TEN = 10
    EMPTY = ()
    UTF16_DATA = "data", "utf-16"
    NULL = b""


class TestBytesEnum:
    def test_inheritance_and_type(self):
        member = Protocol.START
        assert isinstance(member, BytesEnum)
        assert isinstance(member, bytes)
        assert issubclass(Protocol, bytes)

    def test_value_and_name_access(self):
        assert Protocol.START.name == "START"
        assert Protocol.START.value == b"\x02"
        assert Protocol.STOP.value == b"STOP"

    def test_zero_arguments(self):
        assert Protocol.NULL.value == b""
        assert len(Protocol.NULL) == 0

    def test_one_argument_bytes_literal(self):
        assert Protocol.START == b"\x02"

    def test_one_argument_string(self):
        assert Protocol.STOP == b"STOP"

    def test_one_argument_iterable(self):
        assert Protocol.ACK.value == b"ACK"
        assert Protocol.ACK.decode("ascii") == "ACK"

    def test_one_argument_integer_size(self):
        assert len(Protocol.SIZE_TEN) == 10
        assert Protocol.SIZE_TEN == b"\x00" * 10

    def test_two_arguments_encoding(self):
        assert Protocol.UTF16_DATA.value == "data".encode("utf-16")
        assert Protocol.UTF16_DATA.value.decode("utf-16") == "data"

    def test_bytes_functionality(self):
        result = Protocol.START + Protocol.STOP
        assert result == b"\x02STOP"
        assert result.startswith(Protocol.START)
        assert b"O" in Protocol.STOP

    def test_repr_and_str_from_reprenum(self):
        assert str(Protocol.STOP) == repr(b"STOP")
        expected_repr = "<Protocol.STOP: b'STOP'>"
        assert repr(Protocol.STOP) == expected_repr


class TestBytesEnumErrorHandling:
    def test_too_many_arguments_raises_typeerror(self):
        with pytest.raises(TypeError, match="too many arguments for bytes\(\)"):

            class BadEnum(BytesEnum):
                MEMBER = "a", "b", "c", "d"

    def test_invalid_encoding_type_raises_typeerror(self):
        with pytest.raises(TypeError, match="encoding must be a string"):

            class BadEnum(BytesEnum):
                MEMBER = "data", 123

    def test_invalid_errors_type_raises_typeerror(self):
        with pytest.raises(TypeError, match="errors must be a string"):

            class BadEnum(BytesEnum):
                MEMBER = "data", "utf-8", 123

    def test_invalid_bytes_construction_raises_error(self):
        with pytest.raises(LookupError):

            class BadEnum(BytesEnum):
                MEMBER = "data", "bad_encoding_name"
