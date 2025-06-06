from contextlib import contextmanager

import pytest
import valkey

BITS_IN_BYTE = 8


def key_value_list_to_dict(key_value_list: list):
    return dict(zip(key_value_list[0::2], key_value_list[1::2]))


@contextmanager
def assert_raises(expected_exception: type[valkey.ValkeyError], message):
    with pytest.raises(expected_exception) as e:
        yield
    assert e.value.args[0] == message


def bits_to_bytes(value: str) -> bytes:
    result = []
    while value:
        byte = value[0:8]
        if len(byte) < BITS_IN_BYTE:
            byte += "0" * (8 - len(byte))
        value = value[8:]
        result.append(int(byte, 2))
    return bytes(result)
