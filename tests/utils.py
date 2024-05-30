from contextlib import contextmanager

import pytest
import redis


def key_value_list_to_dict(key_value_list: list):
    return dict(zip(key_value_list[0::2], key_value_list[1::2]))


@contextmanager
def assert_raises(expected_exception: type[redis.RedisError], message):
    with pytest.raises(expected_exception) as e:
        yield
    assert e.value.args[0] == message
