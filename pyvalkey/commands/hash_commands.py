import random
from math import isinf, isnan

from pyvalkey.commands.consts import LONG_MAX, LONG_MIN
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.commands.utils import increment_bytes_value_as_float, is_floating_point, is_integer
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.utils import flatten
from pyvalkey.resp import RESP_OK, RespProtocolVersion, ValueType


def increment_by_int(database: Database, key: bytes, field: bytes, increment: int) -> int:
    hash_value = database.hash_database.get_value_or_create(key)
    previous_value = hash_value.get(field, 0)
    if isinstance(previous_value, bytes):
        if not is_integer(previous_value):
            raise ServerError(b"ERR hash value is not an integer")
        previous_value = int(previous_value)
    if (increment < 0 and previous_value < 0 and increment < LONG_MIN - previous_value) or (
        increment > 0 and previous_value > 0 and increment > LONG_MAX - previous_value
    ):
        raise ServerError(b"ERR increment or decrement would overflow")
    new_value = previous_value + increment
    hash_value[field] = new_value
    return new_value


def increment_by_float(database: Database, key: bytes, field: bytes, increment: float) -> bytes:
    hash_value = database.hash_database.get_value_or_create(key)
    previous_value = hash_value.get(field, b"0")
    if isinstance(previous_value, bytes):
        if not is_floating_point(previous_value):
            raise ServerError(b"ERR hash value is not an float")
    elif isinstance(previous_value, int):
        previous_value = str(previous_value).encode()
    new_value = increment_bytes_value_as_float(previous_value, increment)
    hash_value[field] = new_value
    return new_value


def apply_hash_map_increase_by(database: Database, key: bytes, field: bytes, increment: int | float) -> bytes | int:
    if isinstance(increment, int):
        return increment_by_int(database, key, field, increment)
    elif isinstance(increment, float):
        return increment_by_float(database, key, field, increment)
    else:
        raise ValueError()

    # hash_get = database.hash_database.get_value_or_create(key)
    #
    # if field not in hash_get:
    #     hash_get[field] = 0
    # if not isinstance(hash_get[field], int | float):
    #     if not is_numeric(hash_get[field]):
    #         raise ServerError(b"ERR hash value is not an " + (b"integer" if isinstance(increment, int) else b"float"))
    #     hash_get[field] = type(increment)(hash_get[field])
    # if not (LONG_LONG_MIN < hash_get[field] + increment < LONG_LONG_MAX):
    #     raise ServerError(b"ERR increment or decrement would overflow")
    # hash_get[field] += increment
    # return hash_get[field]


@command(b"hdel", {b"write", b"hash", b"fast"})
class HashMapDelete(DatabaseCommand):
    key: bytes = positional_parameter()
    fields: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.hash_database.get_value_or_empty(self.key)
        result = sum([1 if value.pop(f, None) is not None else 0 for f in self.fields])
        return result


@command(b"hexists", {b"read", b"hash", b"fast"})
class HashMapExists(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.field in self.database.hash_database.get_value(self.key)


@command(b"hget", {b"read", b"hash", b"fast"})
class HashMapGet(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.hash_database.get_value(self.key).get(self.field)


@command(b"hgetall", {b"read", b"hash", b"slow"})
class HashMapGetAll(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        hash_set = self.database.hash_database.get_value_or_empty(self.key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response


@command(b"hincrby", {b"write", b"hash", b"fast"})
class HashMapIncreaseBy(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()
    value: int = positional_parameter()

    def execute(self) -> ValueType:
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@command(b"hincrbyfloat", {b"write", b"hash", b"fast"})
class HashMapIncreaseByFloat(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()
    value: float = positional_parameter()

    def execute(self) -> ValueType:
        if isnan(self.value) or isinf(self.value):
            raise ServerError(b"ERR value is NaN or Infinity")

        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@command(b"hkeys", {b"read", b"hash", b"slow"})
class HashMapKeys(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.hash_database.get_value(self.key).keys())


@command(b"hlen", {b"read", b"hash", b"fast"})
class HashMapLength(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.hash_database.get_value(self.key))


@command(b"hmget", {b"read", b"hash", b"fast"})
class HashMapGetMultiple(DatabaseCommand):
    key: bytes = positional_parameter()
    fields: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.hash_database.get_value_or_empty(self.key)
        return [hash_map.get(f, None) for f in self.fields]


@command(b"hmset", {b"write", b"hash", b"fast"})
class HashMapSetMultiple(DatabaseCommand):
    key: bytes = positional_parameter()
    fields_values: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        for field, value in self.fields_values:
            hash_map[field] = value
        return RESP_OK


@command(b"hrandfield", {b"write", b"hash", b"fast"})
class HashRandomField(DatabaseCommand):
    protocol: RespProtocolVersion = dependency()

    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)
    with_values: bool = keyword_parameter(flag=b"WITHVALUES", default=False)

    def execute(self) -> ValueType:
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        if self.count is None:
            if not hash_map:
                return None
            return random.choice(list(hash_map.keys()))

        if not hash_map:
            return [] if not self.with_values else {}

        if self.count > LONG_MAX / 2 or self.count < -LONG_MAX / 2:
            raise ServerError(b"ERR value is out of range")

        keys = list(hash_map.keys())
        if self.count > 0:
            random.shuffle(keys)
            keys = keys[0 : self.count]
        else:
            keys = random.choices(keys, k=abs(self.count))

        if not self.with_values:
            return keys

        if self.protocol == RespProtocolVersion.RESP3:
            return [[key, hash_map[key]] for key in keys]
        return list(flatten([[key, hash_map[key]] for key in keys]))


@command(b"hscan", {b"hash"})
class HashMapScan(DatabaseCommand):
    key: bytes = positional_parameter()
    cursor: int = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.get_value(self.key)
        return RESP_OK


@command(b"hset", {b"write", b"hash", b"fast"})
class HashMapSet(DatabaseCommand):
    key: bytes = positional_parameter()
    fields_values: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        added_fields = 0
        for field, value in self.fields_values:
            if field not in hash_map:
                added_fields += 1
            hash_map[field] = value
        return added_fields


@command(b"hsetnx", {b"write", b"hash", b"fast"})
class HashMapSetIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        if self.field in hash_map:
            return False
        hash_map[self.field] = self.value
        return True


@command(b"hstrlen", {b"read", b"hash", b"fast"})
class HashMapStringLength(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.hash_database.get_value_or_empty(self.key)
        field_value = value.get(self.field, b"")
        if isinstance(field_value, int):
            field_value = str(field_value).encode()
        return len(field_value)


@command(b"hvals", {b"read", b"hash", b"slow"})
class HashMapValues(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.hash_database.get_value(self.key).values())
