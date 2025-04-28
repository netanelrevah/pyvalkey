import random

from pyvalkey.commands.consts import LONG_LONG_MAX, LONG_LONG_MIN, LONG_MAX
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.commands.utils import is_numeric
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.utils import flatten
from pyvalkey.resp import RESP_OK, RespProtocolVersion, ValueType


def apply_hash_map_increase_by(database: Database, key: bytes, field: bytes, increment: int | float) -> dict:
    hash_get = database.hash_database.get_value_or_create(key)

    if field not in hash_get:
        hash_get[field] = 0
    if not isinstance(hash_get[field], int | float):
        if not is_numeric(hash_get[field]):
            raise ServerError(b"ERR hash value is not an " + (b"integer" if isinstance(increment, int) else b"float"))
        hash_get[field] = type(increment)(hash_get[field])
    if not (LONG_LONG_MIN < hash_get[field] + increment < LONG_LONG_MAX):
        raise ServerError(b"ERR increment or decrement would overflow")
    hash_get[field] += increment
    return hash_get[field]


@command(b"hdel", {b"write", b"hash", b"fast"})
class HashMapDelete(DatabaseCommand):
    key: bytes = positional_parameter()
    fields: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.hash_database.get_or_none(self.key)

        if key_value is None:
            return 0
        hash_map = key_value.value

        result = sum([1 if hash_map.pop(f, None) is not None else 0 for f in self.fields])

        if not hash_map:
            self.database.pop(self.key)

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
        hash_set = self.database.hash_database.get_value(self.key)

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
        hash_map = self.database.hash_database.get_value(self.key)
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
    protocol: RespProtocolVersion = server_command_dependency()

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
        return len(self.database.hash_database.get_value(self.key).get(self.field, b""))


@command(b"hvals", {b"read", b"hash", b"slow"})
class HashMapValues(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.hash_database.get_value(self.key).values())
