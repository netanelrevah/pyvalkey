from typing import List

from pyvalkey.commands.databases import DatabaseCommand
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerInvalidIntegerError
from pyvalkey.resp import RESP_OK, ValueType


def apply_hash_map_increase_by(database: Database, key: bytes, field: bytes, increment: int | float) -> dict:
    hash_get = database.get_or_create_hash_table(key)

    if field not in hash_get:
        hash_get[field] = 0
    if not isinstance(hash_get[field], (int, float)):
        raise ServerInvalidIntegerError
    hash_get[field] += increment
    return hash_get[field]


@ServerCommandsRouter.command(b"hincrby", [b"write", b"hash", b"fast"])
class HashMapIncreaseBy(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()
    value: int = positional_parameter()

    def execute(self) -> ValueType:
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@ServerCommandsRouter.command(b"hincrbyfloat", [b"write", b"hash", b"fast"])
class HashMapIncreaseByFloat(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()
    value: float = positional_parameter()

    def execute(self) -> ValueType:
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@ServerCommandsRouter.command(b"hexists", [b"read", b"hash", b"fast"])
class HashMapExists(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.field in self.database.get_hash_table(self.key)


@ServerCommandsRouter.command(b"hget", [b"read", b"hash", b"fast"])
class HashMapGet(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.get_hash_table(self.key).get(self.field)


@ServerCommandsRouter.command(b"hmget", [b"read", b"hash", b"fast"])
class HashMapGetMultiple(DatabaseCommand):
    key: bytes = positional_parameter()
    fields: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.get_hash_table(self.key)
        return [hash_map.get(f, None) for f in self.fields]


@ServerCommandsRouter.command(b"hgetall", [b"read", b"hash", b"slow"])
class HashMapGetAll(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        hash_set = self.database.get_hash_table(self.key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response


@ServerCommandsRouter.command(b"hkeys", [b"read", b"hash", b"slow"])
class HashMapKeys(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.get_hash_table(self.key).keys())


@ServerCommandsRouter.command(b"hlen", [b"read", b"hash", b"fast"])
class HashMapLength(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.get_hash_table(self.key))


@ServerCommandsRouter.command(b"hvals", [b"read", b"hash", b"slow"])
class HashMapValues(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return list(self.database.get_hash_table(self.key).values())


@ServerCommandsRouter.command(b"hstrlen", [b"read", b"hash", b"fast"])
class HashMapStringLength(DatabaseCommand):
    key: bytes = positional_parameter()
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.get_hash_table(self.key).get(self.field, b""))


@ServerCommandsRouter.command(b"hdel", [b"write", b"hash", b"fast"])
class HashMapDelete(DatabaseCommand):
    key: bytes = positional_parameter()
    fields: List[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.get_hash_table(self.key)

        return sum([1 if hash_map.pop(f, None) is not None else 0 for f in self.fields])


@ServerCommandsRouter.command(b"hset", [b"write", b"hash", b"fast"])
class HashMapSet(DatabaseCommand):
    key: bytes = positional_parameter()
    fields_values: List[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.get_or_create_hash_table(self.key)

        added_fields = 0
        for field, value in self.fields_values:
            if field not in hash_map:
                added_fields += 1
            hash_map[field] = value
        return added_fields


@ServerCommandsRouter.command(b"hmset", [b"write", b"hash", b"fast"])
class HashMapSetMultiple(DatabaseCommand):
    key: bytes = positional_parameter()
    fields_values: List[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        hash_map = self.database.get_or_create_hash_table(self.key)

        for field, value in self.fields_values:
            hash_map[field] = value
        return RESP_OK
