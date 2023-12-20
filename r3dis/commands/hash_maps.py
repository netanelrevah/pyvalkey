from r3dis.commands.databases import DatabaseCommand
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.databases import Database
from r3dis.database_objects.errors import RedisInvalidIntegerError
from r3dis.resp import RESP_OK


def apply_hash_map_increase_by(database: Database, key: bytes, field: bytes, increment: int | float):
    hash_get = database.get_or_create_hash_table(key)

    if field not in hash_get:
        hash_get[field] = 0
    if not isinstance(hash_get[field], (int, float)):
        raise RedisInvalidIntegerError
    hash_get[field] += increment
    return hash_get[field]


@RedisCommandsRouter.command(b"hincrby", [b"write", b"hash", b"fast"])
class HashMapIncreaseBy(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()
    value: int = redis_positional_parameter()

    def execute(self):
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@RedisCommandsRouter.command(b"hincrbyfloat", [b"write", b"hash", b"fast"])
class HashMapIncreaseByFloat(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()
    value: float = redis_positional_parameter()

    def execute(self):
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@RedisCommandsRouter.command(b"hexists", [b"read", b"hash", b"fast"])
class HashMapExists(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()

    def execute(self):
        return self.field in self.database.get_hash_table(self.key)


@RedisCommandsRouter.command(b"hget", [b"read", b"hash", b"fast"])
class HashMapGet(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()

    def execute(self):
        return self.database.get_hash_table(self.key).get(self.field)


@RedisCommandsRouter.command(b"hmget", [b"read", b"hash", b"fast"])
class HashMapGetMultiple(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields: list[bytes] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_hash_table(self.key)
        return [hash_map.get(f, None) for f in self.fields]


@RedisCommandsRouter.command(b"hgetall", [b"read", b"hash", b"slow"])
class HashMapGetAll(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        hash_set = self.database.get_hash_table(self.key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response


@RedisCommandsRouter.command(b"hkeys", [b"read", b"hash", b"slow"])
class HashMapKeys(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return list(self.database.get_hash_table(self.key).keys())


@RedisCommandsRouter.command(b"hlen", [b"read", b"hash", b"fast"])
class HashMapLength(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return len(self.database.get_hash_table(self.key))


@RedisCommandsRouter.command(b"hvals", [b"read", b"hash", b"slow"])
class HashMapValues(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return list(self.database.get_hash_table(self.key).values())


@RedisCommandsRouter.command(b"hstrlen", [b"read", b"hash", b"fast"])
class HashMapStringLength(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()

    def execute(self):
        return len(self.database.get_hash_table(self.key).get(self.field, b""))


@RedisCommandsRouter.command(b"hdel", [b"write", b"hash", b"fast"])
class HashMapDelete(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields: list[bytes] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_hash_table(self.key)

        return sum([1 if hash_map.pop(f, None) is not None else 0 for f in self.fields])


@RedisCommandsRouter.command(b"hset", [b"write", b"hash", b"fast"])
class HashMapSet(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields_values: list[tuple[bytes, bytes]] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_or_create_hash_table(self.key)

        added_fields = 0
        for field, value in self.fields_values:
            if field not in hash_map:
                added_fields += 1
            hash_map[field] = value
        return added_fields


@RedisCommandsRouter.command(b"hmset", [b"write", b"hash", b"fast"])
class HashMapSetMultiple(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields_values: list[tuple[bytes, bytes]] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_or_create_hash_table(self.key)

        for field, value in self.fields_values:
            hash_map[field] = value
        return RESP_OK
