from dataclasses import dataclass

from r3dis.commands.core import redis_argument
from r3dis.commands.databases import DatabaseCommand
from r3dis.databases import Database
from r3dis.errors import RedisInvalidIntegerError
from r3dis.resp import RESP_OK


def apply_hash_map_increase_by(database: Database, key: bytes, field: bytes, increment: int | float):
    hash_get = database.get_or_create_hash_table(key)

    if field not in hash_get:
        hash_get[field] = 0
    if not isinstance(hash_get[field], (int, float)):
        raise RedisInvalidIntegerError
    hash_get[field] += increment
    return hash_get[field]


@dataclass
class HashMapIncreaseBy(DatabaseCommand):
    key: bytes = redis_argument()
    field: bytes = redis_argument()
    value: int = redis_argument()

    def execute(self):
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@dataclass
class HashMapIncreaseByFloat(DatabaseCommand):
    key: bytes = redis_argument()
    field: bytes = redis_argument()
    value: float = redis_argument()

    def execute(self):
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@dataclass
class HashMapExists(DatabaseCommand):
    key: bytes = redis_argument()
    field: bytes = redis_argument()

    def execute(self):
        return self.field in self.database.get_hash_table(self.key)


@dataclass
class HashMapGet(DatabaseCommand):
    key: bytes = redis_argument()
    field: bytes = redis_argument()

    def execute(self):
        return self.database.get_hash_table(self.key).get(self.field)


@dataclass
class HashMapGetMultiple(DatabaseCommand):
    key: bytes = redis_argument()
    fields: list[bytes] = redis_argument()

    def execute(self):
        hash_map = self.database.get_hash_table(self.key)
        return [hash_map.get(f, None) for f in self.fields]


@dataclass
class HashMapGetAll(DatabaseCommand):
    key: bytes = redis_argument()

    def execute(self):
        hash_set = self.database.get_hash_table(self.key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response


@dataclass
class HashMapKeys(DatabaseCommand):
    key: bytes = redis_argument()

    def execute(self):
        return list(self.database.get_hash_table(self.key).keys())


@dataclass
class HashMapLength(DatabaseCommand):
    key: bytes = redis_argument()

    def execute(self):
        return len(self.database.get_hash_table(self.key))


@dataclass
class HashMapValues(DatabaseCommand):
    key: bytes = redis_argument()

    def execute(self):
        return list(self.database.get_hash_table(self.key).values())


@dataclass
class HashMapStringLength(DatabaseCommand):
    key: bytes = redis_argument()
    field: bytes = redis_argument()

    def execute(self):
        return len(self.database.get_hash_table(self.key).get(self.field, b""))


@dataclass
class HashMapDelete(DatabaseCommand):
    key: bytes = redis_argument()
    fields: list[bytes] = redis_argument()

    def execute(self):
        hash_map = self.database.get_hash_table(self.key)

        return sum([1 if hash_map.pop(f, None) is not None else 0 for f in self.fields])


@dataclass
class HashMapSet(DatabaseCommand):
    key: bytes = redis_argument()
    fields_values: list[tuple[bytes, bytes]] = redis_argument()

    def execute(self):
        hash_map = self.database.get_or_create_hash_table(self.key)

        added_fields = 0
        for field, value in self.fields_values:
            if field not in hash_map:
                added_fields += 1
            hash_map[field] = value
        return added_fields


@dataclass
class HashMapSetMultiple(DatabaseCommand):
    key: bytes = redis_argument()
    fields_values: list[tuple[bytes, bytes]] = redis_argument()

    def execute(self):
        hash_map = self.database.get_or_create_hash_table(self.key)

        for field, value in self.fields_values:
            hash_map[field] = value
        return RESP_OK
