from dataclasses import dataclass
from typing import Iterator

from r3dis.commands.core import CommandHandler
from r3dis.errors import RedisInvalidIntegerError, RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK
from r3dis.utils import chunks


@dataclass
class HashMapIncreaseBy(CommandHandler):
    float_allowed: bool = False

    def handle(self, key: bytes, field: bytes, increment: int):
        hash_get = self.database.get_or_create_hash_table(key)

        if field not in hash_get:
            hash_get[field] = 0
        if not isinstance(hash_get[field], (int, float)):
            raise RedisInvalidIntegerError
        hash_get[field] += increment
        return hash_get[field]

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)
        field = parameters.pop(0)
        try:
            increment = int(parameters.pop(0)) if not self.float_allowed else float(parameters.pop(0))
        except ValueError:
            raise RedisInvalidIntegerError

        return key, field, increment


@dataclass
class HashMapExists(CommandHandler):
    def handle(self, key: bytes, field: bytes):
        return field in self.database.get_hash_table(key)

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)
        field = parameters.pop(0)

        return key, field


@dataclass
class HashMapGet(CommandHandler):
    def handle(self, key: bytes, field: bytes):
        return self.database.get_hash_table(key).get(field)

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)
        field = parameters.pop(0)

        return key, field


@dataclass
class HashMapGetMultiple(CommandHandler):
    def handle(self, key: bytes, fields: bytes):
        hash_map = self.database.get_hash_table(key)
        return [hash_map.get(f, None) for f in fields]

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return key, parameters


@dataclass
class HashMapGetAll(CommandHandler):
    def handle(self, key: bytes):
        hash_set = self.database.get_hash_table(key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return (key,)


@dataclass
class HashMapKeys(CommandHandler):
    def handle(self, key: bytes):
        return list(self.database.get_hash_table(key).keys())

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return (key,)


@dataclass
class HashMapLength(CommandHandler):
    def handle(self, key: bytes):
        return len(self.database.get_hash_table(key))

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return (key,)


@dataclass
class HashMapValues(CommandHandler):
    def handle(self, key: bytes):
        return list(self.database.get_hash_table(key).values())

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return key


@dataclass
class HashMapStringLength(CommandHandler):
    def handle(self, key: bytes, field: bytes):
        return len(self.database.get_hash_table(key).get(field, b""))

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)
        field = parameters.pop(0)

        return key, field


@dataclass
class HashMapDelete(CommandHandler):
    def handle(self, key: bytes, fields: list[bytes]):
        hash_map = self.database.get_hash_table(key)

        return sum([1 if hash_map.pop(f, None) is not None else 0 for f in fields])

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return key, parameters


@dataclass
class HashMapSet(CommandHandler):
    def handle(self, key: bytes, fields_values: Iterator[tuple[bytes, bytes]]):
        hash_map = self.database.get_or_create_hash_table(key)

        added_fields = 0
        for field, value in fields_values:
            if field not in hash_map:
                added_fields += 1
            hash_map[field] = value
        return added_fields

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return key, chunks(parameters, 2)


@dataclass
class HashMapSetMultiple(CommandHandler):
    def handle(self, key: bytes, fields_values: Iterator[tuple[bytes, bytes]]):
        hash_map = self.database.get_or_create_hash_table(key)

        for field, value in fields_values:
            hash_map[field] = value
        return RESP_OK

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        if len(parameters) % 2:
            raise RedisWrongNumberOfArguments()

        return key, chunks(parameters, 2)
