from r3dis.commands.databases import DatabaseCommand
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.consts import Commands
from r3dis.databases import Database
from r3dis.errors import RedisInvalidIntegerError
from r3dis.resp import RESP_OK

hash_map_commands_router = RedisCommandsRouter()


def apply_hash_map_increase_by(database: Database, key: bytes, field: bytes, increment: int | float):
    hash_get = database.get_or_create_hash_table(key)

    if field not in hash_get:
        hash_get[field] = 0
    if not isinstance(hash_get[field], (int, float)):
        raise RedisInvalidIntegerError
    hash_get[field] += increment
    return hash_get[field]


@hash_map_commands_router.command(Commands.HashMapIncreaseBy)
class HashMapIncreaseBy(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()
    value: int = redis_positional_parameter()

    def execute(self):
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@hash_map_commands_router.command(Commands.HashMapIncreaseByFloat)
class HashMapIncreaseByFloat(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()
    value: float = redis_positional_parameter()

    def execute(self):
        return apply_hash_map_increase_by(self.database, self.key, self.field, self.value)


@hash_map_commands_router.command(Commands.HashMapExists)
class HashMapExists(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()

    def execute(self):
        return self.field in self.database.get_hash_table(self.key)


@hash_map_commands_router.command(Commands.HashMapGet)
class HashMapGet(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()

    def execute(self):
        return self.database.get_hash_table(self.key).get(self.field)


@hash_map_commands_router.command(Commands.HashMapGetMultiple)
class HashMapGetMultiple(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields: list[bytes] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_hash_table(self.key)
        return [hash_map.get(f, None) for f in self.fields]


@hash_map_commands_router.command(Commands.HashMapGetAll)
class HashMapGetAll(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        hash_set = self.database.get_hash_table(self.key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response


@hash_map_commands_router.command(Commands.HashMapKeys)
class HashMapKeys(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return list(self.database.get_hash_table(self.key).keys())


@hash_map_commands_router.command(Commands.HashMapLength)
class HashMapLength(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return len(self.database.get_hash_table(self.key))


@hash_map_commands_router.command(Commands.HashMapValues)
class HashMapValues(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def execute(self):
        return list(self.database.get_hash_table(self.key).values())


@hash_map_commands_router.command(Commands.HashMapStringLength)
class HashMapStringLength(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    field: bytes = redis_positional_parameter()

    def execute(self):
        return len(self.database.get_hash_table(self.key).get(self.field, b""))


@hash_map_commands_router.command(Commands.HashMapDelete)
class HashMapDelete(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields: list[bytes] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_hash_table(self.key)

        return sum([1 if hash_map.pop(f, None) is not None else 0 for f in self.fields])


@hash_map_commands_router.command(Commands.HashMapSet)
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


@hash_map_commands_router.command(Commands.HashMapSetMultiple)
class HashMapSetMultiple(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    fields_values: list[tuple[bytes, bytes]] = redis_positional_parameter()

    def execute(self):
        hash_map = self.database.get_or_create_hash_table(self.key)

        for field, value in self.fields_values:
            hash_map[field] = value
        return RESP_OK
