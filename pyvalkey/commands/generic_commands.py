import fnmatch
import json
import random
import time
from typing import Any

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command, DatabaseCommand
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, KeyValue, StringType, ValkeySortedSet
from pyvalkey.database_objects.errors import ServerError, ServerWrongTypeError
from pyvalkey.resp import RESP_OK, ValueType


@ServerCommandsRouter.command(b"copy", [b"keyspace", b"write", b"slow"])
class Copy(Command):
    client_context: ClientContext = server_command_dependency()

    source: bytes = positional_parameter(key_mode=b"R")
    destination: bytes = positional_parameter(key_mode=b"W")
    replace: bool = keyword_parameter(flag=b"REPLACE", default=False)
    db: int | None = keyword_parameter(flag=b"DB", default=None)

    def execute(self) -> ValueType:
        source_key_value = self.client_context.database.get_or_none(self.source)

        if source_key_value is None:
            return False

        database = self.client_context.server_context.databases[
            self.client_context.current_database if self.db is None else self.db
        ]

        destination_key_value = database.get_or_none(self.destination)

        if destination_key_value is None:
            database.copy_from(source_key_value, self.destination)
            return True

        if not self.replace:
            return False

        database.pop(self.destination)
        database.copy_from(source_key_value, self.destination)
        return True


@ServerCommandsRouter.command(b"del", [b"keyspace", b"write", b"slow"])
class Delete(DatabaseCommand):
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return len([1 for _ in filter(None, [self.database.pop(key, None) for key in self.keys])])


@ServerCommandsRouter.command(b"dump", [b"keyspace", b"read", b"slow"])
class Dump(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None

        dump_value: dict[str, Any] = {
            "value": key_value.value,
        }
        if isinstance(key_value.value, list):
            dump_value["type"] = "list"
        elif isinstance(key_value.value, set):
            dump_value["type"] = "set"
        elif isinstance(key_value.value, dict):
            dump_value["type"] = "hash"
        elif isinstance(key_value.value, int):
            dump_value["type"] = "int"
        elif isinstance(key_value.value, ValkeySortedSet):
            dump_value = {
                "type": "sorted_set",
                "value": key_value.value.members,
            }
        elif isinstance(key_value.value, StringType):
            dump_value = {
                "type": "string",
                "value": key_value.value.value,
            }
        else:
            raise TypeError()
        return json.dumps(dump_value)


@ServerCommandsRouter.command(b"exists", [b"read", b"string", b"fast"])
class Exists(DatabaseCommand):
    keys: list[bytes] = positional_parameter(key_mode=b"RW")

    def execute(self) -> ValueType:
        return sum(1 for key in self.keys if self.database.get_or_none(key) is not None)


@ServerCommandsRouter.command(b"expire", [b"keyspace", b"write", b"fast"])
class Expire(DatabaseCommand):
    key: bytes = positional_parameter()
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration(self.key, self.seconds * 1000)


@ServerCommandsRouter.command(b"expireat", [b"keyspace", b"write", b"fast"])
class ExpireAt(DatabaseCommand):
    key: bytes = positional_parameter()
    timestamp: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_at(self.key, self.timestamp * 1000)


@ServerCommandsRouter.command(b"expiretime", [b"keyspace", b"write", b"fast"])
class Expiration(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        expiration = self.database.get_expiration(self.key)
        return expiration // 1000 if expiration is not None else None


@ServerCommandsRouter.command(b"keys", [b"keyspace", b"read", b"slow", b"dangerous"])
class Keys(DatabaseCommand):
    MAXIMUM_NESTING = 1000

    pattern: bytes = positional_parameter()

    @classmethod
    def nesting(cls, pattern: bytes) -> int:
        return sum(1 for p in pattern.split(b"*")[1:] if p)

    def execute(self) -> ValueType:
        if self.nesting(self.pattern) > self.MAXIMUM_NESTING:
            return []
        return list(fnmatch.filter(self.database.keys(), self.pattern))


@ServerCommandsRouter.command(b"migrate", [b"keyspace", b"read", b"slow", b"dangerous"])
class Migrate(DatabaseCommand):
    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"move", [b"keyspace", b"write", b"slow", b"dangerous"])
class Move(Command):
    database: Database = server_command_dependency()
    server_context: ServerContext = server_command_dependency()

    key: bytes = positional_parameter()
    db: int = positional_parameter()

    def execute(self) -> ValueType:
        key_value = self.database.pop(self.key)

        if key_value is None:
            return True

        database = self.server_context.databases[self.db]

        if database.has_key(self.key):
            return False
        database.set_key_value(key_value)

        return True


@ServerCommandsRouter.command(b"encoding", [b"read", b"keyspace", b"slow"], parent_command=b"object")
class ObjectEncoding(DatabaseCommand):
    configuration: Configurations = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"R")

    @classmethod
    def approximate_list_size(cls, value: list[bytes]) -> int:
        return sum(len(item) for item in value)

    def is_set_intset(self, value: set[bytes]) -> bool:
        if len(value) > self.configuration.set_max_intset_entries:
            return False
        if any([not item.isdigit() for item in value]):
            return False
        return True

    def is_sorted_set_listpack(self, value: ValkeySortedSet) -> bool:
        if len(value) > self.configuration.zset_max_listpack_entries:
            return False
        if max(8 + len(m) for s, m in value.members) > self.configuration.zset_max_listpack_value:
            return False
        return True

    def is_dict_listpack(self, value: dict[bytes, bytes]) -> bool:
        if len(value) > self.configuration.hash_max_listpack_entries:
            return False
        if (
            max(4 + len(k) + (len(v) if isinstance(v, bytes) else len(str(v))) for k, v in value.items())
            > self.configuration.hash_max_listpack_value
        ):
            return False
        return True

    def execute(self) -> ValueType:
        key_value = self.database.get(self.key)
        if key_value is None:
            return None
        if isinstance(key_value.value, list):
            if self.configuration.list_max_listpack_size < 0:
                if self.approximate_list_size(key_value.value) <= (
                    abs(self.configuration.list_max_listpack_size) * (4 * 1024)
                ):
                    return b"listpack"
                else:
                    return b"quicklist"
            elif len(key_value.value) <= self.configuration.list_max_listpack_size:
                return b"listpack"
            return b"quicklist"

        if isinstance(key_value.value, set):
            if self.is_set_intset(key_value.value):
                return b"intset"
            if len(key_value.value) <= self.configuration.set_max_listpack_entries:
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, ValkeySortedSet):
            if self.is_sorted_set_listpack(key_value.value):
                return b"listpack"
            return b"skiplist"

        if isinstance(key_value.value, dict):
            if self.is_dict_listpack(key_value.value):
                return b"listpack"
            return b"hashtable"

        if isinstance(key_value.value, int):
            return b"int"

        return b"raw"


@ServerCommandsRouter.command(b"persist", [b"keyspace", b"write", b"fast"])
class Persist(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        return self.database.set_persist(self.key)


@ServerCommandsRouter.command(b"pexpire", [b"keyspace", b"write", b"fast"])
class ExpireMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter()
    seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration(self.key, self.seconds)


@ServerCommandsRouter.command(b"pexpireat", [b"keyspace", b"write", b"fast"])
class ExpireAtMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter()
    timestamp: int = positional_parameter()

    def execute(self) -> ValueType:
        return self.database.set_expiration_at(self.key, self.timestamp)


@ServerCommandsRouter.command(b"pexpiretime", [b"keyspace", b"write", b"fast"])
class ExpirationMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            return None
        return self.database.get_expiration(self.key)


@ServerCommandsRouter.command(b"pttl", [b"read", b"keyspace", b"fast"])
class TimeToLiveMilliseconds(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_time_to_live(self.key)
            if expiration is None:
                return -1
            return expiration
        except KeyError:
            return -2


@ServerCommandsRouter.command(b"randomkey", [b"keyspace", b"write", b"slow", b"dangerous"])
class RandomKey(Command):
    database: Database = server_command_dependency()

    def execute(self) -> ValueType:
        if self.database.empty():
            return None

        fully_volatile = self.database.are_fully_volatile()
        max_tries = 100 if fully_volatile else None

        while True:
            random_key: bytes = random.choice(list(self.database.keys()))

            if self.database.has_key(random_key):
                break

            if max_tries is None:
                continue

            if max_tries == 0:
                break

            max_tries -= 1

        return random_key


@ServerCommandsRouter.command(b"rename", [b"keyspace", b"write", b"slow"])
class Rename(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            raise ServerError(b"ERR no such key")

        self.database.rename_unsafely(self.key, self.new_key)

        return RESP_OK


@ServerCommandsRouter.command(b"renamenx", [b"keyspace", b"write", b"slow"])
class RenameIfNotExists(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")
    new_key: bytes = positional_parameter(key_mode=b"W")

    def execute(self) -> ValueType:
        if not self.database.has_key(self.key):
            raise ServerError(b"ERR no such key")
        if self.database.has_key(self.new_key):
            return 0

        self.database.rename_unsafely(self.key, self.new_key)

        return 1


@ServerCommandsRouter.command(b"restore", [b"keyspace", b"write", b"slow", b"dangerous"])
class Restore(DatabaseCommand):
    key: bytes = positional_parameter()
    ttl: int = positional_parameter()
    serialized_value: bytes = positional_parameter()
    replace: bool = keyword_parameter(flag=b"REPLACE")
    absolute_ttl: bool = keyword_parameter(flag=b"ABSTTL")
    idle_time_seconds: bool = keyword_parameter(default=False, token=b"IDLETIME")
    frequency: bool = keyword_parameter(default=False, token=b"FREQ")

    def execute(self) -> ValueType:
        json_value: dict[str, Any] = json.loads(self.serialized_value)

        if json_value["type"] == "hash":
            value = json_value["value"]
        elif json_value["type"] == "set":
            value = set(json_value["value"])
        elif json_value["type"] == "list":
            value = json_value["value"]
        elif json_value["type"] == "sorted_set":
            value = ValkeySortedSet([(score, member) for score, member in json_value["value"]])
        elif json_value["type"] == "string":
            value = StringType(json_value["value"])
        elif json_value["type"] == "int":
            value = json_value["value"]
        else:
            raise ServerError(b"ERR DUMP payload version or checksum are wrong")

        if not self.replace and self.database.has_key(self.key):
            raise ServerError(b"BUSYKEY Target key name already exists.")

        self.database.set_key_value(
            KeyValue(
                self.key, value, (int(time.time() * 1000) + self.ttl) if not self.absolute_ttl else self.absolute_ttl
            )
        )

        return RESP_OK


@ServerCommandsRouter.command(b"scan", [b"keyspace", b"write", b"slow", b"dangerous"])
class Scan(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@ServerCommandsRouter.command(b"sort", [b"read", b"set", b"sortedset", b"list", b"slow", b"dangerous"])
class Sort(Command):
    database: Database = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = keyword_parameter(flag=b"ALPHA", default=False)
    destination: bytes | None = keyword_parameter(skip_first=True, token=b"STORE", default=None, key_mode=b"W")

    def execute(self) -> ValueType:
        result_values = SortReadOnly(
            database=self.database,
            key=self.key,
            by=self.by,
            limit=self.limit,
            get_values=self.get_values,
            descending=self.descending,
            alpha=self.alpha,
        ).internal_execute()

        if self.destination is None:
            return result_values

        if not result_values:
            self.database.pop(self.destination, None)
            return 0

        self.database.set_key_value(
            KeyValue(self.destination, [str(v).encode() if v is not None else b"" for v in result_values]),
        )
        return len(result_values)


@ServerCommandsRouter.command(b"sort_ro", [b"write", b"set", b"sortedset", b"list", b"slow", b"dangerous"])
class SortReadOnly(Command):
    database: Database = server_command_dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    by: bytes | None = keyword_parameter(token=b"BY", default=None)
    limit: tuple[int, int] | None = keyword_parameter(token=b"LIMIT", default=None)
    get_values: list[bytes] | None = keyword_parameter(multi_token=True, token=b"GET", default=None)
    descending: bool = keyword_parameter(flag={b"ASC": False, b"DESC": True}, default=False)
    alpha: bool = keyword_parameter(flag=b"ALPHA", default=False)

    def _get_referenced_value(self, value: bytes, reference: bytes) -> int | bytes | None:
        reference_key, reference_field = reference, None
        if b"->" in reference_key and not reference_key.endswith(b"->"):
            reference_key, reference_field = reference.rsplit(b"->", 1)

        key_value = self.database.get_or_none(reference_key.replace(b"*", value, 1))
        if key_value is None:
            return None
        if reference_field:
            if isinstance(key_value.value, dict):
                return key_value.value[reference_field]
            return None
        if isinstance(key_value.value, int):
            return key_value.value
        if not isinstance(key_value.value, StringType):
            return None
        return key_value.value.value

    def internal_execute(self) -> list[int | bytes | None] | None:
        key_value = self.database.get_or_none(self.key)

        if key_value is None:
            return None

        if not isinstance(key_value.value, list | set | ValkeySortedSet):
            raise ServerWrongTypeError(b"Operation against a key holding the wrong kind of value")

        values: list[bytes]
        if isinstance(key_value.value, ValkeySortedSet):
            values = [member for score, member in key_value.value.members]
        else:
            values = list(key_value.value)

        if self.by != b"nosort":
            referenced_values: dict[bytes, bytes | int] | None = None
            if self.by is not None:
                referenced_values = {}
                for value in values:
                    referenced_value = self._get_referenced_value(value, self.by)
                    if referenced_value is not None:
                        referenced_values[value] = referenced_value

            scores: dict[bytes, float | bytes] = {v: v for v in values}
            for value in values:
                if not self.alpha:
                    try:
                        scores[value] = float(
                            referenced_values.get(value, 0) if referenced_values is not None else value
                        )
                    except ValueError:
                        raise ServerError(b"ERR One or more scores can't be converted into double")
                else:
                    scores[value] = referenced_values.get(value, value) if referenced_values is not None else value

            values.sort(key=lambda v: (scores[v], v), reverse=self.descending)
        elif self.descending:
            values.reverse()

        if self.limit is not None:
            offset, count = self.limit
            values = values[offset : offset + count]

        result_values: list[int | bytes | None] = list(values)
        if self.get_values:
            get_result: list[int | bytes | None] = []
            for value in values:
                for get_value in self.get_values:
                    if get_value == b"#":
                        get_result.append(value)
                        continue
                    get_result.append(self._get_referenced_value(value, get_value))
            result_values = get_result

        return result_values

    def execute(self) -> ValueType:
        return self.internal_execute()


@ServerCommandsRouter.command(b"touch", [b"keyspace", b"write", b"slow", b"dangerous"])
class Touch(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@ServerCommandsRouter.command(b"ttl", [b"read", b"keyspace", b"fast"])
class TimeToLive(DatabaseCommand):
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        try:
            expiration = self.database.get_time_to_live(self.key)
            if expiration is None:
                return -1
            return expiration // 1000
        except KeyError:
            return -2


@ServerCommandsRouter.command(b"type", [b"keyspace", b"write", b"slow", b"dangerous"])
class Type(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@ServerCommandsRouter.command(b"unlink", [b"keyspace", b"write", b"slow", b"dangerous"])
class Unlink(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@ServerCommandsRouter.command(b"wait", [b"keyspace", b"write", b"slow", b"dangerous"])
class Wait(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK


@ServerCommandsRouter.command(b"waitaof", [b"keyspace", b"write", b"slow", b"dangerous"])
class WaitAOF(DatabaseCommand):
    def execute(self) -> ValueType:
        return RESP_OK
