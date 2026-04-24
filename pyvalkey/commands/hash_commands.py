import fnmatch
import random
from collections.abc import Iterable
from math import isinf, isnan

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import increment_bytes_value_as_float, is_floating_point, is_integer
from pyvalkey.consts import LONG_MAX, LONG_MIN
from pyvalkey.database_objects.databases import (
    HFE_NO_FIELD,
    HFE_NO_TTL,
    HFE_SET,
    Database,
)
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.utils import flatten
from pyvalkey.enums import NotificationType
from pyvalkey.notifications import NotificationsManager
from pyvalkey.resp import RESP_OK, RespProtocolVersion, ValueType
from pyvalkey.utils.dependencies import dependency
from pyvalkey.utils.times import now_ms


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
    if isinstance(previous_value, int):
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


@command(b"hdel", {b"hash"}, flags={b"fast", b"write"})
class HashMapDelete(Command):
    """
    summary: >-
      Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.
    complexity: O(N) where N is the number of fields to be removed.
    since: 2.0.0
    function: hdelCommand
    reply_schema:
      type: integer
      description: The number of fields that were removed from the hash.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    fields: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        value = self.database.hash_database.get_value_or_empty(self.key)
        result = 0
        for f in self.fields:
            if value.pop(f, None) is not None:
                self.database.hash_database.remove_field_expiration(self.key, f)
                result += 1
        return result


@command(b"hexists", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashMapExists(Command):
    """
    summary: Determines whether a field exists in a hash.
    complexity: O(1)
    since: 2.0.0
    function: hexistsCommand
    reply_schema:
      oneOf:
      - description: The hash does not contain the field, or key does not exist.
        const: 0
      - description: The hash contains the field.
        const: 1
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        return self.field in self.database.hash_database.get_value_or_empty(self.key)


@command(b"hget", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashMapGet(Command):
    """
    summary: Returns the value of a field in a hash.
    complexity: O(1)
    since: 2.0.0
    function: hgetCommand
    reply_schema:
      oneOf:
      - description: The value associated with the field.
        type: string
      - description: If the field is not present in the hash or key does not exist.
        type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        return self.database.hash_database.get_value_or_empty(self.key).get(self.field)


@command(b"hgetall", {b"hash", b"read", b"slow"}, flags={b"readonly"})
class HashMapGetAll(Command):
    """
    summary: Returns all fields and values in a hash.
    complexity: O(N) where N is the size of the hash.
    since: 2.0.0
    function: hgetallCommand
    reply_schema:
      type: object
      description: >-
        Map of fields and their values stored in the hash, or an empty list when key does not exist. In RESP2 this
        is returned as a flat array.
      additionalProperties:
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        hash_set = self.database.hash_database.get_value_or_empty(self.key)

        response = []
        for k, v in hash_set.items():
            response.extend([k, v])
        return response


@command(b"hincrby", {b"hash"}, flags={b"denyoom", b"fast", b"write"})
class HashMapIncreaseBy(Command):
    """
    summary: >-
      Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't
      exist.
    complexity: O(1)
    since: 2.0.0
    function: hincrbyCommand
    reply_schema:
      type: integer
      description: The value of the field after the increment operation.
    """

    database: Database = dependency()
    notifications: NotificationsManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    field: bytes = positional_parameter()
    value: int = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        result = apply_hash_map_increase_by(self.database, self.key, self.field, self.value)
        self.notifications.notify(NotificationType.HASH, b"hincrby", self.key)
        return result


@command(b"hincrbyfloat", {b"hash"}, flags={b"denyoom", b"fast", b"write"})
class HashMapIncreaseByFloat(Command):
    """
    summary: >-
      Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist.
    complexity: O(1)
    since: 2.6.0
    function: hincrbyfloatCommand
    reply_schema:
      type: string
      description: The value of the field after the increment operation.
    """

    database: Database = dependency()
    notifications: NotificationsManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    field: bytes = positional_parameter()
    value: float = positional_parameter()

    def execute(self) -> ValueType:
        if isnan(self.value) or isinf(self.value):
            raise ServerError(b"ERR value is NaN or Infinity")

        self.database.hash_database.evict_expired_fields(self.key)
        result = apply_hash_map_increase_by(self.database, self.key, self.field, self.value)
        self.notifications.notify(NotificationType.HASH, b"hincrbyfloat", self.key)
        return result


@command(b"hkeys", {b"hash", b"read", b"slow"}, flags={b"readonly"})
class HashMapKeys(Command):
    """
    summary: Returns all fields in a hash.
    complexity: O(N) where N is the size of the hash.
    since: 2.0.0
    function: hkeysCommand
    reply_schema:
      type: array
      description: List of fields in the hash, or an empty list when the key does not exist.
      uniqueItems: true
      items:
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        return list(self.database.hash_database.get_value_or_empty(self.key).keys())


@command(b"hlen", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashMapLength(Command):
    """
    summary: Returns the number of fields in a hash.
    complexity: O(1)
    since: 2.0.0
    function: hlenCommand
    reply_schema:
      type: integer
      description: Number of the fields in the hash, or 0 when the key does not exist.
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        return len(self.database.hash_database.get_value_or_empty(self.key))


@command(b"hmget", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashMapGetMultiple(Command):
    """
    summary: Returns the values of all fields in a hash.
    complexity: O(N) where N is the number of fields being requested.
    since: 2.0.0
    function: hmgetCommand
    reply_schema:
      description: >-
        List of values associated with the given fields, in the same order as they are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - type: string
        - type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    fields: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        hash_map = self.database.hash_database.get_value_or_empty(self.key)
        return [hash_map.get(f, None) for f in self.fields]


@command(b"hmset", {b"hash"}, flags={b"denyoom", b"fast", b"write"})
class HashMapSetMultiple(Command):
    """
    summary: Sets the values of multiple fields.
    complexity: O(N) where N is the number of fields being set.
    since: 2.0.0
    function: hsetCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    fields_values: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        for field, value in self.fields_values:
            hash_map[field] = value
            self.database.hash_database.remove_field_expiration(self.key, field)

        self.database.notify(NotificationType.HASH, b"hset", self.key)
        return RESP_OK


@command(b"hrandfield", {b"hash", b"read", b"slow"}, flags={b"readonly"})
class HashRandomField(Command):
    """
    summary: Returns one or more random fields from a hash.
    complexity: O(N) where N is the number of fields returned
    since: 6.2.0
    function: hrandfieldCommand
    reply_schema:
      anyOf:
      - description: Key doesn't exist
        type: 'null'
      - description: A single random field. Returned in case `COUNT` was not used.
        type: string
      - description: A list of fields. Returned in case `COUNT` was used.
        type: array
        items:
          type: string
      - description: >-
          Fields and their values. Returned in case `COUNT` and `WITHVALUES` were used. In RESP2 this is returned
          as a flat array.
        type: array
        items:
          type: array
          minItems: 2
          maxItems: 2
          items:
          - description: Field
            type: string
          - description: Value
            type: string
    """

    database: Database = dependency()
    protocol: RespProtocolVersion = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    count: int | None = positional_parameter(default=None)
    with_values: bool = flag_parameter(token=b"WITHVALUES")

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
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


@command(b"hscan", {b"hash", b"read", b"slow"}, flags={b"readonly"})
class HashMapScan(Command):
    """
    summary: Iterates over fields and values of a hash.
    complexity: >-
      O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return
      back to 0. N is the number of elements inside the collection.
    since: 2.8.0
    function: hscanCommand
    reply_schema:
      description: Cursor and scan response in array form.
      type: array
      minItems: 2
      maxItems: 2
      items:
      - description: Cursor.
        type: string
      - description: >-
          List of key/value pairs from the hash where each even element is the key, and each odd element is the
          value, or when novalues option is on, a list of keys from the hash.
        type: array
        items:
          type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    cursor: int = positional_parameter()
    match: bytes | None = keyword_parameter(token=b"MATCH", default=None)
    count: int | None = keyword_parameter(token=b"COUNT", default=None)
    no_values: bool = flag_parameter(token=b"NOVALUES")

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        value = self.database.hash_database.get_value_or_empty(self.key)

        def scan() -> Iterable[tuple[bytes, bytes | int]]:
            selected = 0
            for k, v in value.items():
                if self.count is not None and selected >= self.count:
                    break
                if self.match is not None and not fnmatch.fnmatch(k, self.match):
                    continue
                yield k, v
                selected += 1

        if self.no_values:
            return [b"0", [k for k, v in scan()]]
        return [b"0", dict(scan())]


@command(b"hset", {b"hash"}, flags={b"denyoom", b"fast", b"write"})
class HashMapSet(Command):
    """
    summary: Creates or modifies the value of a field in a hash.
    complexity: >-
      O(1) for each field/value pair added, so O(N) to add N field/value pairs when the command is called with multiple
      field/value pairs.
    since: 2.0.0
    function: hsetCommand
    reply_schema:
      description: The number of fields that were added
      type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    fields_values: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        added_fields = 0
        for field, value in self.fields_values:
            if field not in hash_map:
                added_fields += 1
            hash_map[field] = value
            self.database.hash_database.remove_field_expiration(self.key, field)
        if added_fields:
            self.database.notify(NotificationType.HASH, b"hset", self.key)
        return added_fields


@command(b"hsetnx", {b"hash"}, flags={b"denyoom", b"fast", b"write"})
class HashMapSetIfNotExists(Command):
    """
    summary: Sets the value of a field in a hash only when the field doesn't exist.
    complexity: O(1)
    since: 2.0.0
    function: hsetnxCommand
    reply_schema:
      oneOf:
      - description: The field is a new field in the hash and value was set.
        const: 0
      - description: The field already exists in the hash and no operation was performed.
        const: 1
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    field: bytes = positional_parameter()
    value: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        hash_map = self.database.hash_database.get_value_or_create(self.key)

        if self.field in hash_map:
            return False
        hash_map[self.field] = self.value
        self.database.hash_database.remove_field_expiration(self.key, self.field)
        return True


@command(b"hstrlen", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashMapStringLength(Command):
    """
    summary: Returns the length of the value of a field.
    complexity: O(1)
    since: 3.2.0
    function: hstrlenCommand
    reply_schema:
      type: integer
      description: >-
        String length of the value associated with the field, or zero when the field is not present in the hash
        or key does not exist at all.
      minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    field: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        value = self.database.hash_database.get_value_or_empty(self.key)
        field_value = value.get(self.field, b"")
        if isinstance(field_value, int):
            field_value = str(field_value).encode()
        return len(field_value)


@command(b"hvals", {b"hash", b"read", b"slow"}, flags={b"readonly"})
class HashMapValues(Command):
    """
    summary: Returns all values in a hash.
    complexity: O(N) where N is the size of the hash.
    since: 2.0.0
    function: hvalsCommand
    reply_schema:
      type: array
      description: List of values in the hash, or an empty list when the key does not exist.
      items:
        type: string
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        self.database.hash_database.evict_expired_fields(self.key)
        return list(self.database.hash_database.get_value_or_empty(self.key).values())


_FIELD_TTL_CONDITIONS = (b"NX", b"XX", b"GT", b"LT")


def _parse_fields_tail(args: list[bytes]) -> tuple[bytes | None, list[bytes]]:
    if not args:
        raise ServerError(b"ERR syntax error")
    cond: bytes | None = None
    idx = 0
    first_upper = args[0].upper()
    if first_upper in _FIELD_TTL_CONDITIONS:
        cond = first_upper
        idx += 1
    if idx >= len(args) or args[idx].upper() != b"FIELDS":
        raise ServerError(b"ERR Mandatory keyword FIELDS is missing or not at the right position")
    idx += 1
    if idx >= len(args):
        raise ServerError(b"ERR syntax error")
    try:
        numfields = int(args[idx])
    except ValueError as e:
        raise ServerError(b"ERR numfields should be greater than 0") from e
    idx += 1
    if numfields <= 0:
        raise ServerError(b"ERR numfields should be greater than 0")
    fields = args[idx:]
    if len(fields) != numfields:
        raise ServerError(b"ERR Parameter `numFields` should be equal to the number of fields")
    return cond, fields


@command(b"hexpire", {b"hash"}, flags={b"fast", b"write"})
class HashExpire(Command):
    """
    summary: Sets expiry time on hash fields.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hexpireCommand
    reply_schema:
      description: >-
        List of integer codes indicating the result of setting expiry on each specified field, in the same order
        as the fields are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the HASH, or key does not exist.
          const: -2
        - description: The specified NX | XX | GT | LT condition has not been met.
          const: 0
        - description: The expiration time was applied.
          const: 1
        - description: When called with a 0 second
          const: 2
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    seconds: int = positional_parameter()
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        cond, fields = _parse_fields_tail(self.tail)
        at_ms = now_ms() + self.seconds * 1000
        return self.database.hash_database.apply_field_expirations(self.key, at_ms, cond, fields)


@command(b"hpexpire", {b"hash"}, flags={b"fast", b"write"})
class HashPExpire(Command):
    """
    summary: Sets expiry time on hash object.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hpexpireCommand
    reply_schema:
      description: >-
        List of integer codes indicating the result of setting expiry on each specified field, in the same order
        as the fields are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the HASH, or HASH is empty.
          const: -2
        - description: The specified NX | XX | GT | LT condition has not been met.
          const: 0
        - description: The expiration time was applied.
          const: 1
        - description: When called with a 0 millisecond
          const: 2
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    milliseconds: int = positional_parameter()
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        cond, fields = _parse_fields_tail(self.tail)
        at_ms = now_ms() + self.milliseconds
        return self.database.hash_database.apply_field_expirations(self.key, at_ms, cond, fields)


@command(b"hexpireat", {b"hash"}, flags={b"fast", b"write"})
class HashExpireAt(Command):
    """
    summary: Sets expiry time on hash fields.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hexpireatCommand
    reply_schema:
      description: >-
        List of integer codes indicating the result of setting expiry on each specified field, in the same order
        as the fields are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the HASH, or HASH is empty.
          const: -2
        - description: The specified NX | XX | GT | LT condition has not been met.
          const: 0
        - description: The expiration time was applied.
          const: 1
        - description: When called with a 0 second or is called with a past Unix time in seconds.
          const: 2
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    unix_seconds: int = positional_parameter()
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        cond, fields = _parse_fields_tail(self.tail)
        at_ms = self.unix_seconds * 1000
        return self.database.hash_database.apply_field_expirations(self.key, at_ms, cond, fields)


@command(b"hpexpireat", {b"hash"}, flags={b"fast", b"write"})
class HashPExpireAt(Command):
    """
    summary: Sets expiration time on hash field.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hpexpireatCommand
    reply_schema:
      description: >-
        List of integer codes indicating the result of setting expiry on each specified field, in the same order
        as the fields are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the HASH, or HASH is empty.
          const: -2
        - description: The specified NX | XX | GT | LT condition has not been met.
          const: 0
        - description: The expiration time was applied.
          const: 1
        - description: When called with a 0 second or is called with a past Unix time in milliseconds.
          const: 2
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    unix_milliseconds: int = positional_parameter()
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        cond, fields = _parse_fields_tail(self.tail)
        return self.database.hash_database.apply_field_expirations(self.key, self.unix_milliseconds, cond, fields)


@command(b"httl", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashTTL(Command):
    """
    summary: >-
      Returns the remaining time to live (in seconds) of a hash key's field(s) that have an associated expiration.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: httlCommand
    reply_schema:
      description: >-
        List of values associated with the result of getting the remaining time-to-live of the specific fields,
        in the same order as they are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the provided hash key, or the hash key is empty
          const: -2
        - description: Field exists in the provided hash key, but has no expiration associated with it.
          const: -1
        - description: The expiration time associated with the hash key field, in seconds.
          type: integer
          minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        _, fields = _parse_fields_tail(self.tail)
        now = now_ms()
        return self.database.hash_database.query_field_expirations(
            self.key, fields, lambda e: max(0, (e - now) // 1000)
        )


@command(b"hpttl", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashPTTL(Command):
    """
    summary: >-
      Returns the remaining time to live in milliseconds of a hash key's field(s) that have an associated expiration.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hpttlCommand
    reply_schema:
      description: >-
        List of values associated with the result of getting the remaining time-to-live of the specific fields,
        in the same order as they are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the provided hash key, or the hash key is empty
          const: -2
        - description: Field exists in the provided hash key, but has no expiration associated with it.
          const: -1
        - description: The expiration time associated with the hash key field, in milliseconds.
          type: integer
          minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        _, fields = _parse_fields_tail(self.tail)
        now = now_ms()
        return self.database.hash_database.query_field_expirations(self.key, fields, lambda e: max(0, e - now))


@command(b"hexpiretime", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashExpireTime(Command):
    """
    summary: >-
      Returns Unix timestamps in seconds since the epoch at which the given key's field(s) will expire.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hexpiretimeCommand
    reply_schema:
      description: >-
        List of values associated with the result of getting the absolute expiry timestamp of the specific fields,
        in the same order as they are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the provided hash key, or the hash key is empty.
          const: -2
        - description: Field exists in the provided hash key, but has no expiration associated with it.
          const: -1
        - description: The expiration time associated with the hash key field, in seconds.
          type: integer
          minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        _, fields = _parse_fields_tail(self.tail)
        return self.database.hash_database.query_field_expirations(self.key, fields, lambda e: e // 1000)


@command(b"hpexpiretime", {b"hash", b"read"}, flags={b"fast", b"readonly"})
class HashPExpireTime(Command):
    """
    summary: >-
      Returns the Unix timestamp in milliseconds since Unix epoch at which the given key's field(s) will expire.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hpexpiretimeCommand
    reply_schema:
      description: >-
        List of values associated with the result of getting the absolute expiry timestamp of the specific fields,
        in the same order as they are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the provided hash key, or the hash key is empty.
          const: -2
        - description: Field exists in the provided hash key, but has no expiration associated with it.
          const: -1
        - description: The expiration time associated with the hash key field, in milliseconds.
          type: integer
          minimum: 0
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        _, fields = _parse_fields_tail(self.tail)
        return self.database.hash_database.query_field_expirations(self.key, fields, lambda e: e)


@command(b"hpersist", {b"hash"}, flags={b"fast", b"write"})
class HashPersist(Command):
    """
    summary: Remove the existing expiration on a hash key's field(s).
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hpersistCommand
    reply_schema:
      description: >-
        List of integer codes indicating the result of setting expiry on each specified field, in the same order
        as the fields are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - description: Field does not exist in the provided hash key, or the hash key does not exist.
          const: -2
        - description: Field exists in the provided hash key, but has no expiration associated with it.
          const: -1
        - description: The expiration time was removed from the hash key field.
          const: 1
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    tail: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        _, fields = _parse_fields_tail(self.tail)
        self.database.hash_database.evict_expired_fields(self.key)
        if not self.database.hash_database.has_key(self.key):
            return [HFE_NO_FIELD for _ in fields]
        hash_map = self.database.hash_database.get_value(self.key)
        results: list[int] = []
        for f in fields:
            if f not in hash_map:
                results.append(HFE_NO_FIELD)
                continue
            if self.database.hash_database.get_field_expiration(self.key, f) is None:
                results.append(HFE_NO_TTL)
                continue
            self.database.hash_database.remove_field_expiration(self.key, f)
            results.append(HFE_SET)
        return results


def _parse_expire_option(args: list[bytes], idx: int) -> tuple[int | None, bool, int]:
    """Returns (at_ms or None, persist_flag, new_index). Stops before FIELDS."""
    if idx >= len(args):
        return None, False, idx
    tok = args[idx].upper()
    if tok == b"PERSIST":
        return None, True, idx + 1
    if tok in (b"EX", b"PX", b"EXAT", b"PXAT"):
        if idx + 1 >= len(args):
            raise ServerError(b"ERR syntax error")
        try:
            n = int(args[idx + 1])
        except ValueError as e:
            raise ServerError(b"ERR value is not an integer or out of range") from e
        if tok == b"EX":
            return now_ms() + n * 1000, False, idx + 2
        if tok == b"PX":
            return now_ms() + n, False, idx + 2
        if tok == b"EXAT":
            return n * 1000, False, idx + 2
        return n, False, idx + 2
    return None, False, idx


@command(b"hgetex", {b"hash"}, flags={b"fast", b"write"})
class HashGetEx(Command):
    """
    summary: >-
      Gets the value of one or more fields of a given hash key, and optionally sets their expiration time or
      time-to-live (TTL).
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hgetexCommand
    reply_schema:
      description: >-
        List of values associated with the given fields, in the same order as they are requested.
      type: array
      minItems: 1
      items:
        oneOf:
        - type: string
        - type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        at_ms, persist, idx = _parse_expire_option(self.args, 0)
        if idx >= len(self.args) or self.args[idx].upper() != b"FIELDS":
            raise ServerError(b"ERR Mandatory keyword FIELDS is missing or not at the right position")
        _, fields = _parse_fields_tail(self.args[idx:])
        self.database.hash_database.evict_expired_fields(self.key)
        if not self.database.hash_database.has_key(self.key):
            return [None for _ in fields]
        hash_map = self.database.hash_database.get_value(self.key)
        now = now_ms()
        results: list[bytes | int | None] = []
        for f in fields:
            if f not in hash_map:
                results.append(None)
                continue
            results.append(hash_map[f])
            if persist:
                self.database.hash_database.remove_field_expiration(self.key, f)
            elif at_ms is not None:
                if at_ms <= now:
                    hash_map.pop(f, None)
                    self.database.hash_database.remove_field_expiration(self.key, f)
                else:
                    self.database.hash_database.set_field_expiration(self.key, f, at_ms)
        if not hash_map:
            self.database.hash_database.pop_unsafely(self.key)
        return results


@command(b"hgetdel", {b"hash"}, flags={b"fast", b"write"})
class HashGetDel(Command):
    """
    summary: Returns the values of one or more fields and deletes them from a hash.
    complexity: O(N) where N is the number of fields to be retrieved and deleted.
    since: 9.1.0
    function: hgetdelCommand
    reply_schema:
      description: >-
        List of values associated with the given fields, in the same order as they are requested. Returns nil for
        fields that do not exist.
      type: array
      minItems: 1
      items:
        oneOf:
        - type: string
        - type: 'null'
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if not self.args or self.args[0].upper() != b"FIELDS":
            raise ServerError(b"ERR Mandatory keyword FIELDS is missing or not at the right position")
        _, fields = _parse_fields_tail(self.args)
        self.database.hash_database.evict_expired_fields(self.key)
        if not self.database.hash_database.has_key(self.key):
            return [None for _ in fields]
        hash_map = self.database.hash_database.get_value(self.key)
        results: list[bytes | int | None] = []
        for f in fields:
            if f not in hash_map:
                results.append(None)
                continue
            results.append(hash_map.pop(f))
            self.database.hash_database.remove_field_expiration(self.key, f)
        if not hash_map:
            self.database.hash_database.pop_unsafely(self.key)
        return results


@command(b"hsetex", {b"hash"}, flags={b"denyoom", b"fast", b"write"})
class HashSetEx(Command):
    """
    summary: >-
      Sets the value of one or more fields of a given hash key, and optionally sets their expiration time.
    complexity: O(N) where N is the number of specified fields.
    since: 9.0.0
    function: hsetexCommand
    reply_schema:
      oneOf:
      - description: None of the provided fields value and or expiration time was set.
        const: 0
      - description: All the fields value and or expiration time was set.
        const: 1
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"RW")
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        # HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT <value>|KEEPTTL] FIELDS numfields field value [field value ...]
        idx = 0
        field_cond: bytes | None = None
        at_ms: int | None = None
        keep_ttl = False
        while idx < len(self.args):
            tok = self.args[idx].upper()
            if tok in (b"FNX", b"FXX"):
                if field_cond is not None:
                    raise ServerError(b"ERR syntax error")
                field_cond = tok
                idx += 1
                continue
            if tok == b"KEEPTTL":
                keep_ttl = True
                idx += 1
                continue
            if tok in (b"EX", b"PX", b"EXAT", b"PXAT"):
                if at_ms is not None or keep_ttl:
                    raise ServerError(b"ERR syntax error")
                if idx + 1 >= len(self.args):
                    raise ServerError(b"ERR syntax error")
                try:
                    n = int(self.args[idx + 1])
                except ValueError as e:
                    raise ServerError(b"ERR value is not an integer or out of range") from e
                if tok == b"EX":
                    at_ms = now_ms() + n * 1000
                elif tok == b"PX":
                    at_ms = now_ms() + n
                elif tok == b"EXAT":
                    at_ms = n * 1000
                else:
                    at_ms = n
                idx += 2
                continue
            break

        if idx >= len(self.args) or self.args[idx].upper() != b"FIELDS":
            raise ServerError(b"ERR Mandatory keyword FIELDS is missing or not at the right position")
        idx += 1
        if idx >= len(self.args):
            raise ServerError(b"ERR syntax error")
        try:
            numfields = int(self.args[idx])
        except ValueError as e:
            raise ServerError(b"ERR value is not an integer or out of range") from e
        idx += 1
        pairs = self.args[idx:]
        if numfields <= 0 or len(pairs) != numfields * 2:
            raise ServerError(b"ERR wrong number of arguments for HSETEX")
        items = [(pairs[i], pairs[i + 1]) for i in range(0, len(pairs), 2)]

        self.database.hash_database.evict_expired_fields(self.key)
        if field_cond is not None:
            existing = self.database.hash_database.get_value_or_empty(self.key)
            all_present = all(f in existing for f, _ in items)
            any_present = any(f in existing for f, _ in items)
            if field_cond == b"FNX" and any_present:
                return 0
            if field_cond == b"FXX" and not all_present:
                return 0

        hash_map = self.database.hash_database.get_value_or_create(self.key)
        for f, v in items:
            hash_map[f] = v
            if keep_ttl:
                continue
            if at_ms is not None:
                self.database.hash_database.set_field_expiration(self.key, f, at_ms)
            else:
                self.database.hash_database.remove_field_expiration(self.key, f)
        self.database.notify(NotificationType.HASH, b"hset", self.key)
        return 1
