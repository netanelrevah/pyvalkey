import math
from dataclasses import field
from typing import cast

from pyvalkey.blocking import StreamBlockingManager, StreamWaitingContext
from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import flag_parameter, keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata, parameters_object
from pyvalkey.commands.router import command
from pyvalkey.commands.stream_commands_data import XAUTOCLAIM_REPLY_SCHEMA, XINFO_STREAM_REPLY_SCHEMA
from pyvalkey.commands.utils import _parse_entry_id, format_entry_id, parse_strict_entry_id
from pyvalkey.consts import LONG_MAX, UINT64_MAX
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.stream import (
    Consumer,
    ConsumerGroup,
    EntryID,
    PendingEntry,
    Stream,
    range_entries,
)
from pyvalkey.enums import NotificationType
from pyvalkey.resp import RESP_OK, ValueType
from pyvalkey.utils.dependencies import dependency
from pyvalkey.utils.times import now_ms


@parameters_object
class MaxLength:
    equal: bool = flag_parameter(token=b"=")
    approximate: bool = flag_parameter(token=b"~")
    threshold: int = positional_parameter()
    limit: int | None = keyword_parameter(flag=b"LIMIT", default=None)


@parameters_object
class MinimumId:
    equal: bool = flag_parameter(token=b"=")
    approximate: bool = flag_parameter(token=b"~")
    threshold: bytes = positional_parameter()
    limit: int | None = keyword_parameter(flag=b"LIMIT", default=None)


@command(b"xtrim", {b"slow", b"stream"}, flags={b"write"})
class StreamTrim(Command):
    """
    summary: Deletes messages from the beginning of a stream.
    complexity: >-
      O(N), with N being the number of evicted entries. Constant times are very small however, since entries are
      organized in macro nodes containing multiple entries that can be released with a single deallocation.
    since: 5.0.0
    function: xtrimCommand
    reply_schema:
      description: The number of entries deleted from the stream.
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    configuration: Configurations = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    no_make_stream: bool = flag_parameter(token=b"NOMKSTREAM")
    maximum_length: MaxLength | None = keyword_parameter(token=b"MAXLEN", default=None)
    minimum_id: MinimumId | None = keyword_parameter(token=b"MINID", default=None)

    @classmethod
    def trim(
        cls,
        value: Stream,
        maximum_length: MaxLength | None = None,
        minimum_id: MinimumId | None = None,
        node_max_entries: int = 100,
    ) -> int:
        deleted = 0
        if maximum_length is not None:
            deleted = value.trim_maximum_length(
                maximum_length.threshold, maximum_length.approximate, maximum_length.limit, node_max_entries
            )
        if minimum_id is not None:
            try:
                threshold_timestamp, threshold_sequence = _parse_entry_id(minimum_id.threshold)
                if threshold_timestamp is None:
                    raise ValueError("threshold_timestamp cannot be None")
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

            deleted = value.trim_minimum_id(
                threshold_timestamp, threshold_sequence, minimum_id.approximate, minimum_id.limit, node_max_entries
            )
        return deleted

    def execute(self) -> ValueType:
        if self.minimum_id is not None and self.maximum_length is not None:
            raise ServerError(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible")

        if self.minimum_id is not None or self.maximum_length is not None:
            limiter = cast("MaxLength | MinimumId", self.maximum_length or self.minimum_id)

            if limiter.equal and limiter.approximate:
                raise ServerError(b"ERR value is not an integer or out of range")

            if not limiter.approximate:
                limiter.equal = True

            if limiter.limit is None:
                if limiter.approximate:
                    limiter.limit = self.configuration.stream_node_max_entries * 100
                    if limiter.limit <= 0:
                        limiter.limit = 10000
                    limiter.limit = min(limiter.limit, 1000000)
            else:
                if limiter.equal:
                    raise ServerError(b"ERR syntax error, LIMIT cannot be used without the special ~ option")
                if limiter.limit < 0:
                    raise ServerError(b"ERR The LIMIT argument must be >= 0.")

        if self.no_make_stream and not self.database.stream_database.has_key(self.key):
            return None

        value = self.database.stream_database.get_value_or_create(self.key)

        if len(value) > 0:
            last_entry_timestamp, last_entry_sequence = value.last_id
            if last_entry_timestamp >= UINT64_MAX or last_entry_sequence >= UINT64_MAX:
                raise ServerError(b"ERR The stream has exhausted the last possible ID, unable to add more items")

        return self.trim(value, self.maximum_length, self.minimum_id, self.configuration.stream_node_max_entries)


@command(b"xadd", {b"stream"}, flags={b"denyoom", b"fast", b"write"})
class StreamAdd(Command):
    """
    summary: Appends a new message to a stream. Creates the key if it doesn't exist.
    complexity: >-
      O(1) when adding a new entry, O(N) when trimming where N being the number of entries evicted.
    since: 5.0.0
    function: xaddCommand
    reply_schema:
      oneOf:
      - description: >-
          The ID of the added entry. The ID is the one auto-generated if * is passed as ID argument, otherwise the
          command just returns the same ID specified by the user during insertion.
        type: string
        pattern: '[0-9]+-[0-9]+'
      - description: The NOMKSTREAM option is given and the key doesn't exist.
        type: 'null'
    """

    database: Database = dependency()
    configuration: Configurations = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    no_make_stream: bool = flag_parameter(token=b"NOMKSTREAM")
    maximum_length: MaxLength | None = keyword_parameter(token=b"MAXLEN", default=None)
    minimum_id: MinimumId | None = keyword_parameter(token=b"MINID", default=None)
    stream_id: bytes = positional_parameter()
    field_value: list[tuple[bytes, bytes]] = positional_parameter(sequence_allow_empty=False)

    _entry_id: EntryID | None = field(init=False, default=None)

    def execute(self) -> ValueType:
        if self.minimum_id is not None and self.maximum_length is not None:
            raise ServerError(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible")

        if self.minimum_id is not None or self.maximum_length is not None:
            limiter = cast("MaxLength | MinimumId", self.maximum_length or self.minimum_id)

            if limiter.equal and limiter.approximate:
                raise ServerError(b"ERR value is not an integer or out of range")

            if not limiter.approximate:
                limiter.equal = True
                if limiter.limit is not None:
                    raise ServerError(b"syntax error, LIMIT cannot be used without the special ~ option")
                else:
                    limiter.limit = self.configuration.stream_node_max_entries * 100
            elif limiter.limit is not None and limiter.limit < 0:
                raise ServerError(b"ERR The LIMIT argument must be >= 0.")

        if self.no_make_stream and not self.database.stream_database.has_key(self.key):
            return None

        try:
            entry_timestamp, entry_sequence = _parse_entry_id(self.stream_id)
        except ValueError:
            raise ServerError(b"ERR Invalid stream ID specified as stream command argument")
        if entry_timestamp == 0 and entry_sequence == 0:
            raise ServerError(b"ERR The ID specified in XADD must be greater than 0-0")

        value = self.database.stream_database.get_value_or_create(self.key)

        if len(value) > 0:
            last_entry_timestamp, last_entry_sequence = value.last_id
            if last_entry_timestamp == UINT64_MAX and last_entry_sequence == UINT64_MAX:
                raise ServerError(b"ERR The stream has exhausted the last possible ID, unable to add more items")

        self._entry_id = value.generate_entry_id(entry_timestamp=entry_timestamp, entry_sequence=entry_sequence)
        if (
            entry_sequence is None and value.last_id == (self._entry_id[0], UINT64_MAX)
        ) or self._entry_id <= value.last_id:
            raise ServerError(b"ERR The ID specified in XADD is equal or smaller than the target stream top item")
        try:
            value.add(
                self.field_value,
                self._entry_id,
            )
        except ValueError:
            raise ServerError(b"ERR Elements are too large to be stored")

        StreamTrim.trim(value, self.maximum_length, self.minimum_id, self.configuration.stream_node_max_entries)

        self.database.notify(NotificationType.STREAM, b"xadd", self.key)

        return format_entry_id(self._entry_id)

    async def after(self, in_multi: bool = False) -> None:
        if self._entry_id is not None:
            await self.blocking_manager.notify(self.key, in_multi=in_multi)


@command(b"help", {b"slow", b"stream"}, parent_command=b"xgroup", flags={b"loading", b"stale"})
class StreamGroupHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: xgroupCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"CREATE <key> <groupname> <id|$> [option]",
            b"    Create a new consumer group. Options are:",
            b"    * MKSTREAM",
            b"      Create the empty stream if it does not exist.",
            b"    * ENTRIESREAD entries_read",
            b"      Set the group's entries_read counter (internal use).",
            b"CREATECONSUMER <key> <groupname> <consumer>",
            b"    Create a new consumer in the specified group.",
            b"DELCONSUMER <key> <groupname> <consumer>",
            b"    Remove the specified consumer.",
            b"DESTROY <key> <groupname>",
            b"    Remove the specified group.",
            b"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
            b"    Set the current group ID and entries_read counter.",
        ]


@command(b"help", {b"slow", b"stream"}, parent_command=b"xinfo", flags={b"loading", b"stale"})
class StreamInfoHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: xinfoCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"STREAM",
            b"Get information about the stream",
            b"GROUPS",
            b"Get information about groups in the stream",
            b"CONSUMERS",
            b"Get information about consumers in the stream",
            b"CONSUMER",
            b"Get information about a specific consumer in the stream",
            b"HELP",
            b"Get help about stream commands",
        ]


@command(b"xlen", {b"read", b"stream"}, flags={b"fast", b"readonly"})
class StreamLength(Command):
    """
    summary: Returns the number of messages in a stream.
    complexity: O(1)
    since: 5.0.0
    function: xlenCommand
    reply_schema:
      description: The number of entries of the stream at key
      type: integer
      minimum: 0
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        return len(self.database.stream_database.get_value_or_empty(self.key))


@command(b"xdel", {b"stream"}, flags={b"fast", b"write"})
class StreamDelete(Command):
    """
    summary: Returns the number of messages after removing them from a stream.
    complexity: >-
      O(1) for each single item to delete in the stream, regardless of the stream size.
    since: 5.0.0
    function: xdelCommand
    reply_schema:
      description: The number of entries actually deleted
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    ids: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_empty(self.key)

        deleted_count = 0
        for stream_id in self.ids:
            try:
                timestamp, sequence = parse_strict_entry_id(stream_id)

            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

            deleted = value.delete(timestamp, sequence)
            if deleted:
                deleted_count += 1

        return deleted_count

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)


def _parse_range(start: bytes, end: bytes) -> tuple[int | None, int | None, int | None, int | None, bool, bool]:
    if start in {b"(-", b"+"}:
        raise ServerError(b"ERR invalid start ID for the interval")
    if end in {b"(+", b"-"}:
        raise ServerError(b"ERR invalid end ID for the interval")

    minimum_inclusive = not start.startswith(b"(")
    maximum_inclusive = not end.startswith(b"(")

    minimum_timestamp = None
    minimum_sequence = None
    if start != b"-":
        minimum_entry = start.strip(b"(").split(b"-")
        if len(minimum_entry) == 1:
            minimum_timestamp = int(minimum_entry[0])
        else:
            minimum_timestamp = int(minimum_entry[0])
            minimum_sequence = int(minimum_entry[1])

    if not minimum_inclusive and minimum_sequence == UINT64_MAX and minimum_timestamp == UINT64_MAX:
        raise ServerError(b"ERR invalid start ID for the interval")

    maximum_timestamp = None
    maximum_sequence = None
    if end != b"+":
        maximum_entry = end.strip(b"(").split(b"-")
        if len(maximum_entry) == 1:
            maximum_timestamp = int(maximum_entry[0])
        else:
            maximum_timestamp = int(maximum_entry[0])
            maximum_sequence = int(maximum_entry[1])

    if not maximum_inclusive and maximum_timestamp == 0 and maximum_sequence == 0:
        raise ServerError(b"ERR invalid end ID for the interval")

    return (
        minimum_timestamp,
        minimum_sequence,
        maximum_timestamp,
        maximum_sequence,
        minimum_inclusive,
        maximum_inclusive,
    )


@command(b"xrange", {b"read", b"slow", b"stream"}, flags={b"readonly"})
class StreamRange(Command):
    """
    summary: Returns the messages from a stream within a range of IDs.
    complexity: >-
      O(N) with N being the number of elements being returned. If N is constant (e.g. always asking for the first
      10 elements with COUNT), you can consider it O(1).
    since: 5.0.0
    function: xrangeCommand
    reply_schema:
      description: Stream entries with IDs matching the specified range.
      type: array
      uniqueItems: true
      items:
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Entry ID
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Data
          type: array
          items:
            type: string
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    start: bytes = positional_parameter()
    end: bytes = positional_parameter()
    count: int | None = keyword_parameter(flag=b"COUNT", default=None)

    @classmethod
    def range(
        cls,
        value: Stream,
        start: bytes,
        end: bytes,
        count: int | None = None,
        is_reversed: bool = False,
    ) -> ValueType:
        entries = value.range(
            *_parse_range(start, end),
            count=count,
            is_reversed=is_reversed,
        )
        return [[format_entry_id(entry_id), entry_data] for entry_id, entry_data in entries]

    def execute(self) -> ValueType:
        return self.range(self.database.stream_database.get_value_or_empty(self.key), self.start, self.end, self.count)


@command(b"xrevrange", {b"read", b"slow", b"stream"}, flags={b"readonly"})
class StreamReversedRange(Command):
    """
    summary: Returns the messages from a stream within a range of IDs in reverse order.
    complexity: >-
      O(N) with N being the number of elements returned. If N is constant (e.g. always asking for the first 10 elements
      with COUNT), you can consider it O(1).
    since: 5.0.0
    function: xrevrangeCommand
    reply_schema:
      description: An array of the entries with IDs matching the specified range
      type: array
      items:
        type: array
        minItems: 2
        maxItems: 2
        items:
        - description: Stream id
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Array of field-value pairs
          type: array
          items:
            type: string
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    end: bytes = positional_parameter()
    start: bytes = positional_parameter()
    count: int | None = keyword_parameter(flag=b"COUNT", default=None)

    def execute(self) -> ValueType:
        return StreamRange.range(
            self.database.stream_database.get_value_or_empty(self.key),
            self.start,
            self.end,
            self.count,
            is_reversed=True,
        )


@command(b"create", {b"slow", b"stream"}, b"xgroup", flags={b"denyoom", b"write"})
class StreamGroupCreate(Command):
    """
    summary: Creates a consumer group.
    complexity: O(1)
    since: 5.0.0
    function: xgroupCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = positional_parameter()

    stream_id: bytes = positional_parameter()
    make_stream: bool = flag_parameter(token=b"MKSTREAM")
    entries_read: int = keyword_parameter(token=b"ENTRIESREAD", default=-1)

    def execute(self) -> ValueType:
        if self.make_stream:
            value = self.database.stream_database.get_value_or_create(self.key)
        else:
            if not self.database.stream_database.has_key(self.key):
                raise ServerError(
                    b"ERR The XGROUP subcommand requires the key to exist. "
                    b"Note that for CREATE you may want to use the MKSTREAM "
                    b"option to create an empty stream automatically."
                )
            value = self.database.stream_database.get_value(self.key)

        if self.group in value.consumer_groups:
            raise ServerError(b"BUSYGROUP Consumer Group name already exists")

        if self.stream_id == b"$":
            entry_id = value.last_id
        else:
            entry_id = parse_strict_entry_id(self.stream_id, sequence_fill=0)

        if self.entries_read is not None and self.entries_read < -1:
            raise ServerError(b"ERR value for ENTRIESREAD must be positive or -1")

        value.consumer_groups[self.group] = ConsumerGroup(self.group, entry_id, self.entries_read)

        self.database.notify(NotificationType.STREAM, b"xgroup-create", self.key)

        return RESP_OK


@command(b"setid", {b"slow", b"stream"}, b"xgroup", flags={b"write"})
class StreamGroupSetId(Command):
    """
    summary: Sets the last-delivered ID of a consumer group.
    complexity: O(1)
    since: 5.0.0
    function: xgroupCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = positional_parameter()
    stream_id: bytes = positional_parameter()
    entries_read: int = positional_parameter(default=-1)

    def execute(self) -> ValueType:
        if self.entries_read < -1:
            raise ServerError(b"ERR value for ENTRIESREAD must be positive or -1")

        value = self.database.stream_database.get_value_or_none(self.key)
        if value is None:
            raise ServerError(b"ERR no such key")

        if self.group not in value.consumer_groups:
            raise ServerError(
                f"-NOGROUP No such key '{self.key.decode()}' or consumer group '{self.group.decode()}'"
                f" in XGROUP SETID command".encode()
            )

        group = value.consumer_groups[self.group]

        if self.stream_id == b"-":
            entry_id = (0, 0)
        elif self.stream_id == b"$":
            entry_id = value.last_id
        else:
            try:
                entry_id = parse_strict_entry_id(self.stream_id, sequence_fill=0)
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        group.last_id = entry_id
        group.read_entries = self.entries_read

        return RESP_OK


@command(b"groups", {b"read", b"slow", b"stream"}, b"xinfo", flags={b"readonly"})
class StreamInfoGroups(Command):
    """
    summary: Returns a list of the consumer groups of a stream.
    complexity: O(1)
    since: 5.0.0
    function: xinfoCommand
    reply_schema:
      type: array
      items:
        type: object
        additionalProperties: false
        properties:
          name:
            type: string
          consumers:
            type: integer
          pending:
            type: integer
          last-delivered-id:
            type: string
            pattern: '[0-9]+-[0-9]+'
          entries-read:
            oneOf:
            - type: 'null'
            - type: integer
          lag:
            oneOf:
            - type: 'null'
            - type: integer
    """

    database: Database = dependency()
    key: bytes = positional_parameter(key_mode=b"R")

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)

        if value is None:
            raise ServerError(b"ERR no such key")

        return [
            [
                b"name",
                group.name,
                b"consumers",
                len(group.consumers),
                b"pending",
                len(group.pending_entries),
                b"last-delivered-id",
                format_entry_id(group.last_id),
                b"entries-read",
                group.read_entries,
                b"lag",
                value.calculate_consumer_group_lag(group),
            ]
            for group in value.consumer_groups.values()
        ]


@command(
    b"xsetid",
    {b"stream"},
    flags={b"denyoom", b"fast", b"write"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class StreamSetId(Command):
    """
    summary: An internal command for replicating stream values.
    complexity: O(1)
    since: 5.0.0
    function: xsetidCommand
    reply_schema:
      const: OK
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    last_id: bytes = positional_parameter()
    entries_added: int | None = keyword_parameter(token=b"ENTRIESADDED", default=None)
    max_deleted_entry_id: bytes | None = keyword_parameter(token=b"MAXDELETEDID", default=None)

    def execute(self) -> ValueType:
        if self.entries_added is not None and self.entries_added < 0:
            raise ServerError(b"ERR entries_added must be positive")

        try:
            last_id = parse_strict_entry_id(self.last_id, sequence_fill=0)
        except ValueError:
            raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        max_deleted_entry_id = None
        if self.max_deleted_entry_id is not None:
            try:
                max_deleted_entry_id = parse_strict_entry_id(self.max_deleted_entry_id, sequence_fill=0)
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

            if last_id < max_deleted_entry_id:
                raise ServerError(b"ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id")

        value = self.database.stream_database.get_value_or_none(self.key)
        if value is None:
            raise ServerError(b"ERR no such key")

        if max_deleted_entry_id is not None and max_deleted_entry_id < value.max_deleted_entry_id:
            raise ServerError(b"ERR The ID specified in XSETID is smaller than current max_deleted_entry_id")

        if len(value) > 0:
            if last_id < value.last_id:
                raise ServerError(b"ERR The ID specified in XSETID is smaller than the target stream top item")

            if self.entries_added is not None and self.entries_added < value.added_entries:
                raise ServerError(b"ERR The entries_added specified in XSETID is smaller than the target stream length")

        value.last_id = last_id
        if self.entries_added is not None:
            value.added_entries = self.entries_added
        if max_deleted_entry_id is not None:
            value.max_deleted_entry_id = max_deleted_entry_id

        return RESP_OK


@command(b"consumers", {b"read", b"slow", b"stream"}, b"xinfo", flags={b"readonly"})
class StreamInfoConsumers(Command):
    """
    summary: Returns a list of the consumers in a consumer group.
    complexity: O(1)
    since: 5.0.0
    function: xinfoCommand
    reply_schema:
      description: Array list of consumers
      type: array
      uniqueItems: true
      items:
        type: object
        additionalProperties: false
        properties:
          name:
            type: string
          pending:
            type: integer
          idle:
            type: integer
          inactive:
            type: integer
    """

    database: Database = dependency()
    configuration: Configurations = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    group: bytes = keyword_parameter()

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)

        if value is None:
            raise ServerError(b"ERR no such key")

        if self.group not in value.consumer_groups:
            raise ServerError(
                b"NOGROUP No such consumer group '{self.group.decode()}' for key named '{self.key.decode()}'"
            )

        group = value.consumer_groups[self.group]

        return [
            [
                b"name",
                consumer.name,
                b"pending",
                len(consumer.pending_entries),
                b"idle",
                max((now_ms() - consumer.last_seen_timestamp + 5), 0),
                b"inactive",
                (now_ms() - consumer.last_active_timestamp + 5) if consumer.last_active_timestamp is not None else -1,
            ]
            for consumer in group.consumers.values()
        ]


@command(b"consumer", {b"stream", b"fast"}, b"xinfo", flags={b"read"})
class StreamInfoConsumer(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
    group: bytes = positional_parameter()
    consumer: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)
        if value is None:
            raise ServerError(b"ERR no such key")

        group = value.consumer_groups.get(self.group)
        if group is None:
            raise ServerError(b"ERR no such group")

        consumer = group.consumers.get(self.consumer)
        if consumer is None:
            return None

        return [
            b"name",
            consumer.name,
            b"pending",
            len(consumer.pending_entries),
            b"idle",
            max((now_ms() - consumer.last_seen_timestamp + 5), 0),
            b"inactive",
            (now_ms() - consumer.last_active_timestamp + 5) if consumer.last_active_timestamp is not None else -1,
        ]


@command(
    b"stream",
    {b"read", b"slow", b"stream"},
    b"xinfo",
    flags={b"readonly"},
    reply_schema=XINFO_STREAM_REPLY_SCHEMA,
)
class StreamInfoStream(Command):
    """
    summary: Returns information about a stream.
    complexity: O(1)
    since: 5.0.0
    function: xinfoCommand
    """

    database: Database = dependency()
    configuration: Configurations = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    full: bool = flag_parameter(token=b"FULL")
    count: int | None = keyword_parameter(flag=b"COUNT", default=None)

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)

        if value is None:
            raise ServerError(b"ERR no such key")

        info = [
            b"length",
            len(value),
            b"radix-tree-keys",
            len(value),
            b"radix-tree-nodes",
            math.ceil(len(value) / self.configuration.stream_node_max_entries) if len(value) > 0 else 1,
            b"last-generated-id",
            format_entry_id(value.last_id),
            b"max-deleted-entry-id",
            format_entry_id(value.max_deleted_entry_id),
            b"entries-added",
            value.added_entries,
            b"recorded-first-entry-id",
            format_entry_id(value.first_id),
        ]
        if not self.full:
            return info

        return [
            *info,
            b"entries",
            [[format_entry_id(entry_id), entry_data] for entry_id, entry_data in value.range(count=self.count)],
            b"groups",
            [
                [
                    b"name",
                    group.name,
                    b"last-delivered-id",
                    format_entry_id(group.last_id),
                    b"entries-read",
                    group.read_entries if group.read_entries != -1 else None,
                    b"lag",
                    value.calculate_consumer_group_lag(group),
                    b"pending",
                    [
                        [format_entry_id(entry_id), pending_entry.times_delivered]
                        for entry_id, pending_entry in group.pending_entries.items()
                    ],
                    b"consumers",
                    [
                        [
                            b"name",
                            consumer.name,
                            b"seen-time",
                            consumer.last_seen_timestamp,
                            b"active-time",
                            consumer.last_active_timestamp if consumer.last_active_timestamp is not None else -1,
                            b"pel-count",
                            len(consumer.pending_entries),
                            b"pending",
                            [
                                [format_entry_id(entry_id), pending_entry.times_delivered]
                                for entry_id, pending_entry in consumer.pending_entries.items()
                            ],
                        ]
                        for consumer in group.consumers.values()
                    ],
                ]
                for group in value.consumer_groups.values()
            ],
        ]


@command(b"xread", {b"blocking", b"read", b"slow", b"stream"}, flags={b"blocking", b"readonly"})
class StreamRead(Command):
    """
    summary: >-
      Returns messages from multiple streams with IDs greater than the ones requested. Blocks until a message is
      available otherwise.
    since: 5.0.0
    function: xreadCommand
    reply_schema:
      oneOf:
      - description: >-
          A map of key-value elements when each element composed of key name and the entries reported for that key.
        type: object
        patternProperties:
          ^.*$:
            description: The entries reported for that key.
            type: array
            items:
              type: array
              minItems: 2
              maxItems: 2
              items:
              - description: Entry id.
                type: string
                pattern: '[0-9]+-[0-9]+'
              - description: >-
                  A map of key-value elements when each element composed of key name and the entries reported for
                  that key.
                type: array
                items:
                  type: string
      - description: >-
          If BLOCK option is given, and a timeout occurs, or there is no stream we can serve.
        type: 'null'
    """

    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()
    client_context: ClientContext = dependency()

    count: int | None = keyword_parameter(flag=b"COUNT", default=None)
    block_milliseconds: int | None = keyword_parameter(flag=b"BLOCK", default=None)
    keys_and_ids: list[bytes] = keyword_parameter(token=b"STREAMS")

    _keys_to_minimum_id: dict[bytes, EntryID] | None = field(default_factory=dict, init=False)

    async def before(self, in_multi: bool = False) -> None:
        if len(self.keys_and_ids) % 2 != 0:
            raise ServerError(
                b"ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified."
            )

        keys = self.keys_and_ids[0 : len(self.keys_and_ids) // 2]
        ids = self.keys_and_ids[len(self.keys_and_ids) // 2 :]

        self._keys_to_minimum_id = await self.blocking_manager.wait_for_stream(
            self.client_context,
            {key: id_ for key, id_ in zip(keys, ids)},
            block_milliseconds=self.block_milliseconds,
            in_multi=in_multi,
        )

    def execute(self) -> ValueType:
        if self._keys_to_minimum_id is None:
            return None

        result = []
        for key, entry_id in self._keys_to_minimum_id.items():
            value = self.database.stream_database.get_value_or_empty(key)

            entries = [
                [format_entry_id(entry_id), entry_data]
                for entry_id, entry_data in value.range(
                    minimum_timestamp=entry_id[0],
                    minimum_sequence=entry_id[1],
                    count=self.count,
                    minimum_inclusive=False,
                )
            ]

            if not entries:
                continue

            result.append([key, entries])

        return result


@command(b"xreadgroup", {b"blocking", b"slow", b"stream"}, flags={b"blocking", b"write"})
class StreamGroupRead(Command):
    """
    summary: >-
      Returns new or historical messages from a stream for a consumer in a group. Blocks until a message is
      available otherwise.
    complexity: >-
      For each stream mentioned: O(M) with M being the number of elements returned. If M is constant (e.g. always
      asking for the first 10 elements with COUNT), you can consider it O(1). On the other side when XREADGROUP blocks,
      XADD will pay the O(N) time in order to serve the N clients blocked on the stream getting new data.
    since: 5.0.0
    function: xreadCommand
    reply_schema:
      oneOf:
      - description: If BLOCK option is specified and the timeout expired
        type: 'null'
      - description: A map of key-value elements when each element composed of key name
          and the entries reported for that key
        type: object
        additionalProperties:
          description: The entries reported for that key
          type: array
          items:
            type: array
            minItems: 2
            maxItems: 2
            items:
            - description: Stream id
              type: string
              pattern: '[0-9]+-[0-9]+'
            - oneOf:
              - description: Array of field-value pairs
                type: array
                items:
                  type: string
              - type: 'null'
    """

    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()
    client_context: ClientContext = dependency()

    group: bytes = keyword_parameter(token=b"GROUP")  # todo: should be positional
    consumer: bytes = positional_parameter()

    count: int | None = keyword_parameter(flag=b"COUNT", default=None)
    block_milliseconds: int | None = keyword_parameter(flag=b"BLOCK", default=None)
    no_ack: bool = flag_parameter(token=b"NOACK")
    keys_and_ids: list[bytes] = keyword_parameter(token=b"STREAMS")

    _keys_to_minimum_id: dict[bytes, EntryID] | None = field(default_factory=dict, init=False)

    _key_to_consumer_group: dict[bytes, ConsumerGroup] = field(default_factory=dict, init=False)
    _key_to_consumer: dict[bytes, Consumer] = field(default_factory=dict, init=False)

    _key_to_history_only: dict[bytes, bool] = field(default_factory=dict, init=False)

    async def before(self, in_multi: bool = False) -> None:
        if len(self.keys_and_ids) % 2 != 0:
            raise ServerError(
                b"ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
            )

        keys = self.keys_and_ids[0 : len(self.keys_and_ids) // 2]
        ids = self.keys_and_ids[len(self.keys_and_ids) // 2 :]

        self._keys_to_minimum_id = await self.blocking_manager.wait_for_group(
            self.client_context,
            self.group,
            self.consumer,
            {key: id_ for key, id_ in zip(keys, ids)},
            self.block_milliseconds,
            in_multi=in_multi,
            waiting_context=StreamWaitingContext(
                key_to_consumer_group=self._key_to_consumer_group,
                key_to_consumer=self._key_to_consumer,
                key_to_history_only=self._key_to_history_only,
            ),
        )

    def execute(self) -> ValueType:
        if self._keys_to_minimum_id is None:
            return None

        result = []
        for key, stream_id in self._keys_to_minimum_id.items():
            value = self.database.stream_database.get_value_or_empty(key)

            group = self._key_to_consumer_group[key]
            consumer = self._key_to_consumer[key]
            consumer.last_seen_timestamp = self.client_context.current_client.command_time_snapshot

            entries = []
            if not self._key_to_history_only[key]:
                for entry_id, entry_data in value.range(
                    minimum_timestamp=group.last_id[0],
                    minimum_sequence=group.last_id[1],
                    count=self.count,
                    minimum_inclusive=False,
                ):
                    value.update_group_last_id(group, entry_id)

                    if not self.no_ack:
                        consumer.last_active_timestamp = self.client_context.current_client.command_time_snapshot

                        group_pending_entry = group.pending_entries.get(entry_id)
                        if group_pending_entry is not None and group_pending_entry.consumer != consumer:
                            previous_consumer = group_pending_entry.consumer
                            previous_consumer.pending_entries.pop(entry_id, None)

                        consumer.pending_entries[entry_id] = group.pending_entries[entry_id] = PendingEntry(
                            consumer,
                            last_delivery=self.client_context.current_client.command_time_snapshot,
                            times_delivered=1,
                        )
                        self.client_context.propagated_commands.append(
                            StreamGroupClaim(
                                self.database,
                                self.client_context,
                                key,
                                group.name,
                                consumer.name,
                                0,
                                [format_entry_id(entry_id)],
                                time_milliseconds=consumer.pending_entries[entry_id].last_delivery,
                                retry_count=consumer.pending_entries[entry_id].times_delivered,
                                force=True,
                                just_id=True,
                                last_id=format_entry_id(group.last_id),
                            )
                        )

                    entries.append([format_entry_id(entry_id), entry_data])
                self.client_context.propagated_commands.append(
                    StreamGroupSetId(
                        self.database,
                        key,
                        group.name,
                        format_entry_id(group.last_id),
                        group.read_entries,
                    )
                )
            else:
                for entry_id, pending_entry in range_entries(
                    consumer.pending_entries,
                    stream_id[0],
                    stream_id[1],
                    count=self.count,
                    minimum_inclusive=False,
                ):
                    pending_entry = consumer.pending_entries[entry_id]
                    pending_entry.times_delivered += 1
                    pending_entry.last_delivery = self.client_context.current_client.command_time_snapshot
                    entries.append([format_entry_id(entry_id), value.entries.get(entry_id, {}) or {}])

            result.append([key, entries])

        return result


@parameters_object
class ExtendedPendingParameters:
    minimum_idle_time: int | None = keyword_parameter(token=b"IDLE", default=None)
    start: bytes = positional_parameter()
    end: bytes = positional_parameter()
    count: int = positional_parameter()
    consumer: bytes | None = positional_parameter(default=None)


@command(b"xpending", {b"read", b"slow", b"stream"}, flags={b"readonly"})
class StreamGroupPending(Command):
    """
    summary: >-
      Returns the information and entries from a stream consumer group's pending entries list.
    complexity: >-
      O(N) with N being the number of elements returned, so asking for a small fixed number of entries per call
      is O(1). O(M), where M is the total number of entries scanned when used with the IDLE filter. When the command
      returns just the summary and the list of consumers is small, it runs in O(1) time; otherwise, an additional
      O(N) time for iterating every consumer.
    since: 5.0.0
    function: xpendingCommand
    reply_schema:
      oneOf:
      - description: Extended form, in case `start` was given.
        type: array
        items:
          type: array
          minItems: 4
          maxItems: 4
          items:
          - description: Entry ID
            type: string
            pattern: '[0-9]+-[0-9]+'
          - description: Consumer name
            type: string
          - description: Idle time
            type: integer
          - description: Delivery count
            type: integer
      - description: Summary form, in case `start` was not given.
        type: array
        minItems: 4
        maxItems: 4
        items:
        - description: Total number of pending messages
          type: integer
        - description: Minimal pending entry ID
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Maximal pending entry ID
          type: string
          pattern: '[0-9]+-[0-9]+'
        - description: Consumers with pending messages
          type: array
          items:
            type: array
            minItems: 2
            maxItems: 2
            items:
            - description: Consumer name
              type: string
            - description: Number of pending messages
              type: string
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"R")
    group: bytes = positional_parameter()

    extended_parameters: ExtendedPendingParameters | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)
        if value is None or value.consumer_groups.get(self.group) is None:
            raise ServerError(
                f"-NOGROUP No such key '{self.key.decode()}' or consumer group '{self.group.decode()}'".encode()
            )
        group = value.consumer_groups[self.group]

        if self.extended_parameters is None:
            first_non_deleted_pending_entry = None
            last_non_deleted_pending_entry = None
            for entry_id in group.pending_entries.keys():
                if entry_id not in value.entries or value.entries[entry_id] is None:
                    continue
                if first_non_deleted_pending_entry is None:
                    first_non_deleted_pending_entry = format_entry_id(entry_id)
                last_non_deleted_pending_entry = format_entry_id(entry_id)

            return [
                len(group.pending_entries),
                first_non_deleted_pending_entry,
                last_non_deleted_pending_entry,
                [
                    [
                        consumer_name,
                        sum(
                            1
                            for pending_entry in consumer.pending_entries.keys()
                            if pending_entry in value.entries and value.entries[pending_entry] is not None
                        ),
                    ]
                    for consumer_name, consumer in group.consumers.items()
                ],
            ]

        pending_entries = group.pending_entries
        if self.extended_parameters.consumer is not None:
            consumer = group.consumers.get(self.extended_parameters.consumer)
            if consumer is None:
                return []
            pending_entries = consumer.pending_entries

        result = []
        for entry_id, entry_data in range_entries(
            pending_entries,
            *_parse_range(self.extended_parameters.start, self.extended_parameters.end),
            count=self.extended_parameters.count,
            is_reversed=False,
        ):
            if entry_id not in value.entries or value.entries[entry_id] is None:
                continue

            if (
                self.extended_parameters.minimum_idle_time is not None
                and now_ms() - entry_data.last_delivery < self.extended_parameters.minimum_idle_time
            ):
                continue

            result.append(
                [
                    format_entry_id(entry_id),
                    entry_data.consumer.name,
                    now_ms() - entry_data.last_delivery,
                    entry_data.times_delivered,
                ]
            )

        return result


@command(b"xack", {b"stream"}, flags={b"fast", b"write"})
class StreamGroupAcknowledge(Command):
    """
    summary: >-
      Returns the number of messages that were successfully acknowledged by the consumer group member of a stream.
    complexity: O(1) for each message ID processed.
    since: 5.0.0
    function: xackCommand
    reply_schema:
      description: >-
        The command returns the number of messages successfully acknowledged. Certain message IDs may no longer
        be part of the PEL (for example because they have already been acknowledged), and XACK will not count them
        as successfully acknowledged.
      type: integer
      minimum: 0
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = positional_parameter()
    ids: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)
        if value is None or value.consumer_groups.get(self.group) is None:
            return 0
        group = value.consumer_groups[self.group]

        if not group.pending_entries:
            return 0

        parsed_ids: set[EntryID] = set()

        for stream_id in self.ids:
            try:
                parsed_ids.add(parse_strict_entry_id(stream_id, sequence_fill=0))
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        deleted_count = 0
        for parsed_stream_id in parsed_ids:
            deleted = group.pending_entries.pop(parsed_stream_id, None)
            if deleted is not None:
                group.consumers[deleted.consumer.name].pending_entries.pop(parsed_stream_id, None)
                deleted_count += 1

        return deleted_count


@command(b"destroy", {b"slow", b"stream"}, parent_command=b"xgroup", flags={b"write"})
class StreamGroupDestroy(Command):
    """
    summary: Destroys a consumer group.
    complexity: O(N) where N is the number of entries in the group's pending entries list (PEL).
    since: 5.0.0
    function: xgroupCommand
    reply_schema:
      description: The number of destroyed consumer groups (0 or 1)
      oneOf:
      - const: 1
      - const: 0
    """

    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = positional_parameter()

    had_group: bool = field(default=False, init=False)

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)

        if value is None or value.consumer_groups.pop(self.group, None) is None:
            return 0

        self.had_group = True
        return 1

    async def after(self, in_multi: bool = False) -> None:
        if self.had_group:
            await self.blocking_manager.notify_deleted(self.key, in_multi=in_multi)


@command(b"xautoclaim", {b"stream"}, flags={b"fast", b"write"}, reply_schema=XAUTOCLAIM_REPLY_SCHEMA)
class StreamGroupAutoClaim(Command):
    """
    summary: >-
      Changes, or acquires, ownership of messages in a consumer group, as if the messages were delivered to a consumer
      group member.
    complexity: O(1) if COUNT is small.
    since: 6.2.0
    function: xautoclaimCommand
    """

    database: Database = dependency()
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = keyword_parameter()
    consumer_name: bytes = positional_parameter()
    minimum_idle_time: int = positional_parameter()
    start: bytes = positional_parameter()
    count: int = keyword_parameter(flag=b"COUNT", default=100)
    just_id: bool = flag_parameter(token=b"JUSTID")

    def execute(self) -> ValueType:
        if self.count <= 0 or self.count > (LONG_MAX / 16):
            raise ServerError(b"ERR COUNT must be > 0")

        value = self.database.stream_database.get_value_or_none(self.key)
        group = value.consumer_groups.get(self.group) if value else None

        if value is None or group is None:
            raise ServerError(
                f"-NOGROUP No such key '{self.key.decode()}' or consumer group '{self.group.decode()}'".encode()
            )

        consumer = group.consumers.get(self.consumer_name)
        if consumer is None:
            consumer = Consumer(self.consumer_name)
            group.consumers[self.consumer_name] = consumer

        consumer.last_seen_timestamp = self.client_context.current_client.command_time_snapshot

        stream_entries: list = []
        deleted_entries: list = []
        entry_id = (0, 0)
        for entry_id, entry in range_entries(
            group.pending_entries,
            *_parse_range(self.start, b"+"),
            count=self.count,
            is_reversed=False,
        ):
            if entry_id not in value.entries or value.entries[entry_id] is None:
                deleted_entries.append(format_entry_id(entry_id))
                continue
            if now_ms() - entry.last_delivery < self.minimum_idle_time:
                continue

            entry.consumer.pending_entries.pop(entry_id)
            entry.consumer = consumer
            consumer.pending_entries[entry_id] = entry
            consumer.last_active_timestamp = self.client_context.current_client.command_time_snapshot

            if not self.just_id:
                entry.last_delivery = now_ms()
                entry.times_delivered += 1
                stream_entries.append([format_entry_id(entry_id), value.entries.get(entry_id)])
                continue
            stream_entries.append(format_entry_id(entry_id))

        next_entry_id = (0, 0)
        for next_entry_id, _ in range_entries(
            group.pending_entries,
            minimum_timestamp=entry_id[0],
            minimum_sequence=entry_id[1],
            minimum_inclusive=False,
            count=1,
            is_reversed=False,
        ):
            pass

        return [format_entry_id(next_entry_id), stream_entries, deleted_entries]


@command(b"xclaim", {b"stream"}, flags={b"fast", b"write"})
class StreamGroupClaim(Command):
    """
    summary: >-
      Changes, or acquires, ownership of a message in a consumer group, as if the message was delivered to a consumer
      group member.
    complexity: O(log N) with N being the number of messages in the PEL of the consumer group.
    since: 5.0.0
    function: xclaimCommand
    reply_schema:
      description: Stream entries with IDs matching the specified range.
      anyOf:
      - description: >-
          If JUSTID option is specified, return just an array of IDs of messages successfully claimed.
        type: array
        items:
          description: Entry ID.
          type: string
          pattern: '[0-9]+-[0-9]+'
      - description: >-
          Array of stream entries that contains each entry as an array of 2 elements, the Entry ID and the entry
          data itself.
        type: array
        uniqueItems: true
        items:
          type: array
          minItems: 2
          maxItems: 2
          items:
          - description: Entry ID.
            type: string
            pattern: '[0-9]+-[0-9]+'
          - description: Data.
            type: array
            items:
              type: string
    """

    database: Database = dependency()
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = keyword_parameter()
    consumer_name: bytes = positional_parameter()
    minimum_idle_time: int = positional_parameter()
    entry_ids: list[bytes] = positional_parameter()
    idle: int | None = keyword_parameter(token=b"IDLE", default=None)
    time_milliseconds: int | None = keyword_parameter(token=b"TIME", default=None)
    retry_count: int | None = keyword_parameter(token=b"RERETRYCOUNT", default=None)
    force: bool = flag_parameter(token=b"FORCE")
    just_id: bool = flag_parameter(token=b"JUSTID")
    last_id: bytes | None = keyword_parameter(token=b"LASTID", default=None)
    count: int | None = keyword_parameter(flag=b"COUNT", default=None)

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)
        group = value.consumer_groups.get(self.group) if value else None

        if value is None or group is None:
            raise ServerError(
                f"-NOGROUP No such key '{self.key.decode()}' or consumer group '{self.group.decode()}'".encode()
            )

        last_id = (0, 0)
        if self.last_id is not None:
            try:
                last_id = parse_strict_entry_id(self.last_id, sequence_fill=0)
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        group.last_id = max(group.last_id, last_id)

        consumer = group.consumers.get(self.consumer_name)
        if consumer is None:
            consumer = Consumer(self.consumer_name)
            group.consumers[self.consumer_name] = consumer
            self.database.notify(NotificationType.STREAM, b"xgroup-createconsumer", self.key)

        consumer.last_seen_timestamp = self.client_context.current_client.command_time_snapshot

        stream_entries: list = []
        for stream_id in self.entry_ids:
            try:
                entry_id = parse_strict_entry_id(stream_id, sequence_fill=0)
            except ValueError:
                raise ServerError(f"ERR Unrecognized XCLAIM option '{stream_id.decode()}'".encode())

            entry = group.pending_entries.get(entry_id)
            if entry is None:
                continue
            if entry_id not in value.entries or value.entries[entry_id] is None:
                continue
            if now_ms() - entry.last_delivery < self.minimum_idle_time:
                continue

            entry.consumer.pending_entries.pop(entry_id)
            entry.consumer = consumer
            consumer.pending_entries[entry_id] = entry
            consumer.last_active_timestamp = self.client_context.current_client.command_time_snapshot

            if not self.just_id:
                entry.last_delivery = now_ms()
                entry.times_delivered += 1
                stream_entries.append([stream_id, value.entries.get(entry_id)])
                continue
            stream_entries.append(stream_id)
        return stream_entries


@command(b"createconsumer", {b"slow", b"stream"}, parent_command=b"xgroup", flags={b"denyoom", b"write"})
class StreamGroupCreateConsumer(Command):
    """
    summary: Creates a consumer in a consumer group.
    complexity: O(1)
    since: 6.2.0
    function: xgroupCommand
    reply_schema:
      description: The number of created consumers (0 or 1)
      oneOf:
      - const: 1
      - const: 0
    """

    database: Database = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = positional_parameter()
    consumer: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)

        if value is None or value.consumer_groups.get(self.group) is None:
            raise ServerError(
                f"-NOGROUP No such key '{self.key.decode()}' or consumer group '{self.group.decode()}'".encode()
            )

        group = value.consumer_groups[self.group]

        if self.consumer in group.consumers:
            return 0

        group.consumers[self.consumer] = Consumer(self.consumer)

        self.database.notify(NotificationType.STREAM, b"xgroup-createconsumer", self.key)

        return 1


@command(b"delconsumer", {b"slow", b"stream"}, parent_command=b"xgroup", flags={b"write"})
class StreamGroupDeleteConsumer(Command):
    """
    summary: Deletes a consumer from a consumer group.
    complexity: O(1)
    since: 5.0.0
    function: xgroupCommand
    reply_schema:
      description: The number of pending messages that were yet associated with such a consumer
      type: integer
      minimum: 0
    """

    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter(key_mode=b"RW")
    group: bytes = positional_parameter()
    consumer: bytes = positional_parameter()

    removed_pending_count: int = field(default=0, init=False)

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_none(self.key)

        if value is None or value.consumer_groups.get(self.group) is None:
            return 0

        group = value.consumer_groups[self.group]

        consumer = group.consumers.pop(self.consumer, None)
        if consumer is None:
            return 0

        for entry_id in consumer.pending_entries.keys():
            group.pending_entries.pop(entry_id, None)
            self.removed_pending_count += 1

        self.database.notifications_manager.notify(NotificationType.STREAM, b"xgroup-delconsumer", self.key)

        return self.removed_pending_count

    async def after(self, in_multi: bool = False) -> None:
        if self.removed_pending_count > 0:
            await self.blocking_manager.notify_deleted(self.key, in_multi=in_multi)
