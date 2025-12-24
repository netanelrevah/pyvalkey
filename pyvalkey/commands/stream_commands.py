import math
from dataclasses import field
from typing import cast

from pyvalkey.blocking import StreamBlockingManager, StreamWaitingContext
from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata, parameters_object
from pyvalkey.commands.router import command
from pyvalkey.commands.utils import _format_entry_id, _parse_entry_id, _parse_strict_entry_id
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
from pyvalkey.utils.times import now_ms


@parameters_object
class MaxLength:
    equal: bool = keyword_parameter(flag=b"=", default=False)
    approximate: bool = keyword_parameter(flag=b"~", default=False)
    threshold: int = positional_parameter()
    limit: int | None = keyword_parameter(flag=b"LIMIT", default=None)


@parameters_object
class MinimumId:
    equal: bool = keyword_parameter(flag=b"=", default=False)
    approximate: bool = keyword_parameter(flag=b"~", default=False)
    threshold: bytes = positional_parameter()
    limit: int | None = keyword_parameter(flag=b"LIMIT", default=None)


@command(b"xtrim", {b"stream", b"write", b"fast"})
class StreamTrim(Command):
    database: Database = dependency()
    configuration: Configurations = dependency()

    key: bytes = positional_parameter()
    no_make_stream: bool = keyword_parameter(flag=b"NOMKSTREAM", default=False)
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
            limiter = cast(MaxLength | MinimumId, self.maximum_length or self.minimum_id)

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


@command(b"xadd", {b"stream", b"write", b"fast"})
class StreamAdd(Command):
    database: Database = dependency()
    configuration: Configurations = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter()
    no_make_stream: bool = keyword_parameter(flag=b"NOMKSTREAM", default=False)
    maximum_length: MaxLength | None = keyword_parameter(token=b"MAXLEN", default=None)
    minimum_id: MinimumId | None = keyword_parameter(token=b"MINID", default=None)
    stream_id: bytes = positional_parameter()
    field_value: list[tuple[bytes, bytes]] = positional_parameter(sequence_allow_empty=False)

    _entry_id: EntryID | None = field(init=False, default=None)

    def execute(self) -> ValueType:
        if self.minimum_id is not None and self.maximum_length is not None:
            raise ServerError(b"ERR syntax error, MAXLEN and MINID options at the same time are not compatible")

        if self.minimum_id is not None or self.maximum_length is not None:
            limiter = cast(MaxLength | MinimumId, self.maximum_length or self.minimum_id)

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

        return _format_entry_id(self._entry_id)

    async def after(self, in_multi: bool = False) -> None:
        if self._entry_id is not None:
            await self.blocking_manager.notify(self.key, in_multi=in_multi)


@command(b"help", {b"stream", b"write", b"fast"}, parent_command=b"xgroup")
class StreamGroupHelp(Command):
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


@command(b"help", {b"stream", b"write", b"fast"}, parent_command=b"xinfo")
class StreamInfoHelp(Command):
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


@command(b"xlen", {b"stream", b"write", b"fast"})
class StreamLength(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.stream_database.get_value_or_empty(self.key))


@command(b"xdel", {b"stream", b"write", b"fast"})
class StreamDelete(Command):
    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter()
    ids: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.stream_database.get_value_or_empty(self.key)

        deleted_count = 0
        for stream_id in self.ids:
            try:
                timestamp, sequence = _parse_strict_entry_id(stream_id)

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


@command(b"xrange", {b"stream", b"write", b"fast"})
class StreamRange(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
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
        return [[_format_entry_id(entry_id), entry_data] for entry_id, entry_data in entries]

    def execute(self) -> ValueType:
        return self.range(self.database.stream_database.get_value_or_empty(self.key), self.start, self.end, self.count)


@command(b"xrevrange", {b"stream", b"write", b"fast"})
class StreamReversedRange(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
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


@command(b"create", {b"write", b"stream", b"slow"}, b"xgroup")
class StreamGroupCreate(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    stream_id: bytes = positional_parameter()
    make_stream: bool = keyword_parameter(flag=b"MKSTREAM", default=False)
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
            entry_id = _parse_strict_entry_id(self.stream_id, sequence_fill=0)

        if self.entries_read is not None and self.entries_read < -1:
            raise ServerError(b"ERR value for ENTRIESREAD must be positive or -1")

        value.consumer_groups[self.group] = ConsumerGroup(self.group, entry_id, self.entries_read)

        self.database.notify(NotificationType.STREAM, b"xgroup-create", self.key)

        return RESP_OK


@command(b"setid", {b"write", b"stream", b"slow"}, b"xgroup")
class StreamGroupSetId(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
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
                entry_id = _parse_strict_entry_id(self.stream_id, sequence_fill=0)
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        group.last_id = entry_id
        group.read_entries = self.entries_read

        return RESP_OK


@command(b"groups", {b"stream", b"write", b"fast"}, b"xinfo")
class StreamInfoGroups(Command):
    database: Database = dependency()
    key: bytes = positional_parameter()

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
                _format_entry_id(group.last_id),
                b"entries-read",
                group.read_entries,
                b"lag",
                value.calculate_consumer_group_lag(group),
            ]
            for group in value.consumer_groups.values()
        ]


@command(
    b"xsetid",
    {b"stream", b"write", b"fast"},
    metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"},
)
class StreamSetId(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
    last_id: bytes = positional_parameter()
    entries_added: int | None = keyword_parameter(token=b"ENTRIESADDED", default=None)
    max_deleted_entry_id: bytes | None = keyword_parameter(token=b"MAXDELETEDID", default=None)

    def execute(self) -> ValueType:
        if self.entries_added is not None and self.entries_added < 0:
            raise ServerError(b"ERR entries_added must be positive")

        try:
            last_id = _parse_strict_entry_id(self.last_id, sequence_fill=0)
        except ValueError:
            raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        max_deleted_entry_id = None
        if self.max_deleted_entry_id is not None:
            try:
                max_deleted_entry_id = _parse_strict_entry_id(self.max_deleted_entry_id, sequence_fill=0)
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


@command(b"consumers", {b"stream", b"write", b"fast"}, b"xinfo")
class StreamInfoConsumers(Command):
    database: Database = dependency()
    configuration: Configurations = dependency()

    key: bytes = positional_parameter()
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


@command(b"stream", {b"stream", b"write", b"fast"}, b"xinfo")
class StreamInfoStream(Command):
    database: Database = dependency()
    configuration: Configurations = dependency()

    key: bytes = positional_parameter()
    full: bool = keyword_parameter(flag=b"FULL", default=False)
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
            _format_entry_id(value.last_id),
            b"max-deleted-entry-id",
            _format_entry_id(value.max_deleted_entry_id),
            b"entries-added",
            value.added_entries,
            b"recorded-first-entry-id",
            _format_entry_id(value.first_id),
        ]
        if not self.full:
            return info

        return [
            *info,
            b"entries",
            [[_format_entry_id(entry_id), entry_data] for entry_id, entry_data in value.range(count=self.count)],
            b"groups",
            [
                [
                    b"name",
                    group.name,
                    b"last-delivered-id",
                    _format_entry_id(group.last_id),
                    b"entries-read",
                    group.read_entries if group.read_entries != -1 else None,
                    b"lag",
                    value.calculate_consumer_group_lag(group),
                    b"pending",
                    [
                        [_format_entry_id(entry_id), pending_entry.times_delivered]
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
                                [_format_entry_id(entry_id), pending_entry.times_delivered]
                                for entry_id, pending_entry in consumer.pending_entries.items()
                            ],
                        ]
                        for consumer in group.consumers.values()
                    ],
                ]
                for group in value.consumer_groups.values()
            ],
        ]


@command(b"xread", {b"stream"})
class StreamRead(Command):
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
                [_format_entry_id(entry_id), entry_data]
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


@command(b"xreadgroup", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupRead(Command):
    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()
    client_context: ClientContext = dependency()

    group: bytes = keyword_parameter(token=b"GROUP")  # todo: should be positional
    consumer: bytes = positional_parameter()

    count: int | None = keyword_parameter(flag=b"COUNT", default=None)
    block_milliseconds: int | None = keyword_parameter(flag=b"BLOCK", default=None)
    no_ack: bool = keyword_parameter(flag=b"NOACK", default=False)
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
                                [_format_entry_id(entry_id)],
                                time_milliseconds=consumer.pending_entries[entry_id].last_delivery,
                                retry_count=consumer.pending_entries[entry_id].times_delivered,
                                force=True,
                                just_id=True,
                                last_id=_format_entry_id(group.last_id),
                            )
                        )

                    entries.append([_format_entry_id(entry_id), entry_data])
                self.client_context.propagated_commands.append(
                    StreamGroupSetId(
                        self.database,
                        key,
                        group.name,
                        _format_entry_id(group.last_id),
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
                    entries.append([_format_entry_id(entry_id), value.entries.get(entry_id, {}) or {}])

            result.append([key, entries])

        return result


@parameters_object
class ExtendedPendingParameters:
    minimum_idle_time: int | None = keyword_parameter(token=b"IDLE", default=None)
    start: bytes = positional_parameter()
    end: bytes = positional_parameter()
    count: int = positional_parameter()
    consumer: bytes | None = positional_parameter(default=None)


@command(b"xpending", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupPending(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
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
                    first_non_deleted_pending_entry = _format_entry_id(entry_id)
                last_non_deleted_pending_entry = _format_entry_id(entry_id)

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
                    _format_entry_id(entry_id),
                    entry_data.consumer.name,
                    now_ms() - entry_data.last_delivery,
                    entry_data.times_delivered,
                ]
            )

        return result


@command(b"xack", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupAcknowledge(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
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
                parsed_ids.add(_parse_strict_entry_id(stream_id, sequence_fill=0))
            except ValueError:
                raise ServerError(b"ERR Invalid stream ID specified as stream command argument")

        deleted_count = 0
        for parsed_stream_id in parsed_ids:
            deleted = group.pending_entries.pop(parsed_stream_id, None)
            if deleted is not None:
                group.consumers[deleted.consumer.name].pending_entries.pop(parsed_stream_id, None)
                deleted_count += 1

        return deleted_count


@command(b"destroy", {b"write", b"stream", b"slow", b"blocking"}, parent_command=b"xgroup")
class StreamGroupDestroy(Command):
    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter()
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


@command(b"xautoclaim", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupAutoClaim(Command):
    database: Database = dependency()
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter()
    group: bytes = keyword_parameter()
    consumer_name: bytes = positional_parameter()
    minimum_idle_time: int = positional_parameter()
    start: bytes = positional_parameter()
    count: int = keyword_parameter(flag=b"COUNT", default=100)
    just_id: bool = keyword_parameter(flag=b"JUSTID", default=False)

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
                deleted_entries.append(_format_entry_id(entry_id))
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
                stream_entries.append([_format_entry_id(entry_id), value.entries.get(entry_id)])
                continue
            stream_entries.append(_format_entry_id(entry_id))

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

        return [_format_entry_id(next_entry_id), stream_entries, deleted_entries]


@command(b"xclaim", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupClaim(Command):
    database: Database = dependency()
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter()
    group: bytes = keyword_parameter()
    consumer_name: bytes = positional_parameter()
    minimum_idle_time: int = positional_parameter()
    entry_ids: list[bytes] = positional_parameter()
    idle: int | None = keyword_parameter(token=b"IDLE", default=None)
    time_milliseconds: int | None = keyword_parameter(token=b"TIME", default=None)
    retry_count: int | None = keyword_parameter(token=b"RERETRYCOUNT", default=None)
    force: bool = keyword_parameter(flag=b"FORCE", default=False)
    just_id: bool = keyword_parameter(flag=b"JUSTID", default=False)
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
                last_id = _parse_strict_entry_id(self.last_id, sequence_fill=0)
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
                entry_id = _parse_strict_entry_id(stream_id, sequence_fill=0)
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


@command(b"createconsumer", {b"write", b"stream", b"slow", b"blocking"}, parent_command=b"xgroup")
class StreamGroupCreateConsumer(Command):
    database: Database = dependency()

    key: bytes = positional_parameter()
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


@command(b"delconsumer", {b"write", b"stream", b"slow", b"blocking"}, parent_command=b"xgroup")
class StreamGroupDeleteConsumer(Command):
    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()

    key: bytes = positional_parameter()
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
