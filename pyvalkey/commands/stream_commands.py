import asyncio
import math
import time
from asyncio import Queue
from dataclasses import field
from typing import cast, overload

from pyvalkey.commands.consts import UINT64_MAX
from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.parsers import CommandMetadata, parameters_object
from pyvalkey.commands.router import command
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, StreamBlockingManager
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.database_objects.stream import EntryID, Stream
from pyvalkey.resp import RESP_OK, ValueType


@overload
def _parse_strict_entry_id(stream_id: bytes) -> tuple[int, int | None]: ...
@overload
def _parse_strict_entry_id(stream_id: bytes, sequence_fill: int) -> tuple[int, int]: ...
def _parse_strict_entry_id(stream_id: bytes, sequence_fill: int | None = None) -> tuple[int, int | None]:
    if b"-" not in stream_id:
        return int(stream_id), sequence_fill
    timestamp, sequence = stream_id.split(b"-")
    return int(timestamp), int(sequence)


def _parse_entry_id(stream_id: bytes) -> tuple[int | None, int | None]:
    if stream_id == b"*":
        return None, None
    if b"-" not in stream_id:
        return int(stream_id), None
    timestamp, sequence = stream_id.split(b"-")
    if sequence == b"*":
        return int(timestamp), None
    return int(timestamp), int(sequence)


def _format_entry_id(entry_id: EntryID) -> bytes:
    return f"{entry_id[0]}-{entry_id[1]}".encode()


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
            last_entry_timestamp, last_entry_sequence = value.entries.peekitem(-1)[0]
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
            last_entry_timestamp, last_entry_sequence = value.entries.peekitem(-1)[0]
            if last_entry_timestamp >= UINT64_MAX and last_entry_sequence >= UINT64_MAX:
                raise ServerError(b"ERR The stream has exhausted the last possible ID, unable to add more items")

        entry_id = value.generate_entry_id(entry_timestamp=entry_timestamp, entry_sequence=entry_sequence)
        if (
            entry_sequence is None
            and entry_id[0] == value.last_generated_entry_id[0]
            and value.last_generated_entry_id[1] == UINT64_MAX
        ) or entry_id <= value.last_generated_entry_id:
            raise ServerError(b"ERR The ID specified in XADD is equal or smaller than the target stream top item")
        try:
            value.add(
                self.field_value,
                entry_id,
            )
        except ValueError:
            raise ServerError(b"ERR Elements are too large to be stored")

        StreamTrim.trim(value, self.maximum_length, self.minimum_id, self.configuration.stream_node_max_entries)

        return _format_entry_id(entry_id)

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)


@command(b"help", {b"stream", b"write", b"fast"}, parent_command=b"xgroup")
class StreamGroupHelp(Command):
    def execute(self) -> ValueType:
        return [
            b"CREATE",
            b"Create a new stream group",
            b"SETID",
            b"Set the ID of the stream group",
            b"GROUPS",
            b"Get information about groups in the stream",
            b"STREAM",
            b"Get information about the stream itself",
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

        if not maximum_inclusive and maximum_timestamp == 0 and maximum_timestamp == 0:
            raise ServerError(b"ERR invalid end ID for the interval")

        entries = value.range(
            minimum_timestamp=minimum_timestamp,
            minimum_sequence=minimum_sequence,
            maximum_timestamp=maximum_timestamp,
            maximum_sequence=maximum_sequence,
            minimum_inclusive=minimum_inclusive,
            maximum_inclusive=maximum_inclusive,
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
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    stream_id: bytes = positional_parameter()
    make_stream: bool = keyword_parameter(flag=b"MKSTREAM", default=False)

    def execute(self) -> ValueType:
        return None


@command(b"setid", {b"write", b"stream", b"slow"}, b"xgroup")
class StreamGroupSetId(Command):
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    stream_id: bytes = positional_parameter()
    entries_read: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return None


@command(b"groups", {b"stream", b"write", b"fast"}, b"xinfo")
class StreamInfoGroups(Command):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


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
            if last_id < value.last_generated_entry_id:
                raise ServerError(b"ERR The ID specified in XSETID is smaller than the target stream top item")

            if self.entries_added is not None and self.entries_added < value.added_entries:
                raise ServerError(b"ERR The entries_added specified in XSETID is smaller than the target stream length")

        value.last_generated_entry_id = last_id
        if self.entries_added is not None:
            value.added_entries = self.entries_added
        if max_deleted_entry_id is not None:
            value.max_deleted_entry_id = max_deleted_entry_id

        return RESP_OK


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

        return [
            b"length",
            len(value),
            b"radix-tree-keys",
            len(value),
            b"radix-tree-nodes",
            math.ceil(len(value) / self.configuration.stream_node_max_entries) if len(value) > 0 else 1,
            b"last-generated-id",
            _format_entry_id(value.last_generated_entry_id),
            b"max-deleted-entry-id",
            _format_entry_id(value.max_deleted_entry_id),
            b"entries-added",
            value.added_entries,
            b"recorded-first-entry-id",
            _format_entry_id(value.entries.peekitem(0)[0]) if value.entries else b"0-0",
        ]


def _decrease_entry_id(entry_id: EntryID) -> EntryID:
    timestamp, sequence = entry_id

    if sequence == 0:
        if timestamp == 0:
            raise ValueError("Cannot decrease entry ID below 0-0")
        timestamp -= 1
        sequence = UINT64_MAX
    else:
        sequence -= 1

    return timestamp, sequence


@command(b"xread", {b"stream"})
class StreamRead(Command):
    database: Database = dependency()
    blocking_manager: StreamBlockingManager = dependency()
    client_context: ClientContext = dependency()

    count: int | None = keyword_parameter(flag=b"COUNT", default=None)
    block_milliseconds: int | None = keyword_parameter(flag=b"BLOCK", default=None)
    keys_and_ids: list[bytes] = keyword_parameter(token=b"STREAMS")

    _keys_to_ids: dict[bytes, EntryID | bytes] = field(default_factory=dict, init=False)

    had_keys: bool = field(default=False, init=False)

    async def before(self, in_multi: bool = False) -> None:
        if len(self.keys_and_ids) % 2 != 0:
            raise ServerError(b"ERR wrong number of arguments for 'xread' command")

        keys = self.keys_and_ids[0 : len(self.keys_and_ids) // 2]
        ids = self.keys_and_ids[len(self.keys_and_ids) // 2 :]

        for key, id_ in zip(keys, ids):
            if id_ in {b"$", b"+"}:
                self._keys_to_ids[key] = id_
                continue

            timestamp, sequence = _parse_entry_id(id_)

            if timestamp is None:
                raise ServerError(b"ERR wrong type of argument for 'xread' command")
            if sequence is None:
                sequence = 0

            self._keys_to_ids[key] = (timestamp, sequence)

        start_timestamp = time.time()
        try:
            self.client_context.current_client.blocking_queue = Queue()
            while not self.block_milliseconds or time.time() < (start_timestamp + (self.block_milliseconds / 1000)):
                for key, key_id in self._keys_to_ids.items():
                    value = self.database.stream_database.get_value_or_empty(key)

                    if key_id == b"$":
                        if value.entries:
                            self._keys_to_ids[key] = value.entries.peekitem(-1)[0]
                        else:
                            self._keys_to_ids[key] = (0, 0)
                        continue
                    elif key_id == b"+":
                        if value.entries:
                            self._keys_to_ids[key] = _decrease_entry_id(value.entries.peekitem()[0])
                            self.had_keys = True
                        else:
                            self._keys_to_ids[key] = (0, 0)
                        continue
                    elif isinstance(key_id, bytes):
                        raise ValueError()

                    if value.after(key_id) is not None:
                        self.had_keys = True
                        continue

                if self.had_keys or self.block_milliseconds is None:
                    break

                await asyncio.sleep(0.1)

                timeout = None
                if self.block_milliseconds is not None:
                    timeout = (
                        ((start_timestamp + (self.block_milliseconds / 1000)) - time.time())
                        if self.block_milliseconds != 0
                        else 0
                    )

                if timeout < 0:
                    break

                await self.blocking_manager.wait_for_lists(
                    self.client_context,
                    list(self._keys_to_ids.keys()),
                    timeout,
                    in_multi=self.block_milliseconds is None,
                    clear_queue=False,
                )

        finally:
            self.client_context.current_client.blocking_queue = None

    def execute(self) -> ValueType:
        if not self.had_keys:
            return None

        result = []
        for key, entry_id in self._keys_to_ids.items():
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
    group: bytes = keyword_parameter(token=b"GROUP")  # todo: should be positional
    consumer: bytes = positional_parameter()

    count: int | None = keyword_parameter(flag=b"COUNT", default=None)
    block: int | None = keyword_parameter(flag=b"BLOCK", default=None)
    no_ack: bool = keyword_parameter(flag=b"NOACK", default=False)
    keys_and_ids: list[bytes] = keyword_parameter(token=b"STREAMS")

    def execute(self) -> ValueType:
        return None


@parameters_object
class ExtendedPendingParameters:
    minimum_idle_time: int | None = keyword_parameter(token=b"IDLE", default=None)
    start: bytes = positional_parameter()
    end: bytes = positional_parameter()
    count: int = positional_parameter()
    consumer: bytes | None = positional_parameter(default=None)


@command(b"xpending", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupPending(Command):
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    extended_parameters: ExtendedPendingParameters | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return None


@command(b"xack", {b"write", b"stream", b"slow", b"blocking"})
class StreamGroupAcknoledge(Command):
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()
    ids: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return None


@command(b"destroy", {b"write", b"stream", b"slow", b"blocking"}, parent_command=b"xgroup")
class StreamGroupDestroy(Command):
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return None
