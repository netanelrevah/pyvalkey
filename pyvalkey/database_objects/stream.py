from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Self, TypeVar

from sortedcontainers import SortedDict

from pyvalkey.consts import UINT64_MAX
from pyvalkey.utils.times import now_ms

EntryID = tuple[int, int]
EntryData = dict[bytes, bytes]
EntryType = tuple[EntryID, EntryData]


@dataclass
class PendingEntry:
    consumer: Consumer

    last_delivery: int = 0
    times_delivered: int = 0

    def dump(self) -> dict:
        return {
            "consumer_name": self.consumer.name.decode(),
            "last_delivery": self.last_delivery,
            "times_delivered": self.times_delivered,
        }

    @classmethod
    def restore(cls, value: dict, consumer: Consumer) -> Self:
        return cls(
            consumer=consumer,
            last_delivery=value["last_delivery"],
            times_delivered=value["times_delivered"],
        )


GenericEntryData = TypeVar("GenericEntryData", bound=EntryData | PendingEntry)


def range_entries(
    entries: SortedDict[EntryID, GenericEntryData] | SortedDict[EntryID, GenericEntryData | None],
    minimum_timestamp: int | None = None,
    minimum_sequence: int | None = None,
    maximum_timestamp: int | None = None,
    maximum_sequence: int | None = None,
    minimum_inclusive: bool = True,
    maximum_inclusive: bool = True,
    count: int | None = None,
    is_reversed: bool = False,
) -> Iterable[tuple[EntryID, GenericEntryData]]:
    if minimum_timestamp is None and minimum_sequence is None:
        minimum_timestamp = 0
        minimum_sequence = 0
    elif minimum_timestamp is None:
        raise ValueError()
    elif minimum_sequence is None:
        minimum_sequence = 0 if minimum_inclusive else UINT64_MAX

    if maximum_timestamp is None and maximum_sequence is None:
        maximum_timestamp = UINT64_MAX
        maximum_sequence = UINT64_MAX
    elif maximum_timestamp is None:
        raise ValueError()
    elif maximum_sequence is None:
        maximum_sequence = UINT64_MAX if maximum_inclusive else 0

    iterator = entries.irange(
        (minimum_timestamp, minimum_sequence),
        (maximum_timestamp, maximum_sequence),
        (minimum_inclusive, maximum_inclusive),
        reverse=is_reversed,
    )

    yielded = 0
    while count is None or yielded < count:
        entry_id = next(iterator, None)
        if entry_id is None:
            break
        entry_data = entries[entry_id]
        if entry_data is not None:
            yield entry_id, entry_data
            yielded += 1


@dataclass
class Consumer:
    name: bytes
    last_seen_timestamp: int = field(default_factory=lambda: now_ms())
    last_active_timestamp: int | None = None

    pending_entries: SortedDict[EntryID, PendingEntry] = field(default_factory=SortedDict)

    def dump(self) -> dict:
        return {
            "name": self.name.decode(),
            "last_seen_timestamp": self.last_seen_timestamp,
            "last_active_timestamp": self.last_active_timestamp,
            "pending_entries": [
                {
                    "entry_id_timestamp": entry_id[0],
                    "entry_id_sequence": entry_id[1],
                    "pending_entry": pending_entry.dump(),
                }
                for entry_id, pending_entry in self.pending_entries.items()
            ],
        }

    @classmethod
    def restore(cls, value: dict) -> Self:
        consumer = cls(
            name=value["name"].encode(),
            last_seen_timestamp=value["last_seen_timestamp"],
            last_active_timestamp=value["last_active_timestamp"],
        )
        for item in value["pending_entries"]:
            entry_id = (item["entry_id_timestamp"], item["entry_id_sequence"])
            pending_entry = PendingEntry.restore(item["pending_entry"], consumer)
            consumer.pending_entries[entry_id] = pending_entry
        return consumer


@dataclass
class ConsumerGroup:
    name: bytes
    last_id: EntryID
    read_entries: int = -1

    consumers: dict[bytes, Consumer] = field(default_factory=dict)
    pending_entries: SortedDict[EntryID, PendingEntry] = field(default_factory=SortedDict)

    def dump(self) -> dict:
        return {
            "name": self.name.decode(),
            "last_id": {"timestamp": self.last_id[0], "sequence": self.last_id[1]},
            "read_entries": self.read_entries,
            "consumers": {name.decode(): consumer.dump() for name, consumer in self.consumers.items()},
            "pending_entries": [
                {
                    "entry_id_timestamp": entry_id[0],
                    "entry_id_sequence": entry_id[1],
                    "pending_entry": pending_entry.dump(),
                }
                for entry_id, pending_entry in self.pending_entries.items()
            ],
        }

    @classmethod
    def restore(cls, value: dict) -> Self:
        group = cls(
            name=value["name"].encode(),
            last_id=(value["last_id"]["timestamp"], value["last_id"]["sequence"]),
            read_entries=value["read_entries"],
        )
        for name, consumer_value in value["consumers"].items():
            group.consumers[name.encode()] = Consumer.restore(consumer_value)
        for item in value["pending_entries"]:
            entry_id = (item["entry_id_timestamp"], item["entry_id_sequence"])
            consumer_name = item["pending_entry"]["consumer_name"]
            consumer = group.consumers[consumer_name.encode()]
            pending_entry = PendingEntry.restore(item["pending_entry"], consumer)
            group.pending_entries[entry_id] = pending_entry
        return group


class Stream:
    consumer_groups: dict[bytes, ConsumerGroup]

    def __init__(self, entries: Iterable[EntryType] | None = None) -> None:
        self.entries: SortedDict[EntryID, EntryData | None] = SortedDict({key: data for key, data in entries or []})
        self.consumer_groups: dict[bytes, ConsumerGroup] = {}

        self.added_entries = 0
        self.max_deleted_entry_id: EntryID = (0, 0)

        self._length = sum(1 for data in self.entries.values() if data is not None)
        self.first_id = (0, 0)
        self.last_id = (0, 0)

    def generate_entry_id(
        self,
        entry_timestamp: int | None = None,
        entry_sequence: int | None = None,
    ) -> EntryID:
        if entry_sequence is not None and entry_timestamp is None:
            raise ValueError("If entry_sequence is provided, entry_timestamp must also be provided.")

        if entry_timestamp is None:
            entry_timestamp = now_ms()
            if entry_timestamp <= self.last_id[0]:
                entry_timestamp = self.last_id[0] + 1

        if entry_sequence is None:
            entry_sequence = 0
            if self._length > 0:
                last_entry_timestamp, last_entry_sequence = self.entries.peekitem()[0]
                if last_entry_timestamp == entry_timestamp:
                    entry_sequence = last_entry_sequence + 1
            elif entry_timestamp == 0:
                entry_sequence = 1

        return entry_timestamp, entry_sequence

    def _trim(self, to_delete: int, limit: int | None = None) -> int:
        length = self._length

        to_delete = min(to_delete, length, limit if limit is not None else to_delete)

        deleted = 0
        for entry_id in self.entries:
            if deleted >= to_delete:
                break
            if self.entries[entry_id] is not None:
                self.remove(entry_id, update_max_deleted=False)
                deleted += 1

        return length - self._length

    def first_entry_id(self) -> EntryType:
        for entry_id in self.entries:
            entry_data = self.entries[entry_id]
            if entry_data is not None:
                return entry_id, entry_data
        return (0, 0), {}

    def last_entry_id(self) -> EntryType:
        for entry_id in reversed(self.entries.keys()):
            entry_data = self.entries[entry_id]
            if entry_data is not None:
                return entry_id, entry_data
        return (0, 0), {}

    def calculate_trim_approximate_max_length_to_delete(
        self, threshold: int, node_max_entries: int, limit: int | None
    ) -> int:
        threshold_node_size = math.ceil(threshold / node_max_entries) * node_max_entries
        if self._length > threshold_node_size:
            if self._length - threshold_node_size < node_max_entries:
                to_delete = self._length - threshold_node_size
            else:
                to_delete = math.floor((self._length - threshold_node_size) / node_max_entries) * node_max_entries
        else:
            to_delete = math.floor((self._length - threshold) / node_max_entries) * node_max_entries

        if limit is not None and to_delete > limit:
            if limit < node_max_entries:
                to_delete = 0
            else:
                to_delete = math.floor(limit / node_max_entries) * node_max_entries

        return to_delete

    def calculate_trim_max_length_to_delete(self, threshold: int) -> int:
        return self._length - threshold

    def trim_maximum_length(self, threshold: int, approximate: bool, limit: int | None, node_max_entries: int) -> int:
        if self._length <= threshold:
            return 0

        if approximate:
            to_delete = self.calculate_trim_approximate_max_length_to_delete(threshold, node_max_entries, limit)
        else:
            to_delete = self.calculate_trim_max_length_to_delete(threshold)

        return self._trim(to_delete, limit)

    def trim_minimum_id(
        self,
        threshold_timestamp: int,
        threshold_sequence: int | None,
        approximate: bool,
        limit: int | None,
        node_max_entries: int,
    ) -> int:
        to_delete = sum(
            1
            for _ in self.range(
                maximum_timestamp=threshold_timestamp,
                maximum_sequence=threshold_sequence,
                maximum_inclusive=False,
            )
        )

        length = self._length

        to_delete = min(to_delete, length, limit if limit is not None else to_delete)
        deleted = 0
        for entry_id in self.entries:
            if deleted >= to_delete:
                break
            if self.entries[entry_id] is not None:
                self.remove(entry_id, update_max_deleted=False)
                deleted += 1

        return length - self._length

    def remove(self, entry_id: EntryID, update_max_deleted: bool = True) -> None:
        self.entries[entry_id] = None
        if update_max_deleted:
            self.max_deleted_entry_id = max(self.max_deleted_entry_id, entry_id)
        self._length -= 1
        if self._length == 0:
            self.first_id = (0, 0)
        elif self.first_id == entry_id:
            self.first_id = self.first_entry_id()[0]

    def add(
        self,
        data: Iterable[tuple[bytes, bytes]],
        entry_id: EntryID,
    ) -> EntryID:
        self.entries[entry_id] = dict(data)
        self.added_entries += 1

        self._length += 1
        self.last_id = entry_id
        if self._length == 1:
            self.first_id = entry_id

        return entry_id

    def delete(self, timestamp: int, sequence: int | None = None) -> bool:
        if sequence is None:
            iterator = self.range(
                minimum_timestamp=timestamp,
                minimum_sequence=0,
                maximum_timestamp=timestamp,
                maximum_sequence=UINT64_MAX,
                count=1,
            )
            entry = next(iter(iterator), None)
            if entry is None:
                return False
            entry_id = entry[0]
        else:
            entry_id = (timestamp, sequence)

        if entry_id not in self.entries:
            return False

        if self.entries[entry_id] is None:
            return False

        self.remove(entry_id)

        return True

    def __len__(self) -> int:
        return self._length

    def after(self, minimum_entry_id: EntryID) -> EntryType | None:
        entries = self.entries

        iterator = entries.irange(
            (minimum_entry_id[0], minimum_entry_id[1]),  # Start after the given entry ID
            inclusive=(False, True),
        )

        for entry_id in iterator:
            entry_data = entries[entry_id]
            if entry_data is not None:
                return entry_id, entry_data
        return None

    def after_non_pending(self, minimum_entry_id: EntryID, group: ConsumerGroup) -> EntryType | None:
        iterator = self.entries.irange(
            (minimum_entry_id[0], minimum_entry_id[1]),  # Start after the given entry ID
            inclusive=(False, True),
        )

        for entry_id in iterator:
            entry_data = self.entries[entry_id]
            if entry_id not in group.pending_entries and entry_data is not None:
                return entry_id, entry_data
        return None

    def range_has_tombstones(
        self,
        start_entry_id: EntryID | None = None,
        end_entry_id: EntryID | None = None,
    ) -> bool:
        print(f"range_has_tombstones start_entry_id={start_entry_id} end_entry_id={end_entry_id} ")
        if self._length == 0 or self.max_deleted_entry_id == (0, 0):
            print(f"range_has_tombstones length={self._length} max_deleted_entry_id={self.max_deleted_entry_id}")
            return False

        start_entry_id = start_entry_id or (0, 0)
        end_entry_id = end_entry_id or (UINT64_MAX, UINT64_MAX)

        if start_entry_id <= self.max_deleted_entry_id <= end_entry_id:
            print(f"range_has_tombstones found tombstone max_deleted_entry_id={self.max_deleted_entry_id}")
            return True
        return False

    def update_group_last_id(self, group: ConsumerGroup, entry_id: EntryID) -> None:
        if entry_id > group.last_id:
            if (
                group.read_entries != -1
                and group.last_id >= self.first_id
                and not self.range_has_tombstones(start_entry_id=group.last_id)
            ):
                group.read_entries += 1
            elif self.added_entries != 0:
                estimate = self.estimate_distance_from_first_ever_entry(entry_id)
                group.read_entries = estimate if estimate is not None else -1
            group.last_id = entry_id

    def has_deleted(
        self,
        minimum_timestamp: int,
        minimum_sequence: int,
    ) -> bool:
        iterator = self.entries.irange(
            (minimum_timestamp, minimum_sequence),
            inclusive=(False, True),
        )

        for entry_id in iterator:
            entry_data = self.entries[entry_id]
            if entry_data is None:
                return True
        return False

    def estimate_distance_from_first_ever_entry(self, entry_id: EntryID) -> int | None:
        if self.added_entries == 0:
            return 0

        if self._length == 0 and entry_id <= self.last_id:
            return self.added_entries

        if entry_id != (0, 0) and entry_id < self.max_deleted_entry_id:
            return None

        if entry_id == self.last_id:
            return self.added_entries
        elif entry_id > self.last_id:
            return None

        if self.max_deleted_entry_id == (0, 0) or (self.max_deleted_entry_id < self.first_id):
            if entry_id < self.first_id:
                return self.added_entries - self._length
            if entry_id == self.first_id:
                return self.added_entries - self._length + 1

        return None

    def calculate_consumer_group_lag(self, group: ConsumerGroup) -> int | None:
        print(
            f"calculate_consumer_group_lag {group.name.decode()}: "
            f"last_id={group.last_id} read_entries={group.read_entries}"
        )
        if self.added_entries <= 0:
            print(f"calculate_consumer_group_lag {group.name.decode()}: no entries in stream")
            return 0

        if group.read_entries >= 0 and not self.range_has_tombstones(start_entry_id=group.last_id):
            print(f"calculate_consumer_group_lag {group.name.decode()}: added_entries={self.added_entries}")
            return self.added_entries - group.read_entries

        estimate = self.estimate_distance_from_first_ever_entry(group.last_id)
        if estimate is not None:
            print(
                f"calculate_consumer_group_lag {group.name.decode()}: "
                f"added_entries={self.added_entries} estimate={estimate}"
            )
            return self.added_entries - estimate

        return None

    def range(
        self,
        minimum_timestamp: int | None = None,
        minimum_sequence: int | None = None,
        maximum_timestamp: int | None = None,
        maximum_sequence: int | None = None,
        minimum_inclusive: bool = True,
        maximum_inclusive: bool = True,
        count: int | None = None,
        is_reversed: bool = False,
    ) -> Iterable[EntryType]:
        yield from range_entries(
            self.entries,
            minimum_timestamp,
            minimum_sequence,
            maximum_timestamp,
            maximum_sequence,
            minimum_inclusive,
            maximum_inclusive,
            count,
            is_reversed,
        )

    def dump(self) -> dict:
        return {
            "entries": [
                {
                    "entry_id_timestamp": entry_id[0],
                    "entry_id_sequence": entry_id[1],
                    "data": {k.decode(): v.decode() for k, v in data.items()} if data is not None else None,
                }
                for entry_id, data in self.entries.items()
            ],
            "consumer_groups": {name.decode(): group.dump() for name, group in self.consumer_groups.items()},
            "added_entries": self.added_entries,
            "max_deleted_entry_id": {
                "timestamp": self.max_deleted_entry_id[0],
                "sequence": self.max_deleted_entry_id[1],
            },
            "first_id": {
                "timestamp": self.first_id[0],
                "sequence": self.first_id[1],
            },
            "last_id": {
                "timestamp": self.last_id[0],
                "sequence": self.last_id[1],
            },
        }

    @classmethod
    def restore(cls, value: dict) -> Self:
        stream = cls(
            entries=[
                (
                    (item["entry_id_timestamp"], item["entry_id_sequence"]),
                    {k.encode(): v.encode() for k, v in item["data"].items()},
                )
                for item in value["entries"]
            ]
        )
        stream.consumer_groups = {
            name.encode(): ConsumerGroup.restore(group_value) for name, group_value in value["consumer_groups"].items()
        }
        stream.added_entries = value["added_entries"]
        stream.max_deleted_entry_id = (
            value["max_deleted_entry_id"]["timestamp"],
            value["max_deleted_entry_id"]["sequence"],
        )
        stream.first_id = (
            value["first_id"]["timestamp"],
            value["first_id"]["sequence"],
        )
        stream.last_id = (
            value["last_id"]["timestamp"],
            value["last_id"]["sequence"],
        )
        return stream
