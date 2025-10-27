from __future__ import annotations

import itertools
import math
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TypeVar

from sortedcontainers import SortedDict

from pyvalkey.consts import UINT64_MAX

EntryID = tuple[int, int]
EntryData = dict[bytes, bytes]
EntryType = tuple[EntryID, EntryData]


@dataclass
class PendingEntry:
    consumer: Consumer

    last_delivery: int = 0
    times_delivered: int = 0


GenericEntryData = TypeVar("GenericEntryData", bound=EntryData | PendingEntry)


def range_entries(
    entries: SortedDict[EntryID, GenericEntryData],
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

    for entry_id in itertools.islice(iterator, 0, count):
        yield entry_id, entries[entry_id]


@dataclass
class Consumer:
    name: bytes
    last_seen_timestamp: int = 0
    last_active_timestamp: int = 0

    pending_entries: SortedDict[EntryID, PendingEntry] = field(default_factory=SortedDict)


@dataclass
class ConsumerGroup:
    name: bytes
    last_id: EntryID
    read_entries: int = -1

    consumers: dict[bytes, Consumer] = field(default_factory=dict)
    pending_entries: SortedDict[EntryID, PendingEntry] = field(default_factory=SortedDict)


class Stream:
    consumer_groups: dict[bytes, ConsumerGroup]

    def __init__(self, entries: Iterable[EntryType] | None = None) -> None:
        self.entries: SortedDict[EntryID, EntryData] = SortedDict({key: data for key, data in entries or []})
        self.consumer_groups: dict[bytes, ConsumerGroup] = {}

        self.added_entries = 0
        self.last_generated_entry_id: EntryID = (0, 0)
        self.max_deleted_entry_id: EntryID = (0, 0)

    def generate_entry_id(
        self,
        entry_timestamp: int | None = None,
        entry_sequence: int | None = None,
    ) -> EntryID:
        if entry_sequence is not None and entry_timestamp is None:
            raise ValueError("If entry_sequence is provided, entry_timestamp must also be provided.")

        if entry_timestamp is None:
            entry_timestamp = int(time.time() * 1000)
            if entry_timestamp <= self.last_generated_entry_id[0]:
                entry_timestamp = self.last_generated_entry_id[0] + 1

        if entry_sequence is None:
            entry_sequence = 0
            if self.entries:
                last_entry_timestamp, last_entry_sequence = self.entries.peekitem()[0]
                if last_entry_timestamp == entry_timestamp:
                    entry_sequence = last_entry_sequence + 1
            elif entry_timestamp == 0:
                entry_sequence = 1

        return entry_timestamp, entry_sequence

    def _trim(self, to_delete: int, limit: int | None = None) -> int:
        length = len(self.entries)

        to_delete = min(to_delete, length, limit if limit is not None else to_delete)
        for _ in range(to_delete):
            entry_id, _ = self.entries.popitem(0)
            self.max_deleted_entry_id = max(self.max_deleted_entry_id, entry_id)

        return length - len(self.entries)

    def calculate_trim_approximate_max_length_to_delete(
        self, threshold: int, node_max_entries: int, limit: int | None
    ) -> int:
        threshold_node_size = math.ceil(threshold / node_max_entries) * node_max_entries
        if len(self.entries) > threshold_node_size:
            if len(self.entries) - threshold_node_size < node_max_entries:
                to_delete = len(self.entries) - threshold_node_size
            else:
                to_delete = math.floor((len(self.entries) - threshold_node_size) / node_max_entries) * node_max_entries
        else:
            to_delete = math.floor((len(self.entries) - threshold) / node_max_entries) * node_max_entries

        if limit is not None and to_delete > limit:
            if limit < node_max_entries:
                to_delete = 0
            else:
                to_delete = math.floor(limit / node_max_entries) * node_max_entries

        return to_delete

    def calculate_trim_max_length_to_delete(self, threshold: int) -> int:
        return len(self.entries) - threshold

    def trim_maximum_length(self, threshold: int, approximate: bool, limit: int | None, node_max_entries: int) -> int:
        if len(self.entries) <= threshold:
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

        length = len(self.entries)
        for _ in range(min(to_delete, length, limit if limit is not None else to_delete)):
            entry_id, _ = self.entries.popitem(0)
            self.max_deleted_entry_id = max(self.max_deleted_entry_id, entry_id)

        return length - len(self.entries)

    def add(
        self,
        data: Iterable[tuple[bytes, bytes]],
        entry_id: EntryID,
    ) -> EntryID:
        self.last_generated_entry_id = max(self.last_generated_entry_id, entry_id)
        self.entries[entry_id] = dict(data)
        self.added_entries += 1

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

        del self.entries[entry_id]
        self.max_deleted_entry_id = max(self.max_deleted_entry_id, entry_id)

        return True

    def __len__(self) -> int:
        return len(self.entries)

    def after(self, minimum_entry_id: EntryID) -> EntryType | None:
        iterator = self.entries.irange(
            (minimum_entry_id[0], minimum_entry_id[1]),  # Start after the given entry ID
            inclusive=(False, True),
        )

        entry_id = next(iterator, None)
        return None if entry_id is None else (entry_id, self.entries[entry_id])

    def after_non_pending(self, minimum_entry_id: EntryID, group: ConsumerGroup) -> EntryType | None:
        iterator = self.entries.irange(
            (minimum_entry_id[0], minimum_entry_id[1]),  # Start after the given entry ID
            inclusive=(False, True),
        )

        for entry_id in iterator:
            if entry_id not in group.pending_entries:
                return entry_id, self.entries[entry_id]

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
