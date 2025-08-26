from __future__ import annotations

import time
from unittest.mock import Mock

import pytest

from pyvalkey.commands.stream_commands import (
    ExtendedPendingParameters,
    MaxLength,
    StreamAdd,
    StreamGroupPending,
    StreamGroupRead,
    StreamGroupSetId,
)
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, DatabaseContent, KeyValue, StreamBlockingManager
from pyvalkey.database_objects.stream import Consumer, ConsumerGroup, PendingEntry, Stream


class BaseStreamTest:
    content: DatabaseContent
    database: Database
    blocking_manager: StreamBlockingManager
    client_context: Mock
    configurations: Configurations

    @pytest.fixture(autouse=True, scope="function")
    def setup_test(self):
        self.content = DatabaseContent()
        self.database = Database(0, self.content)
        self.blocking_manager = StreamBlockingManager()
        self.client_context = Mock()
        self.configurations = Configurations()


class TestStreamGroupRead(BaseStreamTest):
    def test_parse(self):
        assert StreamGroupRead.parse([b"GROUP", b"g1", b"Alice", b"COUNT", b"1", b"STREAMS", b"x{t}", b">"]) == {
            "group": b"g1",
            "consumer": b"Alice",
            "count": 1,
            "keys_and_ids": [b"x{t}", b">"],
        }

    @pytest.mark.asyncio
    async def test_execute(self):
        my_stream = Stream()
        self.content.data[b"mystream"] = KeyValue(b"mystream", my_stream)

        my_group = my_stream.consumer_groups[b"mygroup"] = ConsumerGroup(b"mygroup", (0, 0))

        ###

        my_stream.entries[(1, 0)] = {b"a": b"1"}
        my_stream.entries[(2, 0)] = {b"b": b"2"}

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"consumer-1",
            keys_and_ids=[b"mystream", b">"],
        )
        await command.before()
        result = command.execute()

        timestamp = int(time.time() * 1000)
        assert result == [
            [
                b"mystream",
                [
                    [b"1-0", {b"a": b"1"}],
                    [b"2-0", {b"b": b"2"}],
                ],
            ],
        ]

        consumer_1 = my_group.consumers[b"consumer-1"]

        expected_pending_ids = {
            (1, 0),
            (2, 0),
        }
        assert len(consumer_1.pending_entries) == 2
        for entry_id, pending_entry in consumer_1.pending_entries.items():
            assert entry_id in expected_pending_ids
            assert entry_id in my_group.pending_entries
            assert my_group.pending_entries[entry_id] == pending_entry

            assert pending_entry.consumer == consumer_1
            assert pending_entry.times_delivered == 1
            assert pending_entry.last_delivery >= timestamp

        ###

        my_stream.entries[(3, 0)] = {b"c": b"3"}
        my_stream.entries[(4, 0)] = {b"d": b"4"}

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"consumer-2",
            keys_and_ids=[b"mystream", b">"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"mystream",
                [
                    [b"3-0", {b"c": b"3"}],
                    [b"4-0", {b"d": b"4"}],
                ],
            ],
        ]

        consumer_2 = my_group.consumers[b"consumer-2"]

        expected_pending_ids = {
            (3, 0),
            (4, 0),
        }
        assert len(consumer_2.pending_entries) == 2
        for entry_id, pending_entry in consumer_2.pending_entries.items():
            assert entry_id in expected_pending_ids
            assert entry_id in my_group.pending_entries
            assert my_group.pending_entries[entry_id] == pending_entry

            assert pending_entry.consumer == consumer_2
            assert pending_entry.times_delivered == 1
            assert pending_entry.last_delivery >= timestamp

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"consumer-1",
            count=10,
            keys_and_ids=[b"mystream", b"0"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"mystream",
                [
                    [b"1-0", {b"a": b"1"}],
                    [b"2-0", {b"b": b"2"}],
                ],
            ],
        ]

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"consumer-2",
            count=10,
            keys_and_ids=[b"mystream", b"0"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"mystream",
                [
                    [b"3-0", {b"c": b"3"}],
                    [b"4-0", {b"d": b"4"}],
                ],
            ],
        ]

    @pytest.mark.asyncio
    async def test_execute_will_not_report_data_on_empty_history(self):
        my_stream = Stream()
        self.content.data[b"events"] = KeyValue(b"events", my_stream)
        my_stream.consumer_groups[b"mygroup"] = ConsumerGroup(b"mygroup", (0, 0))

        my_stream.entries[(1, 0)] = {b"a": b"1"}
        my_stream.entries[(2, 0)] = {b"b": b"2"}
        my_stream.entries[(3, 0)] = {b"c": b"3"}

        ###

        result = StreamGroupPending(
            database=self.database,
            key=b"events",
            group=b"mygroup",
            extended_parameters=ExtendedPendingParameters(start=b"-", end=b"+", count=10),
        ).execute()

        assert result == []

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"myconsumer",
            count=3,
            keys_and_ids=[b"events", b"0"],
        )
        await command.before()
        result = command.execute()

        assert result == [[b"events", []]]

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"myconsumer",
            count=3,
            keys_and_ids=[b"events", b">"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"events",
                [
                    [b"1-0", {b"a": b"1"}],
                    [b"2-0", {b"b": b"2"}],
                    [b"3-0", {b"c": b"3"}],
                ],
            ]
        ]

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"myconsumer",
            count=3,
            keys_and_ids=[b"events", b"0"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"events",
                [
                    [b"1-0", {b"a": b"1"}],
                    [b"2-0", {b"b": b"2"}],
                    [b"3-0", {b"c": b"3"}],
                ],
            ]
        ]

    @pytest.mark.asyncio
    async def test_execute_history_reporting_of_deleted_entries(self):
        my_stream = Stream()
        self.content.data[b"mystream"] = KeyValue(b"mystream", my_stream)
        my_stream.consumer_groups[b"mygroup"] = ConsumerGroup(b"mygroup", (0, 0))

        my_stream.entries[(1, 0)] = {b"field1": b"A"}

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"myconsumer",
            keys_and_ids=[b"mystream", b">"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"mystream",
                [
                    [b"1-0", {b"field1": b"A"}],
                ],
            ]
        ]

        ###

        command = StreamAdd(
            database=self.database,
            blocking_manager=self.blocking_manager,
            configuration=self.configurations,
            key=b"mystream",
            stream_id=b"2",
            maximum_length=MaxLength(threshold=1),
            field_value=[(b"field1", b"B")],
        )
        result = command.execute()

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"myconsumer",
            keys_and_ids=[b"mystream", b">"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"mystream",
                [
                    [b"2-0", {b"field1": b"B"}],
                ],
            ]
        ]

        ###

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=self.blocking_manager,
            client_context=self.client_context,
            group=b"mygroup",
            consumer=b"myconsumer",
            keys_and_ids=[b"mystream", b"0-1"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"mystream",
                [
                    [b"1-0", {}],
                    [b"2-0", {b"field1": b"B"}],
                ],
            ]
        ]


class TestStreamGroupPending(BaseStreamTest):
    def test_execute(self):
        my_stream = Stream()
        self.content.data[b"mystream"] = KeyValue(b"mystream", my_stream)

        my_stream.entries[(1, 0)] = {b"a": b"1"}
        my_stream.entries[(2, 0)] = {b"b": b"2"}
        my_stream.entries[(3, 0)] = {b"c": b"3"}
        my_stream.entries[(4, 0)] = {b"d": b"4"}

        my_group = my_stream.consumer_groups[b"mygroup"] = ConsumerGroup(b"mygroup", (0, 0))

        consumer_1 = my_group.consumers[b"consumer-1"] = Consumer(b"consumer-1")
        consumer_2 = my_group.consumers[b"consumer-2"] = Consumer(b"consumer-2")

        consumer_1.pending_entries[(0, 1)] = my_group.pending_entries[(1, 0)] = PendingEntry(
            consumer=consumer_1, times_delivered=1, last_delivery=int(time.time() * 1000)
        )
        consumer_1.pending_entries[(0, 2)] = my_group.pending_entries[(2, 0)] = PendingEntry(
            consumer=consumer_1, times_delivered=1, last_delivery=int(time.time() * 1000)
        )
        consumer_2.pending_entries[(0, 3)] = my_group.pending_entries[(3, 0)] = PendingEntry(
            consumer=consumer_2, times_delivered=1, last_delivery=int(time.time() * 1000)
        )
        consumer_2.pending_entries[(0, 4)] = my_group.pending_entries[(4, 0)] = PendingEntry(
            consumer=consumer_2, times_delivered=1, last_delivery=int(time.time() * 1000)
        )

        ###

        result = StreamGroupPending(
            database=self.database,
            key=b"mystream",
            group=b"mygroup",
            extended_parameters=ExtendedPendingParameters(start=b"-", end=b"+", count=10),
        ).execute()

        assert result == [
            [
                b"1-0",
                b"consumer-1",
                int(time.time() * 1000) - consumer_1.pending_entries[(0, 1)].last_delivery,
                consumer_1.pending_entries[(0, 1)].times_delivered,
            ],
            [
                b"2-0",
                b"consumer-1",
                int(time.time() * 1000) - consumer_1.pending_entries[(0, 2)].last_delivery,
                consumer_1.pending_entries[(0, 2)].times_delivered,
            ],
            [
                b"3-0",
                b"consumer-2",
                int(time.time() * 1000) - consumer_2.pending_entries[(0, 3)].last_delivery,
                consumer_2.pending_entries[(0, 3)].times_delivered,
            ],
            [
                b"4-0",
                b"consumer-2",
                int(time.time() * 1000) - consumer_2.pending_entries[(0, 4)].last_delivery,
                consumer_2.pending_entries[(0, 4)].times_delivered,
            ],
        ]

        ###

        result = StreamGroupPending(
            database=self.database,
            key=b"mystream",
            group=b"mygroup",
        ).execute()

        assert result == [
            4,
            b"1-0",
            b"4-0",
            [[b"consumer-1", 2], [b"consumer-2", 2]],
        ]


class TestStreamGroupSetId(BaseStreamTest):
    @pytest.mark.asyncio
    async def test_execute(self):
        my_stream = Stream()
        self.content.data[b"events"] = KeyValue(b"events", my_stream)

        my_stream.entries[(1, 0)] = {b"f1": b"v1"}
        my_stream.entries[(2, 0)] = {b"f1": b"v1"}
        my_stream.entries[(3, 0)] = {b"f1": b"v1"}
        my_stream.entries[(4, 0)] = {b"f1": b"v1"}
        my_stream.entries[(5, 0)] = {b"f1": b"v1"}

        my_stream.consumer_groups[b"g1"] = ConsumerGroup(b"g1", (4, 0))

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=StreamBlockingManager(),
            client_context=Mock(),
            group=b"g1",
            consumer=b"c1",
            keys_and_ids=[b"events", b">"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"events",
                [
                    [b"5-0", {b"f1": b"v1"}],
                ],
            ],
        ]

        StreamGroupSetId(
            database=self.database,
            key=b"events",
            group=b"g1",
            stream_id=b"-",
        ).execute()

        command = StreamGroupRead(
            database=self.database,
            blocking_manager=StreamBlockingManager(),
            client_context=Mock(),
            group=b"g1",
            consumer=b"c2",
            keys_and_ids=[b"events", b">"],
        )
        await command.before()
        result = command.execute()

        assert result == [
            [
                b"events",
                [
                    [b"1-0", {b"f1": b"v1"}],
                    [b"2-0", {b"f1": b"v1"}],
                    [b"3-0", {b"f1": b"v1"}],
                    [b"4-0", {b"f1": b"v1"}],
                    [b"5-0", {b"f1": b"v1"}],
                ],
            ],
        ]
