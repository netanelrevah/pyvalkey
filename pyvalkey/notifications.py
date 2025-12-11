from __future__ import annotations

import asyncio
import fnmatch
import typing
from dataclasses import dataclass, field

from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.enums import NOTIFICATION_TYPE_ALL, NotificationType
from pyvalkey.resp import ValueType
from pyvalkey.utils.collections import SetMap

if typing.TYPE_CHECKING:
    pass


@dataclass
class NotificationsManager:
    configurations: Configurations
    subscriptions_manager: SubscriptionsManager
    database_index: int

    def notify(self, notification_type: NotificationType, event: bytes, key: bytes) -> None:
        configured_notification_type = self.configurations.notify_keyspace_events
        has_keyspace = NotificationType.KEYSPACE in configured_notification_type
        if has_keyspace:
            configured_notification_type = configured_notification_type.replace(NotificationType.KEYSPACE, b"")
        has_key_event = NotificationType.KEYEVENT in configured_notification_type
        if has_key_event:
            configured_notification_type = configured_notification_type.replace(NotificationType.KEYEVENT, b"")
        configured_notification_type = configured_notification_type.replace(
            NotificationType.ALL, b"".join(NOTIFICATION_TYPE_ALL)
        )

        if notification_type not in configured_notification_type:
            return

        if has_keyspace:
            channel = f"__keyspace@{self.database_index!s}__:{key.decode()}".encode()
            self.subscriptions_manager.publish(channel, event)

        if has_key_event:
            channel = f"__keyevent@{self.database_index!s}__:{event.decode()}".encode()
            self.subscriptions_manager.publish(channel, key)


@dataclass
class SubscriptionsManager:
    channels_queues: SetMap[bytes, asyncio.Queue[ValueType]] = field(default_factory=SetMap)
    patterns_queues: SetMap[bytes, asyncio.Queue[ValueType]] = field(default_factory=SetMap)

    def publish(self, channel: bytes, message: bytes) -> int:
        receivers = 0
        for queue in self.channels_queues.iter_values(channel):
            queue.put_nowait(["message", channel, message])
            receivers += 1

        for patten in self.patterns_queues.iter_keys():
            if fnmatch.fnmatch(channel, patten):
                for queue in self.patterns_queues.iter_values(patten):
                    queue.put_nowait(["pmessage", patten, channel, message])
                    receivers += 1
        return receivers


@dataclass
class ClientSubscriptions:
    queue: asyncio.Queue[ValueType]
    subscriptions_manager: SubscriptionsManager

    subscribed_channels: set[bytes] = field(default_factory=set)
    subscribed_patterns: set[bytes] = field(default_factory=set)

    @property
    def active_subscriptions(self) -> int:
        return len(self.subscribed_channels) + len(self.subscribed_patterns)

    def subscribe_channel(self, channel: bytes) -> None:
        self.subscribed_channels.add(channel)
        self.subscriptions_manager.channels_queues.add(channel, self.queue)

    def unsubscribe_channel(self, channel: bytes) -> None:
        if channel not in self.subscribed_channels:
            return
        self.subscribed_channels.remove(channel)
        self.subscriptions_manager.channels_queues.remove(channel, self.queue)

    def subscribe_pattern(self, pattern: bytes) -> None:
        self.subscribed_patterns.add(pattern)
        self.subscriptions_manager.patterns_queues.add(pattern, self.queue)

    def unsubscribe_pattern(self, pattern: bytes) -> None:
        if pattern not in self.subscribed_patterns:
            return
        self.subscribed_patterns.remove(pattern)
        self.subscriptions_manager.patterns_queues.remove(pattern, self.queue)

    def unsubscribe_all(self) -> None:
        for channel in list(self.subscribed_channels):
            self.unsubscribe_channel(channel)
        for pattern in list(self.subscribed_patterns):
            self.unsubscribe_pattern(pattern)

    def publish(self, head_message: ValueType, *tail_message: ValueType) -> None:
        message = head_message
        if len(tail_message) > 0:
            message = [head_message, *tail_message]

        self.queue.put_nowait(message)
