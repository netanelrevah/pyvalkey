from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.notifications import ClientSubscriptions, SubscriptionsManager
from pyvalkey.resp import BulkArray, DoNotReply, ValueType


@command(b"subscribe", {b"slow", b"connection"})
class Subscribe(Command):
    subscriptions: ClientSubscriptions = dependency()

    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        for channel in self.channels:
            self.subscriptions.subscribe_channel(channel)
            self.subscriptions.publish("subscribe", channel, self.subscriptions.active_subscriptions)

        return DoNotReply


@command(b"psubscribe", {b"slow", b"connection"})
class SubscribeToPatternedChannel(Command):
    subscriptions: ClientSubscriptions = dependency()

    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        for channel in self.channels:
            self.subscriptions.subscribe_pattern(channel)
            self.subscriptions.publish("psubscribe", channel, self.subscriptions.active_subscriptions)

        return DoNotReply


@command(b"unsubscribe", {b"slow", b"connection"})
class Unsubscribe(Command):
    subscriptions: ClientSubscriptions = dependency()

    channels: list[bytes] = positional_parameter(sequence_allow_empty=True)

    def execute(self) -> ValueType:
        if not self.channels:
            self.channels = list(self.subscriptions.subscribed_channels)

        if not self.channels:
            return ["unsubscribe", None, self.subscriptions.active_subscriptions]

        result = BulkArray()
        for channel in self.channels:
            self.subscriptions.unsubscribe_channel(channel)
            result.append(["unsubscribe", channel, self.subscriptions.active_subscriptions])

        return result


@command(b"punsubscribe", {b"slow", b"connection"})
class UnsubscribeFromPatternedChannel(Command):
    subscriptions: ClientSubscriptions = dependency()

    channels: list[bytes] = positional_parameter(sequence_allow_empty=True)

    def execute(self) -> ValueType:
        if not self.channels:
            self.channels = list(self.subscriptions.subscribed_patterns)

        if not self.channels:
            self.subscriptions.publish(
                "punsubscribe",
                None,
                self.subscriptions.active_subscriptions,
            )

        for channel in self.channels:
            self.subscriptions.unsubscribe_pattern(channel)
            self.subscriptions.publish("punsubscribe", channel, self.subscriptions.active_subscriptions)

        return DoNotReply


@command(b"numsub", {b"slow", b"connection"}, parent_command=b"pubsub")
class PubSubNumberOfSubscribers(Command):
    subscriptions_manager: SubscriptionsManager = dependency()

    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        result: list = []

        for channel in self.channels:
            num_of_subscribers = self.subscriptions_manager.channels_queues.count_values(channel)
            result.append(channel)
            result.append(num_of_subscribers)

        return result


@command(b"numpat", {b"slow", b"connection"}, parent_command=b"pubsub")
class PubSubNumberOfPatterns(Command):
    subscriptions_manager: SubscriptionsManager = dependency()

    def execute(self) -> ValueType:
        return self.subscriptions_manager.patterns_queues.keys_count


@command(b"publish", {b"slow", b"connection"})
class Publish(Command):
    subscription_manager: SubscriptionsManager = dependency()

    channel: bytes = positional_parameter()
    message: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.subscription_manager.publish(self.channel, self.message)
