import fnmatch

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import command
from pyvalkey.notifications import ClientSubscriptions, SubscriptionsManager
from pyvalkey.resp import BulkArray, DoNotReply, ValueType
from pyvalkey.utils.dependencies import dependency


@command(b"subscribe", {b"pubsub", b"slow"}, flags={b"loading", b"noscript", b"pubsub", b"sentinel", b"stale"})
class Subscribe(Command):
    """
    summary: Listens for messages published to channels.
    complexity: O(N) where N is the number of channels to subscribe to.
    since: 2.0.0
    function: subscribeCommand
    """

    subscriptions: ClientSubscriptions = dependency()

    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        for channel in self.channels:
            self.subscriptions.subscribe_channel(channel)
            self.subscriptions.publish("subscribe", channel, self.subscriptions.active_subscriptions)

        return DoNotReply


@command(b"psubscribe", {b"pubsub", b"slow"}, flags={b"loading", b"noscript", b"pubsub", b"sentinel", b"stale"})
class SubscribeToPatternedChannel(Command):
    """
    summary: Listens for messages published to channels that match one or more patterns.
    complexity: O(N) where N is the number of patterns to subscribe to.
    since: 2.0.0
    function: psubscribeCommand
    """

    subscriptions: ClientSubscriptions = dependency()

    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        for channel in self.channels:
            self.subscriptions.subscribe_pattern(channel)
            self.subscriptions.publish("psubscribe", channel, self.subscriptions.active_subscriptions)

        return DoNotReply


@command(b"unsubscribe", {b"pubsub", b"slow"}, flags={b"loading", b"noscript", b"pubsub", b"sentinel", b"stale"})
class Unsubscribe(Command):
    """
    summary: Stops listening to messages posted to channels.
    complexity: O(N) where N is the number of channels to unsubscribe.
    since: 2.0.0
    function: unsubscribeCommand
    """

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


@command(b"punsubscribe", {b"pubsub", b"slow"}, flags={b"loading", b"noscript", b"pubsub", b"sentinel", b"stale"})
class UnsubscribeFromPatternedChannel(Command):
    """
    summary: >-
      Stops listening to messages published to channels that match one or more patterns.
    complexity: O(N) where N is the number of patterns to unsubscribe.
    since: 2.0.0
    function: punsubscribeCommand
    """

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


@command(b"channels", {b"pubsub", b"slow"}, parent_command=b"pubsub", flags={b"loading", b"pubsub", b"stale"})
class PubSubChannels(Command):
    """
    summary: Returns the active channels.
    complexity: >-
      O(N) where N is the number of active channels, and assuming constant time pattern matching (relatively short
      channels and patterns)
    since: 2.8.0
    function: pubsubCommand
    reply_schema:
      description: A list of active channels, optionally matching the specified pattern.
      type: array
      uniqueItems: true
      items:
        type: string
    """

    subscriptions_manager: SubscriptionsManager = dependency()

    pattern: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.pattern is None:
            return list(self.subscriptions_manager.channels_queues.iter_keys())

        decoded_pattern = self.pattern.decode()
        return [
            channel
            for channel in self.subscriptions_manager.channels_queues.iter_keys()
            if fnmatch.fnmatch(channel.decode() if isinstance(channel, bytes) else str(channel), decoded_pattern)
        ]


@command(b"help", {b"slow"}, parent_command=b"pubsub", flags={b"loading", b"stale"})
class PubSubHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 6.2.0
    function: pubsubCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"CHANNELS [<pattern>]",
            b"    Return the currently active channels matching a <pattern>.",
            b"NUMSUB [<channel> ...]",
            b"    Return the number of subscribers for the specified channels.",
            b"NUMPAT",
            b"    Return the number of subscribed patterns.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(b"numsub", {b"pubsub", b"slow"}, parent_command=b"pubsub", flags={b"loading", b"pubsub", b"stale"})
class PubSubNumberOfSubscribers(Command):
    """
    summary: Returns a count of subscribers to channels.
    complexity: O(N) for the NUMSUB subcommand, where N is the number of requested channels
    since: 2.8.0
    function: pubsubCommand
    reply_schema:
      description: >-
        The number of subscribers per channel, each even element (including 0th) is channel name, each odd element
        is the number of subscribers.
      type: array
    """

    subscriptions_manager: SubscriptionsManager = dependency()

    channels: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        result: list = []

        for channel in self.channels:
            num_of_subscribers = self.subscriptions_manager.channels_queues.count_values(channel)
            result.append(channel)
            result.append(num_of_subscribers)

        return result


@command(b"numpat", {b"pubsub", b"slow"}, parent_command=b"pubsub", flags={b"loading", b"pubsub", b"stale"})
class PubSubNumberOfPatterns(Command):
    """
    summary: Returns a count of unique pattern subscriptions.
    complexity: O(1)
    since: 2.8.0
    function: pubsubCommand
    reply_schema:
      description: The number of patterns all the clients are subscribed to.
      type: integer
      minimum: 0
    """

    subscriptions_manager: SubscriptionsManager = dependency()

    def execute(self) -> ValueType:
        return self.subscriptions_manager.patterns_queues.keys_count


@command(b"publish", {b"pubsub"}, flags={b"fast", b"loading", b"may_replicate", b"pubsub", b"sentinel", b"stale"})
class Publish(Command):
    """
    summary: Posts a message to a channel.
    complexity: >-
      O(N+M) where N is the number of clients subscribed to the receiving channel and M is the total number of
      subscribed patterns (by any client).
    since: 2.0.0
    function: publishCommand
    reply_schema:
      description: >-
        The number of clients that received the message. Note that in a Cluster, only clients that are connected
        to the same node as the publishing client are included in the count.
      type: integer
      minimum: 0
    """

    subscription_manager: SubscriptionsManager = dependency()

    channel: bytes = positional_parameter()
    message: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.subscription_manager.publish(self.channel, self.message)
