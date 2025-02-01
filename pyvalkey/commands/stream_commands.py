from dataclasses import dataclass

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import keyword_parameter, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"xadd", [b"stream", b"write", b"fast"])
class StreamAdd(Command):
    key: bytes = positional_parameter()

    no_make_stream: bool = keyword_parameter(flag=b"NOMKSTREAM", default=False)
    stream_id: bytes = positional_parameter()
    field_value: list[tuple[bytes, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"xdel", [b"stream", b"write", b"fast"])
class StreamDel(Command):
    key: bytes = positional_parameter()
    ids: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"groups", [b"stream", b"write", b"fast"], b"xinfo")
class StreamInfoGroups(Command):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"stream", [b"stream", b"write", b"fast"], b"xinfo")
class StreamInfoStream(Command):
    key: bytes = positional_parameter()
    full: bool = keyword_parameter(flag=b"FULL", default=False)

    def execute(self) -> ValueType:
        return 1


@ServerCommandsRouter.command(b"create", [b"write", b"stream", b"slow"], b"xgroup")
class StreamGroupCreate(Command):
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    stream_id: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return None


@ServerCommandsRouter.command(b"setid", [b"write", b"stream", b"slow"], b"xgroup")
class StreamGroupSetId(Command):
    key: bytes = positional_parameter()
    group: bytes = positional_parameter()

    stream_id: bytes = positional_parameter()
    entries_read: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        return None


@dataclass
class Streams:
    key: bytes = positional_parameter()
    stream_id: bytes = positional_parameter()


@ServerCommandsRouter.command(b"group", [b"write", b"stream", b"slow", b"blocking"], b"xreadgroup")
class StreamReadGroup(Command):
    group: bytes = positional_parameter()
    consumer: bytes = positional_parameter()

    count: int | None = keyword_parameter(flag=b"COUNT", default=None)
    block: int | None = keyword_parameter(flag=b"BLOCK", default=None)
    no_ack: bool = keyword_parameter(flag=b"NOACK", default=False)
    streams: Streams = keyword_parameter(token=b"STREAMS")

    def execute(self) -> ValueType:
        return None
