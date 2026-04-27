from __future__ import annotations

from enum import Enum, auto

from pyvalkey.utils.enums import BytesEnum


class StreamSpecialIds(BytesEnum):
    CREATE_NEW_ID = b"*"
    FIRST_ENTRY_ID = b"-"
    LAST_ENTRY_ID = b"+"
    NEW_ENTRY_ID = b"$"
    NEW_GROUP_ENTRY_ID = b">"


class UnblockMessage(Enum):
    TIMEOUT = auto()
    ERROR = auto()


class ReplyMode(BytesEnum):
    ON = b"ON"
    OFF = b"OFF"
    SKIP = b"SKIP"


class NotificationType(BytesEnum):
    ALL = b"A"
    GENERIC = b"g"
    STRING = b"$"
    LIST = b"l"
    SET = b"s"
    HASH = b"h"
    ZSET = b"z"
    EXPIRED = b"x"
    EVICTED = b"e"
    STREAM = b"t"
    MODULE = b"d"
    NEW = b"n"
    KEYSPACE = b"K"
    KEYEVENT = b"E"
    KEY_MISS = b"m"
    LOADED = b"loaded"


NOTIFICATION_TYPE_ORDER = b"Ag$lshzxetdnKEm"


NOTIFICATION_TYPE_ALL = {
    NotificationType.GENERIC,
    NotificationType.STRING,
    NotificationType.LIST,
    NotificationType.SET,
    NotificationType.HASH,
    NotificationType.EVICTED,
    NotificationType.STRING,
    NotificationType.MODULE,
}
