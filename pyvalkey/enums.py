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
