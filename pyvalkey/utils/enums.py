from __future__ import annotations

from enum import ReprEnum
from typing import Any, Self


class BytesEnum(bytes, ReprEnum):
    def __new__(cls, *values: Any) -> Self:  # noqa: ANN401
        if len(values) > 3:  # noqa: PLR2004
            raise TypeError(f"too many arguments for bytes(): {values!r}")
        if len(values) >= 2:  # noqa: PLR2004
            if not isinstance(values[1], str):
                raise TypeError(f"encoding must be a string, not {values[1]!r}")
        if len(values) == 3:  # noqa: PLR2004
            if not isinstance(values[2], str):
                raise TypeError(f"errors must be a string, not {values[2]!r}")

        value = bytes(*values)
        member = bytes.__new__(cls, value)
        member._value_ = value
        return member
