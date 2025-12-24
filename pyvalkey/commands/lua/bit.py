from __future__ import annotations

import ctypes
import operator
from functools import reduce
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyvalkey.commands.lua.helpers import LuaRuntimeWrapper


HIGHEST_TO_HEX_LENGTH = 8


def to_bit(value: Any) -> int:  # noqa: ANN401
    return ctypes.c_int32(value).value


def to_hex(value: Any, length: int) -> str:  # noqa: ANN401
    if length > HIGHEST_TO_HEX_LENGTH or length < 0:
        length = HIGHEST_TO_HEX_LENGTH
    hex_value = f"{to_bit(value):X}"
    return hex_value.zfill(length)[:length]


def register_bit_module(lua_runtime: LuaRuntimeWrapper, lua_globals: Any) -> None:  # noqa: ANN401
    lua_globals.bit = lua_runtime.table(
        tobit=to_bit,
        tohex=to_hex,
        bnot=lambda value: ~to_bit(value),
        bor=lambda *values: reduce(operator.or_, map(to_bit, values)),
        band=lambda *values: reduce(operator.and_, map(to_bit, values)),
        bxor=lambda *values: reduce(operator.xor, map(to_bit, values)),
        lshift=lambda value, n: to_bit(to_bit(value) << to_bit(n)),
        rshift=lambda value, n: to_bit(to_bit(value) >> to_bit(n)),
        arshift=lambda value, n: to_bit(to_bit(value) >> to_bit(n)),
        rol=lambda value, n: (2**32 - 1) & (to_bit(value) >> n | to_bit(value) << (32 - n)),
        ror=lambda value, n: (2**32 - 1) & (to_bit(value) << n | to_bit(value) >> (32 - n)),
        bswap=lambda value: int.from_bytes(to_bit(value).to_bytes(4, "big", signed=True), "little", signed=True),
    )  # type: ignore[attr-defined]
