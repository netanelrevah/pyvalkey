from __future__ import annotations

import math
from functools import partial
from typing import TYPE_CHECKING, Any

import umsgpack as msgpack
from lupa.lua51 import lua_type

if TYPE_CHECKING:
    from pyvalkey.commands.lua.helpers import LuaRuntimeWrapper


def convert_to_msgpack_values(lua_runtime: LuaRuntimeWrapper, lua_value: Any, depth: int = 1) -> Any:  # noqa: ANN401
    max_depth = 17
    encode_invalid_numbers = False

    msgpack_value = lua_value
    if lua_type(msgpack_value) == "table":
        msgpack_value = {}
        all_numbers = True
        for key, value in lua_value.items():
            if isinstance(key, bool):
                raise TypeError("msgpack key cannot be boolean")
            if not isinstance(key, int):
                all_numbers = False

            if not encode_invalid_numbers and isinstance(value, float) and math.isnan(value):
                raise TypeError("msgpack value cannot be NaN")

            if lua_type(value) == "table":
                if max_depth is not None and depth >= max_depth:
                    return None
                value = convert_to_msgpack_values(lua_runtime, value, depth + 1)

            msgpack_value[key] = value
        if all_numbers:
            msgpack_value = list(msgpack_value.values())
    return msgpack_value


def convert_from_msgpack_values(lua_runtime: LuaRuntimeWrapper, python_value: Any, depth: int = 1) -> Any:  # noqa: ANN401
    max_depth = 100

    lua_value: Any
    if isinstance(python_value, str):
        lua_value = python_value.encode()
    elif isinstance(python_value, dict):
        if max_depth is not None and depth >= max_depth:
            raise ValueError("max depth reached")
        lua_value = {}
        for key, value in python_value.items():
            value = convert_from_msgpack_values(lua_runtime, value, depth + 1)
            lua_value[key] = value
        lua_value = lua_runtime.table_from(lua_value)
    elif isinstance(python_value, list):
        if max_depth is not None and depth >= max_depth:
            raise ValueError("max depth reached")
        lua_value = []
        for value in python_value:
            value = convert_from_msgpack_values(lua_runtime, value, depth + 1)
            lua_value.append(value)
        lua_value = lua_runtime.table_from(lua_value)
    else:
        lua_value = python_value
    return lua_value


def short_string(value: Any) -> Any:  # noqa: ANN401
    if isinstance(value, str):
        if len(value) > 20:  # noqa: PLR2004
            return value[:20] + "..."
    if isinstance(value, bytes):
        if len(value) > 20:  # noqa: PLR2004
            return value[:20] + b"..."
    if isinstance(value, list):
        if len(value) > 20:  # noqa: PLR2004
            return value[:20] + ["..."]
    if isinstance(value, dict):
        if len(value) > 20:  # noqa: PLR2004
            return {k: v for k, v in list(value.items())[:20]}
    return value


def msgpack_pack(lua_runtime: LuaRuntimeWrapper, *lua_value: Any) -> Any:  # noqa: ANN401
    if len(lua_value) == 1:
        lua_value = lua_value[0]
    python_value = convert_to_msgpack_values(lua_runtime, lua_value)

    return msgpack.dumps(python_value)


def msgpack_unpack(lua_runtime: LuaRuntimeWrapper, value: Any) -> Any:  # noqa: ANN401
    python_value = msgpack.loads(value)
    lua_value = convert_from_msgpack_values(lua_runtime, python_value)
    return lua_value


def msgpack_unpack_one(lua_runtime: LuaRuntimeWrapper, value: Any, offset: int) -> Any:  # noqa: ANN401
    return msgpack_unpack_limit(lua_runtime, value, 1, offset)


def msgpack_unpack_limit(lua_runtime: LuaRuntimeWrapper, value: Any, limit: int, offset: int) -> Any:  # noqa: ANN401
    python_values = msgpack.loads(value)

    real_offset = -1
    if isinstance(python_values, list):
        real_offset = offset + limit if (offset + limit) < len(python_values) else -1
        if offset >= len(python_values):
            raise ValueError(f"Start offset {offset} greater than input length {len(python_values)}.")
        python_values = python_values[offset : offset + limit]
    else:
        python_values = [python_values]

    lua_values = []
    for python_value in python_values:
        lua_values.append(convert_from_msgpack_values(lua_runtime, python_value))

    return real_offset, *lua_values


def register_cmsgpack_module(lua_runtime: LuaRuntimeWrapper, lua_globals: object) -> None:
    lua_globals.cmsgpack = lua_runtime.table(  # type: ignore[attr-defined]
        unpack=partial(msgpack_unpack, lua_runtime),
        unpack_one=partial(msgpack_unpack_one, lua_runtime),
        unpack_limit=partial(msgpack_unpack_limit, lua_runtime),
        pack=partial(msgpack_pack, lua_runtime),
    )
