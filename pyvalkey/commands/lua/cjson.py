from __future__ import annotations

import json
import math
from functools import partial
from typing import TYPE_CHECKING, Any

from lupa.lua51 import lua_type

if TYPE_CHECKING:
    from pyvalkey.commands.lua.helpers import LuaRuntimeWrapper


def json_convert_lua_to_python_values(lua_runtime: LuaRuntimeWrapper, lua_value: Any, depth: int = 1) -> Any:  # noqa: ANN401
    max_depth = lua_runtime.globals().cjson._encode_max_depth  # type: ignore[attr-defined]
    encode_invalid_numbers = lua_runtime.globals().cjson._encode_invalid_numbers  # type: ignore[attr-defined]
    print("convert_to_json_values", depth, max_depth)

    json_value = lua_value
    if lua_type(json_value) == "table":
        json_value = {}
        all_numbers = True
        for key, value in lua_value.items():
            if isinstance(key, bytes):
                key = key.decode()

            if isinstance(key, bool):
                raise TypeError("json key cannot be boolean")
            if not isinstance(key, int):
                all_numbers = False

            if not encode_invalid_numbers and isinstance(value, float) and math.isnan(value):
                raise TypeError("json value cannot be NaN")

            if lua_type(value) == "table":
                if max_depth is not None and depth >= max_depth:
                    print("convert_to_json_values", "max depth reached")
                    raise ValueError("max depth reached")
                value = json_convert_lua_to_python_values(lua_runtime, value, depth + 1)
            elif isinstance(value, bytes):
                value = value.decode()

            json_value[key] = value
        if all_numbers:
            json_value = list(json_value.values())
    return json_value


def json_convert_python_to_lua_values(lua_runtime: LuaRuntimeWrapper, python_value: Any, depth: int = 1) -> Any:  # noqa: ANN401
    max_depth = lua_runtime.globals().cjson._encode_max_depth  # type: ignore[attr-defined]
    print("convert_from_json_values", depth, max_depth)

    lua_value: Any
    if isinstance(python_value, str):
        lua_value = python_value.encode()
    elif isinstance(python_value, dict):
        if max_depth is not None and depth >= max_depth:
            print("convert_to_json_values", "max depth reached")
            raise ValueError("max depth reached")
        lua_value = {}
        for key, value in python_value.items():
            value = json_convert_python_to_lua_values(lua_runtime, value, depth + 1)
            if isinstance(key, str):
                key = key.encode()
            lua_value[key] = value
        lua_value = lua_runtime.table_from(lua_value)
    elif isinstance(python_value, list):
        if max_depth is not None and depth >= max_depth:
            print("convert_to_json_values", "max depth reached")
            raise ValueError("max depth reached")
        lua_value = []
        for value in python_value:
            value = json_convert_python_to_lua_values(lua_runtime, value, depth + 1)
            lua_value.append(value)
        lua_value = lua_runtime.table_from(lua_value)
    else:
        lua_value = python_value
    return lua_value


def json_encode(lua_runtime: LuaRuntimeWrapper, value: Any) -> Any:  # noqa: ANN401
    print("json_encode", dict(value) if lua_type(value) == "table" else value)
    return json.dumps(json_convert_lua_to_python_values(lua_runtime, value))


def json_decode(lua_runtime: LuaRuntimeWrapper, value: bytes) -> Any:  # noqa: ANN401
    python_value = json.loads(value)
    print("json_decode", "python_value", python_value)
    lua_value = json_convert_python_to_lua_values(lua_runtime, python_value)
    print("json_decode", "lua_value", dict(lua_value) if lua_type(lua_value) == "table" else lua_value)
    return lua_value


def json_encode_keep_buffer(value: bool) -> None:
    return None


def json_encode_max_depth(lua_runtime: LuaRuntimeWrapper, value: int) -> None:
    lua_runtime.globals().cjson._encode_max_depth = value  # type: ignore[attr-defined]


def json_decode_max_depth(lua_runtime: LuaRuntimeWrapper, value: int) -> None:
    lua_runtime.globals().cjson._decode_max_depth = value  # type: ignore[attr-defined]


def json_encode_invalid_numbers(lua_runtime: LuaRuntimeWrapper, value: bool) -> None:
    lua_runtime.globals().cjson._encode_invalid_numbers = value  # type: ignore[attr-defined]


def register_cjson_module(lua_runtime: LuaRuntimeWrapper, lua_globals: Any) -> None:  # noqa: ANN401
    lua_globals.cjson = lua_runtime.table(
        decode=partial(json_decode, lua_runtime),
        encode=partial(json_encode, lua_runtime),
        encode_keep_buffer=json_encode_keep_buffer,
        encode_max_depth=partial(json_encode_max_depth, lua_runtime),
        _encode_max_depth=1000,
        decode_max_depth=partial(json_decode_max_depth, lua_runtime),
        _decode_max_depth=1000,
        encode_invalid_numbers=partial(json_encode_invalid_numbers, lua_runtime),
        _encode_invalid_numbers=False,
    )  # type: ignore[attr-defined]
