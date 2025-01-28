from __future__ import annotations

from dataclasses import MISSING, field
from enum import Enum, auto
from typing import Any


class ParameterMetadata(Enum):
    SERVER_PARAMETER = auto()
    VALUES_MAPPING = auto()
    TOKEN = auto()
    MULTI_TOKEN = auto()
    KEY_MODE = auto()
    PARSE_ERROR = auto()
    SKIP_FIRST = auto()


def server_parameter(
    values_mapping: dict[bytes, Any] | None = None,
    default: Any = MISSING,  # noqa: ANN401
    flag: bytes | dict[bytes, Any] | None = None,
    token: bytes | None = None,
    key_mode: bytes | None = None,
    parse_error: bytes | None = None,
    multi_token: bool = False,
    skip_first: bool = False,
) -> Any:  # noqa: ANN401
    metadata: dict[ParameterMetadata, Any] = {
        ParameterMetadata.SERVER_PARAMETER: True,
        ParameterMetadata.PARSE_ERROR: parse_error,
    }
    if values_mapping:
        metadata[ParameterMetadata.VALUES_MAPPING] = values_mapping
    if flag:
        if isinstance(flag, dict):
            metadata[ParameterMetadata.TOKEN] = metadata[ParameterMetadata.VALUES_MAPPING] = {
                key.upper(): value for key, value in flag.items()
            }
            default = None
        else:
            metadata[ParameterMetadata.TOKEN] = flag.upper()
            metadata[ParameterMetadata.VALUES_MAPPING] = {
                flag.upper(): True,
            }
            if default == MISSING:
                default = False
    if token:
        metadata[ParameterMetadata.TOKEN] = token.upper()
        metadata[ParameterMetadata.MULTI_TOKEN] = multi_token
        metadata[ParameterMetadata.SKIP_FIRST] = skip_first
    if key_mode is not None:
        metadata[ParameterMetadata.KEY_MODE] = key_mode
    return field(default=default, metadata=metadata, kw_only=bool(flag))


def positional_parameter(
    values_mapping: dict[bytes, Any] | None = None,
    default: Any = MISSING,  # noqa: ANN401
    key_mode: bytes | None = None,
    parse_error: bytes | None = None,
) -> Any:  # noqa: ANN401
    return server_parameter(values_mapping=values_mapping, default=default, key_mode=key_mode, parse_error=parse_error)


def keyword_parameter(
    values_mapping: dict[bytes, Any] | None = None,
    default: Any = MISSING,  # noqa: ANN401
    key_mode: bytes | None = None,
    flag: bytes | dict[bytes, Any] | None = None,
    token: bytes | None = None,
    multi_token: bool = False,
    skip_first: bool = False,
) -> Any:  # noqa: ANN401
    return server_parameter(
        values_mapping=values_mapping,
        default=default,
        flag=flag,
        token=token,
        multi_token=multi_token,
        key_mode=key_mode,
        skip_first=skip_first,
    )
