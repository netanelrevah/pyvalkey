from __future__ import annotations

from dataclasses import MISSING, field
from enum import Enum, auto
from typing import Any


class ParameterMetadata(Enum):
    SERVER_PARAMETER = auto()
    VALUES_MAPPING = auto()
    FLAG = auto()
    KEY_MODE = auto()


def server_parameter(
    values_mapping: dict[bytes, Any] | None = None,
    default: Any = MISSING,  # noqa: ANN401
    flag: bytes | dict[bytes, Any] | None = None,
    key_mode: bytes | None = None,
) -> Any:  # noqa: ANN401
    metadata: dict[ParameterMetadata, Any] = {ParameterMetadata.SERVER_PARAMETER: True}
    if values_mapping:
        metadata[ParameterMetadata.VALUES_MAPPING] = values_mapping
    if flag:
        if isinstance(flag, dict):
            metadata[ParameterMetadata.FLAG] = {key.upper(): value for key, value in flag.items()}
            default = None
        else:
            metadata[ParameterMetadata.FLAG] = flag.upper()
            metadata[ParameterMetadata.VALUES_MAPPING] = {
                flag.upper(): True,
            }
            default = False
    if key_mode is not None:
        metadata[ParameterMetadata.KEY_MODE] = key_mode
    return field(default=default, metadata=metadata, kw_only=bool(flag))


def positional_parameter(
    values_mapping: dict[bytes, Any] | None = None,
    default: Any = MISSING,  # noqa: ANN401
    key_mode: bytes | None = None,
) -> Any:  # noqa: ANN401
    return server_parameter(values_mapping=values_mapping, default=default, key_mode=key_mode)


def server_keyword_parameter(
    values_mapping: dict[bytes, Any] | None = None,
    default: Any = MISSING,  # noqa: ANN401
    flag: bytes | dict[bytes, Any] | None = None,
) -> Any:  # noqa: ANN401
    return server_parameter(values_mapping=values_mapping, default=default, flag=flag)
