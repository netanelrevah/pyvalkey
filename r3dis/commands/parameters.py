from dataclasses import MISSING, Field, field
from enum import StrEnum, auto
from typing import Any


class ParameterMetadata(StrEnum):
    REDIS_PARAMETER = auto()
    VALUES_MAPPING = auto()
    FLAG = auto()


def redis_parameter(
    values_mapping: dict[bool:bytes] = None,
    default=MISSING,
    flag: bytes = None,
) -> Field:
    metadata = {ParameterMetadata.REDIS_PARAMETER: True}
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
    return field(default=default, metadata=metadata, kw_only=bool(flag))


def redis_positional_parameter(
    values_mapping: dict[bool:bytes] = None,
    default=MISSING,
) -> Field:
    return redis_parameter(
        values_mapping=values_mapping,
        default=default,
    )


def redis_keyword_parameter(
    values_mapping: dict[bool:bytes] = None,
    default=MISSING,
    flag: bytes | dict[bytes, Any] = None,
) -> Field:
    return redis_parameter(values_mapping=values_mapping, default=default, flag=flag)
