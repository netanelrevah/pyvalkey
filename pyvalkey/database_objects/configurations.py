import fnmatch
from dataclasses import Field, dataclass, field, fields
from hashlib import sha256
from typing import Any, Literal

from pyvalkey.database_objects.utils import to_bytes


def configuration(
    default: int | bytes, type_: Literal["string", "password", "integer"] = "string", number_of_values: int = 1
) -> Any:  # noqa:ANN401
    return field(
        default=default,
        metadata={
            "type": type_,
            "number_of_values": number_of_values,
        },
    )


@dataclass
class Configurations:
    requirepass: bytes = configuration(default=b"", type_="password")
    tls_port: bytes = configuration(default=b"0")
    aof_disable_auto_gc: bytes = configuration(default=b"no")
    active_expire_effort: bytes = configuration(default=b"1")
    client_query_buffer_limit: bytes = configuration(default=b"1073741824")
    maxclients: bytes = configuration(default=b"10000")
    unixsocket: bytes = configuration(default=b"")
    active_defrag_threshold_lower: bytes = configuration(default=b"10")
    aof_rewrite_incremental_fsync: bytes = configuration(default=b"yes")
    tls_ca_cert_file: bytes = configuration(default=b"")
    timeout: int = configuration(default=0, type_="integer")

    @classmethod
    def get_number_of_values(cls, name: bytes) -> int:
        try:
            a_field: Field = cls.__dataclass_fields__[name.decode().replace("-", "_")]
            return a_field.metadata["number_of_values"]
        except KeyError:
            return 0

    @classmethod
    def get_type(cls, name: bytes) -> str:
        try:
            a_field: Field = cls.__dataclass_fields__[name.decode().replace("-", "_")]
            return a_field.metadata["type"]
        except KeyError:
            return ""

    def set_values(self, name: bytes, *values: bytes) -> None:
        field_type = self.get_type(name)

        if field_type == "password":
            (value,) = values
            setattr(self, name.decode(), sha256(value).hexdigest().encode())
        elif field_type == "integer":
            (value,) = values
            setattr(self, name.decode(), int(value.decode()))
        else:
            (value,) = values
            setattr(self, name.decode(), value)

    def get_names(self, *patterns: bytes) -> set[bytes]:
        names: set[bytes] = set([])
        for pattern in patterns:
            names.update(set(fnmatch.filter((f.name.replace("_", "-").encode() for f in fields(self)), pattern)))
        return names

    def info(self, names: list[bytes]) -> dict[bytes, bytes]:
        return {
            f.name.replace("_", "-").encode(): to_bytes(getattr(self, f.name))
            for f in fields(self)
            if f.name.replace("_", "-").encode() in names
        }
