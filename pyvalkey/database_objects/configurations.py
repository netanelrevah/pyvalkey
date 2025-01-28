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
    maxclients: bytes = configuration(default=b"10000")
    unixsocket: bytes = configuration(default=b"")
    timeout: int = configuration(default=0, type_="integer")
    availability_zone: bytes = configuration(default=b"")
    save: bytes = configuration(default=b"3600 1 300 100 60 10000")

    client_query_buffer_limit: bytes = configuration(default=b"1073741824")

    active_expire_effort: bytes = configuration(default=b"1")
    active_defrag_threshold_lower: bytes = configuration(default=b"10")

    aof_rewrite_incremental_fsync: bytes = configuration(default=b"yes")
    aof_disable_auto_gc: bytes = configuration(default=b"no")

    tls_port: bytes = configuration(default=b"0")
    tls_ca_cert_file: bytes = configuration(default=b"")

    hash_max_ziplist_value: int = configuration(default=64, type_="integer")

    set_max_intset_entries: int = configuration(default=512, type_="integer")

    list_compress_depth: int = configuration(default=0, type_="integer")
    list_max_listpack_size: int = configuration(default=-2, type_="integer")
    list_max_ziplist_size: int = configuration(default=2, type_="integer")

    @classmethod
    def get_number_of_values(cls, name: bytes) -> int:
        field_name = name.decode().replace("-", "_")

        try:
            a_field: Field = cls.__dataclass_fields__[field_name]
            return a_field.metadata["number_of_values"]
        except KeyError:
            return 0

    @classmethod
    def get_type(cls, name: bytes) -> str:
        field_name = name.decode().replace("-", "_")

        try:
            a_field: Field = cls.__dataclass_fields__[field_name]
            return a_field.metadata["type"]
        except KeyError:
            return ""

    def set_values(self, name: bytes, *values: bytes) -> None:
        field_type = self.get_type(name)
        field_name = name.decode().replace("-", "_")

        if field_type == "password":
            (value,) = values
            setattr(self, field_name, sha256(value).hexdigest().encode())
        elif field_type == "integer":
            (value,) = values
            setattr(self, field_name, int(value.decode()))
        else:
            (value,) = values
            setattr(self, field_name, value)

    def get_names(self, *patterns: bytes) -> set[bytes]:
        names: set[bytes] = set([])
        for pattern in patterns:
            names.update(set(fnmatch.filter((f.name.replace("_", "-").encode() for f in fields(self)), pattern)))
        return names

    def info(self, names: set[bytes]) -> dict[bytes, bytes]:
        return {
            f.name.replace("_", "-").encode(): to_bytes(getattr(self, f.name))
            for f in fields(self)
            if f.name.replace("_", "-").encode() in names
        }
