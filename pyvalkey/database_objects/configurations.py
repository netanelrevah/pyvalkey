import fnmatch
from dataclasses import Field, dataclass, field
from hashlib import sha256
from typing import Any, ClassVar, Literal, TypeVar, dataclass_transform


class ConfigurationError(Exception):
    pass


@dataclass
class ConfigurationFieldData:
    type_: Literal["string", "password", "integer"] = "string"
    alias: bytes | None = None
    flags: set[bytes] = field(default_factory=set)
    _name: bytes | None = None
    _field_name: str | None = None

    @property
    def name(self) -> bytes:
        if self._name is None:
            raise ValueError()
        return self._name

    @name.setter
    def name(self, value: bytes) -> None:
        self._name = value

    @property
    def field_name(self) -> str:
        if self._field_name is None:
            raise ValueError()
        return self._field_name

    @field_name.setter
    def field_name(self, value: str) -> None:
        self._field_name = value


def configuration(
    default: int | bytes,
    type_: Literal["string", "password", "integer"] = "string",
    alias: bytes | None = None,
    flags: set[bytes] | None = None,
) -> Any:  # noqa:ANN401
    return field(
        default=default,
        metadata={
            "configuration": ConfigurationFieldData(type_, alias, flags=flags or set()),
        },
    )


@dataclass_transform()
@dataclass
class ConfigurationBase:
    FIELD_BY_NAME: ClassVar[dict[bytes, ConfigurationFieldData]] = {}
    CONFIGURATIONS_NAMES: ClassVar[list[bytes]] = []
    ALIASES_TO_FIELDS_NAMES: ClassVar[dict[bytes, str]] = {}


ConfigurationType = TypeVar("ConfigurationType", bound=ConfigurationBase)


def configurations(cls: type[ConfigurationType]) -> type[ConfigurationType]:
    for name, f in cls.__dict__.items():
        if not isinstance(f, Field):
            continue

        configuration_field_data = f.metadata.get("configuration")
        if configuration_field_data is None:
            continue

        configuration_field_data.field_name = name

        try:
            configuration_field_data.name
        except ValueError:
            configuration_field_data.name = name.replace("_", "-").encode()

        cls.FIELD_BY_NAME[configuration_field_data.name] = configuration_field_data
        cls.CONFIGURATIONS_NAMES.append(configuration_field_data.name)

        if configuration_field_data.alias is not None:
            cls.CONFIGURATIONS_NAMES.append(configuration_field_data.alias)
            if configuration_field_data.alias in cls.FIELD_BY_NAME:
                raise ValueError(f"only one alias ({configuration_field_data.alias}) allowed per configuration")
            cls.FIELD_BY_NAME[configuration_field_data.alias] = configuration_field_data
    return dataclass(cls)


@configurations
class Configurations(ConfigurationBase):
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

    hash_max_listpack_value: int = configuration(default=64, type_="integer", alias=b"hash-max-ziplist-value")
    hash_max_listpack_entries: int = configuration(default=512, type_="integer", alias=b"hash-max-ziplist-entries")

    set_max_listpack_value: int = configuration(default=64, type_="integer")
    set_max_listpack_entries: int = configuration(default=128, type_="integer")
    set_max_intset_entries: int = configuration(default=512, type_="integer")

    list_compress_depth: int = configuration(default=0, type_="integer")
    list_max_listpack_size: int = configuration(default=-2, type_="integer", alias=b"list-max-ziplist-size")

    zset_max_listpack_value: int = configuration(default=64, type_="integer", alias=b"zset-max-ziplist-value")
    zset_max_listpack_entries: int = configuration(default=128, type_="integer", alias=b"zset-max-ziplist-entries")

    sanitize_dump_payload: bytes = configuration(default=b"yes")

    maxmemory: int = configuration(default=0, type_="integer", flags={b"memory"})
    maxmemory_policy: bytes = configuration(default=b"noeviction")

    repl_ping_replica_period: int = configuration(default=10, type_="integer")

    lua_time_limit: int = configuration(default=5000, type_="integer")

    @classmethod
    def get_field_name(cls, name: bytes) -> str:
        return cls.FIELD_BY_NAME[name].field_name

    @classmethod
    def get_configuration_type(cls, name: bytes) -> str:
        if name in cls.FIELD_BY_NAME:
            return cls.FIELD_BY_NAME[name].type_
        return ""

    def set_value(self, name: bytes, value: bytes) -> None:
        field_name = self.get_field_name(name)
        field_type = self.get_configuration_type(name)

        if field_type == "password":
            setattr(self, field_name, sha256(value).hexdigest().encode())
        elif field_type == "integer":
            try:
                setattr(self, field_name, int(value.decode()))
            except ValueError:
                if b"memory" in self.FIELD_BY_NAME[name].flags:
                    raise ConfigurationError("argument must be a memory value")
                raise
        else:
            setattr(self, field_name, value)

    def get_names(self, *patterns: bytes) -> set[bytes]:
        names: set[bytes] = set([])
        for pattern in patterns:
            names.update(set(fnmatch.filter(self.CONFIGURATIONS_NAMES, pattern)))
        return names

    def info(self, names: set[bytes]) -> dict[bytes, bytes]:
        info = {}
        for name in names:
            if name not in self.FIELD_BY_NAME:
                continue
            f = self.FIELD_BY_NAME[name]
            info[name] = getattr(self, f.field_name)
        return info
