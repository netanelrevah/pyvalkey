import fnmatch
from dataclasses import Field, dataclass, field
from hashlib import sha256
from typing import Any, ClassVar, Literal, TypeVar, dataclass_transform

from pyvalkey.enums import NOTIFICATION_TYPE_ORDER


class ConfigurationError(Exception):
    pass


@dataclass
class ConfigurationFieldData:
    type_: Literal["string", "password", "integer", "ordered"] = "string"
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
    type_: Literal["string", "password", "integer", "ordered"] = "string",
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
                raise ValueError(
                    f"only one alias ({configuration_field_data.alias}) allowed per configuration ({name})"
                )
            cls.FIELD_BY_NAME[configuration_field_data.alias] = configuration_field_data
    return dataclass(cls)


@configurations
class Configurations(ConfigurationBase):
    requirepass: bytes = configuration(default=b"")
    maxclients: int = configuration(default=10000, type_="integer")
    unixsocket: bytes = configuration(default=b"")
    timeout: int = configuration(default=0, type_="integer")
    availability_zone: bytes = configuration(default=b"")
    save: bytes = configuration(default=b"3600 1 300 100 60 10000")

    client_query_buffer_limit: int = configuration(default=1073741824, type_="integer")

    active_expire_effort: int = configuration(default=1, type_="integer")
    active_defrag_threshold_lower: int = configuration(default=10, type_="integer")

    aof_rewrite_incremental_fsync: bytes = configuration(default=b"yes")
    aof_disable_auto_gc: bytes = configuration(default=b"no")

    tls_port: int = configuration(default=0, type_="integer")
    tls_ca_cert_file: bytes = configuration(default=b"")

    hash_max_listpack_value: int = configuration(default=64, type_="integer", alias=b"hash-max-ziplist-value")
    hash_max_listpack_entries: int = configuration(default=512, type_="integer", alias=b"hash-max-ziplist-entries")

    set_max_listpack_value: int = configuration(default=64, type_="integer")
    set_max_listpack_entries: int = configuration(default=128, type_="integer")
    set_max_intset_entries: int = configuration(default=512, type_="integer")

    stream_node_max_entries: int = configuration(default=100, type_="integer")

    list_compress_depth: int = configuration(default=0, type_="integer")
    list_max_listpack_size: int = configuration(default=-2, type_="integer", alias=b"list-max-ziplist-size")

    zset_max_listpack_value: int = configuration(default=64, type_="integer", alias=b"zset-max-ziplist-value")
    zset_max_listpack_entries: int = configuration(default=128, type_="integer", alias=b"zset-max-ziplist-entries")

    sanitize_dump_payload: bytes = configuration(default=b"no")

    maxmemory: int = configuration(default=0, type_="integer", flags={b"memory"})
    maxmemory_policy: bytes = configuration(default=b"noeviction")

    repl_ping_replica_period: int = configuration(default=10, type_="integer", alias=b"repl-ping-slave-period")

    notify_keyspace_events: bytes = configuration(default=b"", type_="ordered")
    busy_reply_threshold: int = configuration(default=5000, type_="integer", alias=b"lua-time-limit")

    acl_pubsub_default: bytes = configuration(default=b"0")
    aclfile: bytes = configuration(default=b"")
    acllog_max_len: int = configuration(default=128, type_="integer")
    active_defrag_cycle_max: int = configuration(default=25, type_="integer")
    active_defrag_cycle_min: int = configuration(default=1, type_="integer")
    active_defrag_cycle_us: int = configuration(default=500, type_="integer")
    active_defrag_ignore_bytes: int = configuration(default=104857600, type_="integer")
    active_defrag_max_scan_fields: int = configuration(default=1000, type_="integer")

    active_defrag_threshold_upper: int = configuration(default=100, type_="integer")

    activedefrag: bytes = configuration(default=b"CONFIG_ACTIVE_DEFRAG_DEFAULT")
    activerehashing: bytes = configuration(default=b"yes")
    always_show_logo: bytes = configuration(default=b"no")

    aof_load_truncated: bytes = configuration(default=b"yes")
    aof_rewrite_cpulist: bytes = configuration(default=b"", alias=b"aof_rewrite_cpulist")

    aof_timestamp_enabled: bytes = configuration(default=b"no")
    aof_use_rdb_preamble: bytes = configuration(default=b"yes")
    appenddirname: bytes = configuration(default=b"appendonlydir")
    appendfilename: bytes = configuration(default=b"appendonly.aof")
    appendfsync: bytes = configuration(default=b"everysec")
    appendonly: bytes = configuration(default=b"no")
    auto_aof_rewrite_min_size: bytes = configuration(default=b"64mb", type_="integer")
    auto_aof_rewrite_percentage: int = configuration(default=100, type_="integer")

    bgsave_cpulist: bytes = configuration(default=b"", alias=b"bgsave_cpulist")
    bind: bytes = configuration(default=b"127.0.0.1 -::1")
    bind_source_addr: bytes = configuration(default=b"")
    bio_cpulist: bytes = configuration(default=b"", alias=b"bio_cpulist")

    client_default_resp: int = configuration(default=2, type_="integer")
    client_output_buffer_limit: bytes = configuration(default=b"pubsub 32mb 8mb 60")

    cluster_allow_pubsubshard_when_down: bytes = configuration(default=b"yes")
    cluster_allow_reads_when_down: bytes = configuration(default=b"no")
    cluster_allow_replica_migration: bytes = configuration(default=b"yes")
    cluster_announce_bus_port: int = configuration(default=0, type_="integer")
    cluster_announce_client_ipv4: bytes = configuration(default=b"")
    cluster_announce_client_ipv6: bytes = configuration(default=b"")
    cluster_announce_client_port: int = configuration(default=0, type_="integer")
    cluster_announce_client_tls_port: int = configuration(default=0, type_="integer")
    cluster_announce_hostname: bytes = configuration(default=b"")
    cluster_announce_human_nodename: bytes = configuration(default=b"")
    cluster_announce_ip: bytes = configuration(default=b"")
    cluster_announce_port: int = configuration(default=0, type_="integer")
    cluster_announce_tls_port: int = configuration(default=0, type_="integer")
    cluster_blacklist_ttl: int = configuration(default=60, type_="integer")
    cluster_config_file: bytes = configuration(default=b"nodes.conf")
    cluster_databases: int = configuration(default=1, type_="integer")
    cluster_enabled: bytes = configuration(default=b"no")
    cluster_link_sendbuf_limit: int = configuration(default=0, type_="integer")
    cluster_manual_failover_timeout: int = configuration(default=5000, type_="integer")
    cluster_migration_barrier: int = configuration(default=1, type_="integer")
    cluster_node_timeout: int = configuration(default=15000, type_="integer")
    cluster_ping_interval: int = configuration(default=0, type_="integer")
    cluster_port: int = configuration(default=0, type_="integer")
    cluster_preferred_endpoint_type: bytes = configuration(default=b"CLUSTER_ENDPOINT_TYPE_IP")
    cluster_replica_no_failover: bytes = configuration(default=b"no", alias=b"cluster-slave-no-failover")
    cluster_replica_validity_factor: int = configuration(
        default=10, type_="integer", alias=b"cluster-slave-validity-factor"
    )
    cluster_require_full_coverage: bytes = configuration(default=b"yes")
    cluster_slot_migration_log_max_len: int = configuration(default=1000, type_="integer")
    cluster_slot_stats_enabled: bytes = configuration(default=b"no")
    commandlog_execution_slower_than: int = configuration(
        default=10000, type_="integer", alias=b"slowlog-log-slower-than"
    )
    commandlog_large_reply_max_len: int = configuration(default=128, type_="integer")
    commandlog_large_request_max_len: int = configuration(default=128, type_="integer")
    commandlog_reply_larger_than: int = configuration(default=1048576, type_="integer")
    commandlog_request_larger_than: int = configuration(default=1048576, type_="integer")
    commandlog_slow_execution_max_len: int = configuration(default=128, type_="integer", alias=b"slowlog-max-len")
    crash_log_enabled: bytes = configuration(default=b"yes")
    crash_memcheck_enabled: bytes = configuration(default=b"yes")
    daemonize: bytes = configuration(default=b"no")
    databases: int = configuration(default=16, type_="integer")
    dbfilename: bytes = configuration(default=b"dump.rdb")
    debug_context: bytes = configuration(default=b"")
    dir: bytes = configuration(default=b"./")
    disable_thp: bytes = configuration(default=b"yes")
    dual_channel_replication_enabled: bytes = configuration(default=b"no")
    enable_debug_assert: bytes = configuration(default=b"no")
    enable_debug_command: bytes = configuration(default=b"PROTECTED_ACTION_ALLOWED_NO")
    enable_module_command: bytes = configuration(default=b"PROTECTED_ACTION_ALLOWED_NO")
    enable_protected_configs: bytes = configuration(default=b"PROTECTED_ACTION_ALLOWED_NO")
    events_per_io_thread: int = configuration(default=2, type_="integer")
    extended_redis_compatibility: bytes = configuration(default=b"no")

    hash_seed: bytes = configuration(default=b"")
    hide_user_data_from_log: bytes = configuration(default=b"yes")
    hll_sparse_max_bytes: int = configuration(default=3000, type_="integer")
    hz: int = configuration(default=10, type_="integer")
    ignore_warnings: bytes = configuration(default=b"")
    import_mode: bytes = configuration(default=b"no")
    io_threads: int = configuration(default=1, type_="integer")
    jemalloc_bg_thread: bytes = configuration(default=b"yes")
    key_load_delay: int = configuration(default=0, type_="integer")
    latency_monitor_threshold: int = configuration(default=0, type_="integer")
    latency_tracking: bytes = configuration(default=b"yes")
    latency_tracking_info_percentiles: bytes = configuration(default=b"None")
    lazyfree_lazy_eviction: bytes = configuration(default=b"yes")
    lazyfree_lazy_expire: bytes = configuration(default=b"yes")
    lazyfree_lazy_server_del: bytes = configuration(default=b"yes")
    lazyfree_lazy_user_del: bytes = configuration(default=b"yes")
    lazyfree_lazy_user_flush: bytes = configuration(default=b"yes")
    lfu_decay_time: int = configuration(default=1, type_="integer")
    lfu_log_factor: int = configuration(default=10, type_="integer")

    loading_process_events_interval_bytes: int = configuration(default=2097152, type_="integer")
    locale_collate: bytes = configuration(default=b"")
    log_format: bytes = configuration(default=b"LOG_FORMAT_LEGACY")
    log_timestamp_format: bytes = configuration(default=b"LOG_TIMESTAMP_LEGACY")
    logfile: bytes = configuration(default=b"")
    loglevel: bytes = configuration(default=b"notice")
    lua_enable_insecure_api: bytes = configuration(default=b"no", alias=b"lua-enable-deprecated-api")
    max_new_connections_per_cycle: int = configuration(default=10, type_="integer")
    max_new_tls_connections_per_cycle: int = configuration(default=1, type_="integer")

    maxmemory_clients: int = configuration(default=0, type_="integer")
    maxmemory_eviction_tenacity: int = configuration(default=10, type_="integer")

    maxmemory_samples: int = configuration(default=5, type_="integer")
    min_io_threads_avoid_copy_reply: int = configuration(default=7, type_="integer")
    min_replicas_max_lag: int = configuration(default=10, type_="integer", alias=b"min-slaves-max-lag")
    min_replicas_to_write: int = configuration(default=0, type_="integer", alias=b"min-slaves-to-write")
    min_string_size_avoid_copy_reply: int = configuration(default=16384, type_="integer")
    min_string_size_avoid_copy_reply_threaded: int = configuration(default=65536, type_="integer")
    mptcp: bytes = configuration(default=b"no")
    no_appendfsync_on_rewrite: bytes = configuration(default=b"no")

    oom_score_adj: bytes = configuration(default=b"no")
    oom_score_adj_values: bytes = configuration(default=b"0 200 800")
    pidfile: bytes = configuration(default=b"/var/run/valkey_6379.pid")
    port: int = configuration(default=6379, type_="integer")
    prefetch_batch_max_size: int = configuration(default=16, type_="integer")
    primaryauth: bytes = configuration(default=b"", alias=b"masterauth")
    primaryuser: bytes = configuration(default=b"", alias=b"masteruser")
    proc_title_template: bytes = configuration(default=b"{title} {listen-addr} {server-mode}")
    propagation_error_behavior: bytes = configuration(default=b"PROPAGATION_ERR_BEHAVIOR_IGNORE")
    protected_mode: bytes = configuration(default=b"yes")
    proto_max_bulk_len: int = configuration(default=536870912, type_="integer")
    rdb_del_sync_files: bytes = configuration(default=b"no")
    rdb_key_save_delay: int = configuration(default=0, type_="integer")
    rdb_save_incremental_fsync: bytes = configuration(default=b"yes")
    rdb_version_check: bytes = configuration(default=b"strict")
    rdbchecksum: bytes = configuration(default=b"yes")
    rdbcompression: bytes = configuration(default=b"yes")
    rdma_bind: bytes = configuration(default=b"None")
    rdma_completion_vector: int = configuration(default=-1, type_="integer")
    rdma_port: int = configuration(default=0, type_="integer")
    rdma_rx_size: int = configuration(default=1048576, type_="integer")
    repl_backlog_size: int = configuration(default=10485760, type_="integer")
    repl_backlog_ttl: int = configuration(default=3600, type_="integer")
    repl_disable_tcp_nodelay: bytes = configuration(default=b"no")
    repl_diskless_load: bytes = configuration(default=b"disabled")
    repl_diskless_sync: bytes = configuration(default=b"yes")
    repl_diskless_sync_delay: int = configuration(default=5, type_="integer")
    repl_diskless_sync_max_replicas: int = configuration(default=0, type_="integer")
    repl_mptcp: bytes = configuration(default=b"no")
    repl_timeout: int = configuration(default=60, type_="integer")
    replica_announce_ip: bytes = configuration(default=b"", alias=b"slave-announce-ip")
    replica_announce_port: int = configuration(default=0, type_="integer", alias=b"slave-announce-port")
    replica_announced: bytes = configuration(default=b"yes")
    replica_ignore_disk_write_errors: bytes = configuration(default=b"no")
    replica_ignore_maxmemory: bytes = configuration(default=b"yes", alias=b"slave-ignore-maxmemory")
    replica_lazy_flush: bytes = configuration(default=b"yes", alias=b"slave-lazy-flush")
    replica_priority: int = configuration(default=100, type_="integer", alias=b"slave-priority")
    replica_read_only: bytes = configuration(default=b"yes", alias=b"slave-read-only")
    replica_serve_stale_data: bytes = configuration(default=b"yes", alias=b"slave-serve-stale-data")
    replicaof: bytes = configuration(default=b"None", alias=b"slaveof")
    req_res_logfile: bytes = configuration(default=b"")

    server_cpulist: bytes = configuration(default=b"", alias=b"server_cpulist")

    set_proc_title: bytes = configuration(default=b"yes")
    shutdown_on_sigint: bytes = configuration(default=b"0")
    shutdown_on_sigterm: bytes = configuration(default=b"0")
    shutdown_timeout: int = configuration(default=10, type_="integer")
    slot_migration_max_failover_repl_bytes: int = configuration(default=0, type_="integer")
    socket_mark_id: int = configuration(default=0, type_="integer")
    stop_writes_on_bgsave_error: bytes = configuration(default=b"yes")
    stream_node_max_bytes: int = configuration(default=4096, type_="integer")

    supervised: bytes = configuration(default=b"SUPERVISED_NONE")
    syslog_enabled: bytes = configuration(default=b"no")
    syslog_facility: bytes = configuration(default=b"LOG_LOCAL0")
    syslog_ident: bytes = configuration(default=b"SERVER_NAME")
    tcp_backlog: int = configuration(default=511, type_="integer")
    tcp_keepalive: int = configuration(default=300, type_="integer")

    tls_auth_clients: bytes = configuration(default=b"TLS_CLIENT_AUTH_YES")
    tls_auth_clients_user: bytes = configuration(default=b"TLS_CLIENT_FIELD_OFF")
    tls_auto_reload_interval: int = configuration(default=0, type_="integer")
    tls_ca_cert_dir: bytes = configuration(default=b"")

    tls_cert_file: bytes = configuration(default=b"")
    tls_ciphers: bytes = configuration(default=b"")
    tls_ciphersuites: bytes = configuration(default=b"")
    tls_client_cert_file: bytes = configuration(default=b"")
    tls_client_key_file: bytes = configuration(default=b"")
    tls_client_key_file_pass: bytes = configuration(default=b"")
    tls_cluster: bytes = configuration(default=b"no")
    tls_dh_params_file: bytes = configuration(default=b"")
    tls_key_file: bytes = configuration(default=b"")
    tls_key_file_pass: bytes = configuration(default=b"")

    tls_prefer_server_ciphers: bytes = configuration(default=b"no")
    tls_protocols: bytes = configuration(default=b"")
    tls_replication: bytes = configuration(default=b"no")
    tls_session_cache_size: int = configuration(default=20480, type_="integer")
    tls_session_cache_timeout: int = configuration(default=300, type_="integer")
    tls_session_caching: bytes = configuration(default=b"yes")
    tracking_table_max_keys: int = configuration(default=1000000, type_="integer")

    unixsocketgroup: bytes = configuration(default=b"")
    unixsocketperm: int = configuration(default=0, type_="integer")
    use_exit_on_panic: bytes = configuration(default=b"no")
    watchdog_period: int = configuration(default=0, type_="integer")

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
                raise ConfigurationError("argument couldn't be parsed into an integer")
        elif field_type == "ordered":
            setattr(
                self,
                field_name,
                "".join(sorted(value.decode(), key=lambda c: NOTIFICATION_TYPE_ORDER.index(c.encode()))).encode(),
            )
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
