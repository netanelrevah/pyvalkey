from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyvalkey.database_objects.utils import to_bytes

if TYPE_CHECKING:
    from pyvalkey.commands.context import ServerContext


@dataclass
class CommandStatistics:
    name: bytes
    calls: int = 0
    microseconds: int = 0
    rejected_calls: int = 0
    failed_calls: int = 0

    def __str__(self) -> str:
        return (
            f"cmdstat_{self.name.decode()}:"
            f"calls={self.calls},"
            f"usec={self.microseconds},"
            f"usec_per_call={(self.microseconds / self.calls) if self.calls else 0:.2f},"
            f"rejected_calls={self.rejected_calls},"
            f"failed_calls={self.failed_calls}"
        )


@dataclass
class Information:
    server_version: bytes = b"255.255.255"
    arch_bits: bytes = b"64"
    cluster_enabled: bool = False
    enterprise: bool = False
    total_commands_processed: int = 0
    rdb_changes_since_last_save: int = 0
    commands_statistics: dict[bytes, CommandStatistics] = field(default_factory=dict)
    error_statistics: Counter[bytes] = field(default_factory=Counter)

    _server_context: ServerContext | None = None

    def get_command_statistics(self, command_name: bytes) -> CommandStatistics:
        if command_name not in self.commands_statistics:
            self.commands_statistics[command_name] = CommandStatistics(command_name)
        return self.commands_statistics[command_name]

    @property
    def server_context(self) -> ServerContext:
        if self._server_context is None:
            raise ValueError()
        return self._server_context

    @server_context.setter
    def server_context(self, value: ServerContext) -> None:
        self._server_context = value

    def commands_statistics_info(self) -> bytes:
        info = [b"# Commandstats"]
        info.extend([str(command_statistics).encode() for command_statistics in self.commands_statistics.values()])
        return b"\r\n".join(info) + b"\r\n"

    def server(self) -> bytes:
        info = [
            b"# Server",
            b"valkey_version:" + self.server_version,
            b"arch_bits:" + self.arch_bits,
        ]
        return b"\r\n".join(info)

    def clients(self) -> bytes:
        info = [
            b"# Clients",
            b"blocked_clients:" + to_bytes(self.server_context.num_of_blocked_clients()),
            b"total_blocking_keys:" + to_bytes(self.server_context.blocking_manager.total_key_blocking()),
            b"total_blocking_keys_on_nokey:" + to_bytes(self.server_context.num_of_blocked_client_for_no_key()),
        ]
        return b"\r\n".join(info) + b"\r\n"

    def errorstats(self) -> bytes:
        info = [b"# Errorstats"]
        for error_type, count in self.error_statistics.items():
            info.append(b"errorstat_" + error_type + b":count=" + to_bytes(count))
        return b"\r\n".join(info) + b"\r\n"

    def cluster(self) -> bytes:
        info = [
            b"# Cluster",
            b"cluster_enabled:" + (b"1" if self.cluster_enabled else b"0"),
        ]
        return b"\r\n".join(info)

    def persistence(self) -> bytes:
        info = [
            b"# Persistence",
            b"rdb_changes_since_last_save:" + to_bytes(self.rdb_changes_since_last_save),
        ]
        return b"\r\n".join(info)

    def stats(self) -> bytes:
        info = [
            b"# Stats",
            b"total_commands_processed:" + to_bytes(self.total_commands_processed),
            b"total_error_replies:" + to_bytes(self.error_statistics.total()),
        ]
        return b"\r\n".join(info)

    def keyspace(self) -> bytes:
        info = [b"# Keyspace"]
        for database_index in sorted(self.server_context.databases.keys()):
            database = self.server_context.databases[database_index]
            if database.empty():
                continue
            info.append(
                f"db{database_index}:"
                f"keys={database.size()}:"
                f"expires={database.number_of_keys_with_expiration()},"
                f"avg_ttl={database.average_ttl()}".encode()
            )

        return b"\r\n".join(info)

    def memory(self) -> bytes:
        info = [
            b"# Memory",
            b"lazyfree_pending_objects:0",
        ]
        return b"\r\n".join(info)

    def sections(self, sections: list[bytes] | None) -> bytes:
        info = []
        if not sections or b"all" in sections or b"server" in sections:
            info.append(self.server())
        if not sections or b"all" in sections or b"clients" in sections:
            info.append(self.clients())
        if not sections or b"all" in sections or b"memory" in sections:
            info.append(self.memory())
        if not sections or b"all" in sections or b"persistence" in sections:
            info.append(self.persistence())
        if not sections or b"all" in sections or b"stats" in sections:
            info.append(self.stats())
        if sections and b"commandstats" in sections:
            info.append(self.commands_statistics_info())
        if not sections or b"all" in sections or b"errorstats" in sections:
            info.append(self.errorstats())
        if not sections or b"all" in sections or b"cluster" in sections:
            info.append(self.cluster())
        if not sections or b"all" in sections or b"keyspace" in sections:
            info.append(self.keyspace())

        return b"\r\n\r\n".join(info)
