from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyvalkey.database_objects.utils import to_bytes

if TYPE_CHECKING:
    from pyvalkey.commands.context import ServerContext


@dataclass
class Information:
    server_version: bytes = b"7.0.0"
    arch_bits: bytes = b"64"
    cluster_enabled: bool = False
    enterprise: bool = False
    total_commands_processed: int = 0

    _server_context: ServerContext | None = None

    @property
    def server_context(self) -> ServerContext:
        if self._server_context is None:
            raise ValueError()
        return self._server_context

    @server_context.setter
    def server_context(self, value: ServerContext) -> None:
        self._server_context = value

    def all(self) -> bytes:
        info = [
            b"# Server",
            b"valkey_version:" + self.server_version,
            b"arch_bits:" + self.arch_bits,
            b"",
            b"# Cluster",
            b"cluster_enabled:" + (b"1" if self.cluster_enabled else b"0"),
            b"",
            b"# Stats",
            b"total_commands_processed:" + to_bytes(self.total_commands_processed),
            b"",
            b"enterprise:" + (b"1" if self.enterprise else b"0"),
        ]

        keyspace = [b"# Keyspace"]
        for database_index in sorted(self.server_context.databases.keys()):
            database = self.server_context.databases[database_index]
            if database.empty():
                continue
            keyspace.append(
                f"db{database_index}:"
                f"keys={database.size()}:"
                f"expires={database.number_of_keys_with_expiration()},"
                f"avg_ttl={database.average_ttl()}".encode()
            )

        return b"\r\n".join(info + keyspace)
