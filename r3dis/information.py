from dataclasses import dataclass

from r3dis.utils import to_bytes


@dataclass
class Information:
    redis_version: bytes = b"7.0.0"
    arch_bits: bytes = b"64"
    cluster_enabled: bool = False
    enterprise: bool = False
    total_commands_processed: bytes = 0

    def all(self):
        return b"\r\n".join(
            [
                b"# Server",
                b"redis_version:" + self.redis_version,
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
        )
