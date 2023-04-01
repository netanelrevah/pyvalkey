from dataclasses import dataclass

from r3dis.utils import to_bytes


@dataclass
class Client:
    client_id: int
    host: bytes
    port: int
    name: bytes = b""

    is_normal: bool = True
    is_replica: bool = False

    is_killed: bool = False
    is_paused: bool = False
    reply_mode: str = "on"

    @property
    def address(self) -> bytes:
        return self.host + b":" + str(self.port).encode()

    @property
    def flags(self) -> bytes:
        return b"".join([b"N" if self.is_normal else b"", b"S" if self.is_replica else b""])

    @property
    def info(self):
        items = {b"id": self.client_id, b"addr": self.address, b"flags": self.flags, b"name": self.name}.items()
        return b" ".join([k + b"=" + to_bytes(v) for k, v in items])


class ClientList(dict[int, Client]):
    def all(self):
        return ClientList({id_: c for id_, c in self.items() if not c.is_killed})

    def filter_(
        self, client_id: int | None = None, address: bytes | None = None, client_type: bytes | None = None
    ) -> "ClientList":
        filtered = ClientList()
        for c in self.all().values():
            if client_id is not None and c.client_id != client_id:
                continue
            if address is not None and c.address != address:
                continue
            if client_type is not None:
                if client_type == b"normal" and not c.is_normal:
                    continue
                if client_type == b"replica" and not c.is_replica:
                    continue
            filtered[c.client_id] = c
        return filtered

    @property
    def info(self) -> bytes:
        return b"\r".join([c.info for c in self.all().values()])
