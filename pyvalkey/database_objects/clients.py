import itertools
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Self

from pyvalkey.database_objects.utils import to_bytes


@dataclass
class Client:
    client_id: int
    host: bytes
    port: int
    name: bytes = b""

    is_normal: bool = True
    is_replica: bool = False

    is_killed: bool = False
    reply_mode: str = "on"

    library_name: bytes = b""
    library_version: bytes = b""

    @property
    def address(self) -> bytes:
        return self.host + b":" + str(self.port).encode()

    @property
    def flags(self) -> bytes:
        return b"".join([b"N" if self.is_normal else b"", b"S" if self.is_replica else b""])

    @property
    def info(self) -> bytes:
        items: dict[bytes, bytes | int] = {
            b"id": self.client_id,
            b"addr": self.address,
            b"flags": self.flags,
            b"name": self.name,
        }
        return b" ".join([k + b"=" + to_bytes(v) for k, v in items.items()])


class ClientList(dict[int, Client]):
    def __init__(
        self, *seq: dict[int, Client] | Iterable[tuple[int, Client]] | None, **kwargs: dict[int, Client]
    ) -> None:
        super().__init__(*seq, **kwargs)

        self.client_ids = itertools.count(0)

    def all(self) -> Self:
        return self.__class__({id_: c for id_, c in self.items() if not c.is_killed})

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

    def create_client(self, host: bytes, port: int) -> Client:
        current_client_id = next(self.client_ids)
        self[current_client_id] = Client(
            client_id=current_client_id,
            host=host,
            port=port,
        )
        return self[current_client_id]
