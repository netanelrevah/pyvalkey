from collections import defaultdict
from dataclasses import dataclass

from r3dis.acl import ACL
from r3dis.clients import Client, ClientList
from r3dis.configurations import Configurations
from r3dis.databases import Database
from r3dis.information import Information


@dataclass
class ServerContext:
    databases: defaultdict[int, Database]
    acl: ACL
    clients: ClientList
    configurations: Configurations
    information: Information

    is_paused: bool = False
    pause_timeout: float = 0


@dataclass
class ClientContext:
    server_context: ServerContext
    current_client: Client

    current_database: int = 0
    current_user: bytes | None = None

    @property
    def database(self):
        return self.server_context.databases[self.current_database]
