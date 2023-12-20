from collections import defaultdict
from dataclasses import dataclass

from r3dis.database_objects.acl import ACL
from r3dis.database_objects.clients import Client, ClientList
from r3dis.database_objects.configurations import Configurations
from r3dis.database_objects.databases import Database
from r3dis.database_objects.information import Information


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
