import itertools
import logging
import time
from collections import defaultdict
from io import BytesIO
from socket import socket
from socketserver import StreamRequestHandler, ThreadingTCPServer

import select

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.clients import Client, ClientList
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database
from pyvalkey.database_objects.errors import (
    CommandPermissionError,
    RouterKeyError,
    ServerException,
    ServerInvalidIntegerError,
    ServerSyntaxError,
    ServerWrongType,
)
from pyvalkey.database_objects.information import Information
from pyvalkey.resp import RESP_OK, RespError, dump, load, ValueType

logger = logging.getLogger(__name__)


class ServerConnectionHandler(StreamRequestHandler):
    def __init__(self, request: socket, client_address: tuple[str, int], server: "ValkeyServer") -> None:
        super().__init__(request, client_address, server)
        self.server: ValkeyServer = server

        self.current_database = self.databases[0]
        self.current_client: Client

    @property
    def configurations(self) -> Configurations:
        return self.server.configurations

    @property
    def databases(self) -> defaultdict[int, Database]:
        return self.server.databases

    @property
    def clients(self) -> ClientList:
        return self.server.clients

    @property
    def acl(self) -> ACL:
        return self.server.acl

    def setup(self) -> None:
        super().setup()

        self.current_client = self.clients.create_client(
            host=self.client_address[0].encode(),
            port=self.client_address[1],
        )

        self.server_context = ServerContext(
            databases=self.databases,
            acl=self.acl,
            clients=self.clients,
            configurations=self.configurations,
            information=self.server.information,
        )

        self.client_context = ClientContext(
            self.server_context,
            current_client=self.current_client,
        )

        self.router = ServerCommandsRouter()

    def dump(self, value: ValueType) -> None:
        dumped = BytesIO()
        dump(value, dumped)
        print(self.current_client.client_id, "result", dumped.getvalue())

        if self.current_client.reply_mode == "skip":
            self.current_client.reply_mode = "on"
            return

        if self.current_client.reply_mode == "off":
            return

        dump(value, self.wfile)

    def handle(self) -> None:
        while not self.current_client.is_killed:
            ready = select.select([self.connection], [], [], 1)
            if not ready:
                continue
            command = load(self.rfile)

            if command is None:
                break
            if command[0] == b"QUIT":
                self.dump(RESP_OK)
                break

            self.server.information.total_commands_processed += 1

            print(self.current_client.client_id, command)

            try:
                routed_command = self.router.route(list(command), self.client_context)

                if self.client_context.current_user:
                    self.client_context.current_user.check_permissions(routed_command)

                self.dump(routed_command.execute())
                if self.server_context.pause_timeout:
                    while self.server_context.is_paused and time.time() < self.server_context.pause_timeout:
                        time.sleep(0.1)
                    self.server_context.pause_timeout = 0
            except RouterKeyError:
                self.dump(
                    RespError(
                        f"ERR unknown command '{command[0]}', "
                        f"with args beginning with: {command[1] if len(command) > 1 else ''}".encode()
                    )
                )
            except ServerWrongType:
                self.dump(RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value"))
            except ServerSyntaxError:
                self.dump(RespError(b"ERR syntax error"))
            except ServerInvalidIntegerError:
                self.dump(RespError(b"ERR hash value is not an integer"))
            except CommandPermissionError as e:
                self.dump(
                    RespError(
                        b"NOPERM User "
                        + self.client_context.current_user.name
                        + b" has no permissions to run the '"
                        + e.command_name
                        + b"' command"
                    )
                )
            except ServerException as e:
                self.dump(RespError(e.message))
            except Exception as e:
                self.dump(RespError(b"ERR internal"))
                raise e

        print(self.current_client.client_id, "exited")

    def finish(self) -> None:
        del self.clients[self.current_client.client_id]
        super().finish()


class ValkeyServer(ThreadingTCPServer):
    def __init__(self, server_address: tuple[str, int], bind_and_activate: bool = True) -> None:
        super().__init__(server_address, ServerConnectionHandler, bind_and_activate)
        self.databases: defaultdict[int, Database] = defaultdict(Database, {0: Database()})
        self.acl: ACL = ACL.create()
        self.client_ids = itertools.count(0)
        self.clients: ClientList = ClientList()
        self.configurations: Configurations = Configurations()
        self.information: Information = Information()
