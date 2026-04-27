from dataclasses import field
from enum import Enum
from hashlib import sha256

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import (
    flag_parameter,
    keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.router import command
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.enums import ReplyMode, UnblockMessage
from pyvalkey.notifications import ClientSubscriptions
from pyvalkey.resp import RESP_OK, DoNotReply, RespError, RespProtocolVersion, ValueType
from pyvalkey.utils.dependencies import dependency
from pyvalkey.utils.times import now_f_s


@command(
    b"auth", {b"connection"}, flags={b"allow_busy", b"fast", b"loading", b"noscript", b"no_auth", b"sentinel", b"stale"}
)
class Authorize(Command):
    """
    summary: Authenticates the connection.
    complexity: O(N) where N is the number of passwords defined for the user
    since: 1.0.0
    function: authCommand
    reply_schema:
      const: OK
    """

    acl: ACL = dependency()
    configurations: Configurations = dependency()
    client_context: ClientContext = dependency()

    username: bytes | None = positional_parameter(default=None)
    password: bytes = positional_parameter()

    def execute(self) -> ValueType:
        password_hash = sha256(self.password).hexdigest().encode()
        if self.username is not None:
            if self.username not in self.acl:
                raise ServerError(b"WRONGPASS invalid username-password pair or user is disabled.")
            if self.username == b"default" and password_hash == self.configurations.requirepass:
                return RESP_OK
            acl_user = self.acl[self.username]
            if not acl_user.is_no_password_user and password_hash not in acl_user.passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            self.client_context.current_user = acl_user
            return RESP_OK

        if self.configurations.requirepass and password_hash == self.configurations.requirepass:
            return RESP_OK
        raise ServerError(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )


@command(b"help", {b"connection", b"slow"}, b"client", flags={b"loading", b"sentinel", b"stale"})
class ClientHelp(Command):
    """
    summary: Returns helpful text about the different subcommands.
    complexity: O(1)
    since: 5.0.0
    function: clientHelpCommand
    reply_schema:
      type: array
      description: Helpful text about subcommands.
      items:
        type: string
    """

    def execute(self) -> ValueType:
        return [
            b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            b"ID",
            b"    Return the ID of the current connection.",
            b"GETNAME",
            b"    Return the name of the current connection.",
            b"SETNAME <name>",
            b"    Assign the name <name> to the current connection.",
            b"LIST [TYPE <type>]",
            b"    Return information about client connections.",
            b"KILL <ip:port> [ID <id>] [ADDR <ip:port>]",
            b"    Terminate a client connection.",
            b"PAUSE <timeout> [WRITE|ALL]",
            b"    Suspend all, or only write, queries from all clients for <timeout> ms.",
            b"UNPAUSE",
            b"    Resume queries from all clients.",
            b"REPLY ON|OFF|SKIP",
            b"    Control the replies sent to the current connection.",
            b"UNBLOCK <id> [TIMEOUT|ERROR]",
            b"    Unblock a client that is currently blocked by a blocking command.",
            b"HELP",
            b"    Prints this help.",
        ]


@command(
    b"list",
    {b"admin", b"connection", b"dangerous", b"slow"},
    b"client",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientList(Command):
    """
    summary: Lists open connections.
    complexity: O(N) where N is the number of client connections
    since: 2.4.0
    function: clientListCommand
    reply_schema:
      type: string
      description: Information and statistics about client connections
    """

    client_context: ClientContext = dependency()
    client_type: bytes | None = keyword_parameter(flag=b"TYPE", default=None)

    def execute(self) -> ValueType:
        if self.client_type:
            return self.client_context.server_context.clients.filter_(client_type=self.client_type).info
        return self.client_context.server_context.clients.info


@command(
    b"id", {b"connection", b"slow"}, b"client", flags={b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"}
)
class ClientId(Command):
    """
    summary: Returns the unique client ID of the connection.
    complexity: O(1)
    since: 5.0.0
    function: clientIDCommand
    reply_schema:
      type: integer
      description: The id of the client
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.client_id


@command(
    b"setname",
    {b"connection", b"slow"},
    b"client",
    flags={b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientSetName(Command):
    """
    summary: Sets the connection name.
    complexity: O(1)
    since: 2.6.9
    function: clientSetNameCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()
    name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_client.name = self.name
        return RESP_OK


@command(
    b"getname",
    {b"connection", b"slow"},
    b"client",
    flags={b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientGetName(Command):
    """
    summary: Returns the name of the connection.
    complexity: O(1)
    since: 2.6.9
    function: clientGetNameCommand
    reply_schema:
      oneOf:
      - type: string
        description: The connection name of the current connection
      - type: 'null'
        description: Connection name was not set
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.name or None


@command(
    b"kill",
    {b"admin", b"connection", b"dangerous", b"slow"},
    b"client",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientKill(Command):
    """
    summary: Terminates open connections.
    complexity: O(N) where N is the number of client connections
    since: 2.4.0
    function: clientKillCommand
    reply_schema:
      oneOf:
      - description: When called in 3 argument format.
        const: OK
      - description: When called in filter/value format, the number of clients killed.
        type: integer
        minimum: 0
    """

    server_context: ServerContext = dependency()
    old_format_address: bytes | None = positional_parameter(default=None)
    client_id: int = keyword_parameter(flag=b"ID", default=None)
    address: bytes = keyword_parameter(flag=b"ADDR", default=None)

    def execute(self) -> ValueType:
        if self.old_format_address:
            clients = self.server_context.clients.filter_(address=self.old_format_address).values()
            if not clients:
                return RespError(b"ERR No such client")
            (client,) = clients
            client.is_killed = True
            return RESP_OK

        clients = self.server_context.clients.filter_(client_id=self.client_id, address=self.address).values()
        for client in clients:
            client.is_killed = True
        return len(clients)


@command(
    b"pause",
    {b"admin", b"connection", b"dangerous", b"slow"},
    b"client",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientPause(Command):
    """
    summary: Suspends commands processing.
    complexity: O(1)
    since: 3.0.0
    function: clientPauseCommand
    reply_schema:
      const: OK
    """

    server_context: ServerContext = dependency()
    timeout_seconds: int = positional_parameter()
    pause_all: bool = flag_parameter(token=b"ALL")
    pause_write: bool = flag_parameter(token=b"WRITE")

    def execute(self) -> ValueType:
        if self.pause_write and self.pause_all:
            raise ServerError(b"ERR Syntax error")
        elif not self.pause_write:
            self.pause_all = True

        self.server_context.pause_timeout = now_f_s() + self.timeout_seconds
        if self.pause_write:
            self.server_context.is_paused_for_write = True
        else:
            self.server_context.is_paused = True
        return RESP_OK


@command(
    b"unpause",
    {b"admin", b"connection", b"dangerous", b"slow"},
    b"client",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientUnpause(Command):
    """
    summary: Resumes processing commands from paused clients.
    complexity: O(N) Where N is the number of paused clients
    since: 6.2.0
    function: clientUnpauseCommand
    reply_schema:
      const: OK
    """

    server_context: ServerContext = dependency()
    timeout_seconds: int = positional_parameter()

    def execute(self) -> ValueType:
        self.server_context.is_paused = False
        self.server_context.is_paused_for_write = False
        return RESP_OK


@command(
    b"reply",
    {b"connection", b"slow"},
    b"client",
    flags={b"allow_busy", b"loading", b"noscript", b"no_multi", b"sentinel", b"stale"},
)
class ClientReply(Command):
    """
    summary: Instructs the server whether to reply to commands.
    complexity: O(1)
    since: 3.2.0
    function: clientReplyCommand
    reply_schema:
      const: OK
      description: >-
        When called with either OFF or SKIP subcommands, no reply is made. When called with ON, reply is OK.
    """

    client_context: ClientContext = dependency()

    mode: ReplyMode = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_client.reply_mode = self.mode

        if self.mode == ReplyMode.OFF:
            return DoNotReply

        return RESP_OK


class UnblockOption(Enum):
    timeout = b"TIMEOUT"
    error = b"ERROR"


@command(
    b"unblock",
    {b"admin", b"connection", b"dangerous", b"slow"},
    b"client",
    flags={b"admin", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientUnblock(Command):
    """
    summary: Unblocks a client blocked by a blocking command from a different connection.
    complexity: O(log N) where N is the number of client connections
    since: 5.0.0
    function: clientUnblockCommand
    reply_schema:
      oneOf:
      - const: 0
        description: If the client was unblocked successfully.
      - const: 1
        description: If the client wasn't unblocked.
    """

    server_context: ServerContext = dependency()

    client_id: int = positional_parameter()
    unblock_option: UnblockOption = positional_parameter(default=UnblockOption.timeout)

    _unblocked: int = field(init=False, default=0)

    async def before(self, in_multi: bool = False) -> None:
        if self.client_id not in self.server_context.clients:
            return

        client = self.server_context.clients[self.client_id]
        if client.blocking_context is None:
            return

        await client.blocking_context.queue.put(
            UnblockMessage.ERROR if self.unblock_option == UnblockOption.error else UnblockMessage.TIMEOUT
        )

        self._unblocked = 1

    def execute(self) -> ValueType:
        return self._unblocked


@command(
    b"setinfo",
    {b"connection", b"slow"},
    b"client",
    flags={b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientSetInformation(Command):
    """
    summary: Sets information specific to the client or connection.
    complexity: O(1)
    since: 7.2.0
    function: clientSetinfoCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()
    library_name: bytes | None = keyword_parameter(token=b"LIB-NAME", default=None)
    library_version: bytes | None = keyword_parameter(token=b"LIB-VER", default=None)

    def execute(self) -> ValueType:
        if self.library_name:
            self.client_context.current_client.library_name = self.library_name
        if self.library_version:
            self.client_context.current_client.library_version = self.library_version
        return RESP_OK


@command(b"echo", {b"connection"}, flags={b"fast", b"loading", b"stale"})
class Echo(Command):
    """
    summary: Returns the given string.
    complexity: O(1)
    since: 1.0.0
    function: echoCommand
    reply_schema:
      description: The given string
      type: string
    """

    message: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return self.message


@command(
    b"hello",
    {b"connection"},
    flags={b"allow_busy", b"fast", b"loading", b"noscript", b"no_auth", b"sentinel", b"stale"},
)
class Hello(Command):
    """
    summary: Handshakes with the server.
    complexity: O(1)
    since: 6.0.0
    function: helloCommand
    reply_schema:
      type: object
      additionalProperties: false
      properties:
        server:
          type: string
        version:
          type: string
        proto:
          const: 3
        id:
          type: integer
        mode:
          type: string
        role:
          type: string
        modules:
          type: array
          items:
            type: object
            additionalProperties: false
            properties:
              name:
                type: string
              ver:
                type: integer
              path:
                type: string
              args:
                type: array
                items:
                  type: string
        availability_zone:
          type: string
    """

    client_context: ClientContext = dependency()
    protocol_version: RespProtocolVersion | None = positional_parameter(
        default=None, parse_error=b"NOPROTO unsupported protocol version"
    )

    def execute(self) -> ValueType:
        if self.protocol_version is not None:
            self.client_context.protocol = self.protocol_version

        response = {
            b"server": b"valkey",
            b"version": self.client_context.server_context.information.server_version,
            b"proto": self.client_context.protocol,
            b"id": self.client_context.current_client.client_id,
            b"mode": b"standalone",
            b"role": b"master",
            b"modules": [],
        }

        if self.client_context.server_context.configurations.availability_zone != b"":
            response[b"availability_zone"] = self.client_context.server_context.configurations.availability_zone

        return response


@command(b"ping", {b"connection"}, flags={b"fast", b"sentinel"})
class Ping(Command):
    """
    summary: Returns the server's liveliness response.
    complexity: O(1)
    since: 1.0.0
    function: pingCommand
    reply_schema:
      anyOf:
      - const: PONG
        description: Default reply.
      - type: string
        description: Relay of given `message`.
    """

    client_context: ClientContext = dependency()
    subscriptions: ClientSubscriptions = dependency()

    message: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.client_context.protocol == RespProtocolVersion.RESP2 and self.subscriptions.active_subscriptions != 0:
            self.subscriptions.publish(b"pong", self.message or b"")
            return DoNotReply

        if self.message:
            return self.message
        return b"PONG"


@command(b"select", {b"connection"}, flags={b"allow_busy", b"fast", b"loading", b"stale"})
class SelectDatabase(Command):
    """
    summary: Changes the selected database.
    complexity: O(1)
    since: 1.0.0
    function: selectCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()
    index: int = positional_parameter()

    def execute(self) -> ValueType:
        self.client_context.current_database = self.index
        return RESP_OK


@command(b"quit", {b"connection"}, flags={b"allow_busy", b"fast", b"loading", b"noscript", b"no_auth", b"stale"})
class Quit(Command):
    """
    summary: Closes the connection.
    complexity: O(1)
    since: 1.0.0
    function: quitCommand
    reply_schema:
      const: OK
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        self.client_context.current_client.is_killed = True
        return RESP_OK


@command(b"info", {b"connection", b"slow"}, b"client", flags={b"loading", b"noscript", b"sentinel", b"stale"})
class ClientInformation(Command):
    """
    summary: Returns information about the connection.
    complexity: O(1)
    since: 6.2.0
    function: clientInfoCommand
    reply_schema:
      description: A unique string, as described at the CLIENT LIST page, for the current client.
      type: string
    """

    client_context: ClientContext = dependency()

    def execute(self) -> ValueType:
        return self.client_context.current_client.info


@command(
    b"caching",
    {b"connection", b"slow"},
    b"client",
    flags={b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientCaching(Command):
    """
    summary: Instructs the server whether to track the keys in the next request.
    complexity: O(1)
    since: 6.0.0
    function: clientCachingCommand
    reply_schema:
      const: OK
    """

    mode: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"capa", {b"connection", b"slow"}, b"client", flags={b"allow_busy", b"loading", b"noscript", b"stale"})
class ClientCapabilities(Command):
    """
    summary: A client claims its capability.
    complexity: O(1)
    since: 8.0.0
    function: clientCapaCommand
    reply_schema:
      const: OK
    """

    capabilities: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(
    b"getredir",
    {b"connection", b"slow"},
    b"client",
    flags={b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientGetRedirect(Command):
    """
    summary: >-
      Returns the client ID to which the connection's tracking notifications are redirected.
    complexity: O(1)
    since: 6.0.0
    function: clientGetredirCommand
    reply_schema:
      oneOf:
      - const: 0
        description: Not redirecting notifications to any client.
      - const: -1
        description: Client tracking is not enabled.
      - type: integer
        description: ID of the client we are redirecting the notifications to.
        minimum: 1
    """

    def execute(self) -> ValueType:
        return -1


@command(b"import-source", {b"connection", b"slow"}, b"client", flags={b"loading", b"noscript", b"stale"})
class ClientImportSource(Command):
    """
    summary: Marks this client as an import source when the server is in import mode.
    complexity: O(1)
    since: 8.1.0
    function: clientImportSourceCommand
    reply_schema:
      const: OK
    """

    mode: bytes = positional_parameter(default=b"OFF")

    def execute(self) -> ValueType:
        return RESP_OK


@command(
    b"no-evict",
    {b"admin", b"connection", b"dangerous", b"slow"},
    b"client",
    flags={b"admin", b"allow_busy", b"loading", b"noscript", b"sentinel", b"stale"},
)
class ClientNoEvict(Command):
    """
    summary: Sets the client eviction mode of the connection.
    complexity: O(1)
    since: 7.0.0
    function: clientNoEvictCommand
    reply_schema:
      const: OK
    """

    mode: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"no-touch", {b"connection", b"slow"}, b"client", flags={b"allow_busy", b"loading", b"noscript", b"stale"})
class ClientNoTouch(Command):
    """
    summary: >-
      Controls whether commands sent by the client affect the LRU/LFU of accessed keys.
    complexity: O(1)
    since: 7.2.0
    function: clientNoTouchCommand
    reply_schema:
      const: OK
    """

    mode: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK
