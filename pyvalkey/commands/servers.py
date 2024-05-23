from dataclasses import dataclass
from hashlib import sha256

from pyvalkey.commands.context import ClientContext, ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, RespError, ValueType


@dataclass
class ServerCommand(Command):
    server_context: ServerContext

    def execute(self) -> ValueType:
        raise NotImplementedError()


@ServerCommandsRouter.command(b"auth", [b"fast", b"connection"])
class Authorize(Command):
    acl: ACL = server_command_dependency()
    configurations: Configurations = server_command_dependency()
    client_context: ClientContext = server_command_dependency()

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
