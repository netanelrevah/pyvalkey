from dataclasses import dataclass
from hashlib import sha256

from pyvalkey.commands.context import ServerContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.errors import ServerException
from pyvalkey.resp import RESP_OK, RespError


@dataclass
class ServerCommand(Command):
    server_context: ServerContext

    def execute(self):
        raise NotImplementedError()


@ServerCommandsRouter.command(b"auth", [b"fast", b"connection"])
class Authorize(Command):
    acl: ACL = server_command_dependency()
    configurations: Configurations = server_command_dependency()

    username: bytes | None = positional_parameter(default=None)
    password: bytes = positional_parameter()

    def execute(self):
        password_hash = sha256(self.password).hexdigest().encode()
        if self.username is not None:
            if self.username not in self.acl:
                raise ServerException(b"WRONGPASS invalid username-password pair or user is disabled.")
            if self.username == b"default" and password_hash == self.configurations.requirepass:
                return RESP_OK
            if password_hash not in self.acl[self.username].passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            return RESP_OK

        if self.configurations.requirepass and password_hash == self.configurations.requirepass:
            return RESP_OK
        raise ServerException(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )
