from dataclasses import dataclass
from hashlib import sha256

from r3dis.commands.context import ServerContext
from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.acl import ACL
from r3dis.database_objects.configurations import Configurations
from r3dis.database_objects.errors import RedisException
from r3dis.resp import RESP_OK, RespError


@dataclass
class ServerCommand(Command):
    server_context: ServerContext

    def execute(self):
        raise NotImplementedError()


@RedisCommandsRouter.command(b"auth", [b"fast", b"connection"])
class Authorize(Command):
    acl: ACL = redis_command_dependency()
    configurations: Configurations = redis_command_dependency()

    username: bytes | None = redis_positional_parameter(default=None)
    password: bytes = redis_positional_parameter()

    def execute(self):
        password_hash = sha256(self.password).hexdigest().encode()
        if self.username is not None:
            if self.username not in self.acl:
                raise RedisException(b"WRONGPASS invalid username-password pair or user is disabled.")
            if self.username == b"default" and password_hash == self.configurations.requirepass:
                return RESP_OK
            if password_hash not in self.acl[self.username].passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            return RESP_OK

        if self.configurations.requirepass and password_hash == self.configurations.requirepass:
            return RESP_OK
        raise RedisException(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )
