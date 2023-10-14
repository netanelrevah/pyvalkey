from dataclasses import dataclass
from functools import partial
from hashlib import sha256

from mypy.typeshed.stdlib.imaplib import Commands

from r3dis.commands.context import ServerContext
from r3dis.commands.core import Command
from r3dis.commands.parsers import SmartCommandParser, redis_positional_parameter
from r3dis.resp import RESP_OK, RespError


@dataclass
class ServerCommand(Command):
    server_context: ServerContext

    def execute(self):
        raise NotImplementedError()


@dataclass
class Authorize(ServerCommand):
    username: bytes = redis_positional_parameter()
    password: bytes | None = redis_positional_parameter(default=None)

    def execute(self):
        password_hash = sha256(self.password).hexdigest().encode()
        if self.username is not None:
            if self.username not in self.server_context.acl:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            if self.username == b"default" and password_hash == self.server_context.configurations.requirepass:
                return RESP_OK
            if password_hash not in self.server_context.acl[self.username].passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            return RESP_OK

        if (
            self.server_context.configurations.requirepass
            and password_hash == self.server_context.configurations.requirepass
        ):
            return RESP_OK
        return RespError(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )


@dataclass
class Information(ServerCommand):
    def execute(self):
        return self.server_context.information.all()


def create_smart_command_parser(
    router, command: Commands, command_cls: type[ServerCommand], context: ServerContext, *args, **kwargs
):
    router.routes[command] = SmartCommandParser(command_cls, partial(command_cls, context, *args, **kwargs))
