from dataclasses import dataclass
from hashlib import sha256

from r3dis.commands.handlers import CommandHandler
from r3dis.errors import RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK, RespError


@dataclass
class Authorize(CommandHandler):
    def handle(self, password: bytes, username: bytes | None = None):
        password_hash = sha256(password).hexdigest().encode()
        if username is not None:
            if username not in self.acl:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            if username == b"default" and password_hash == self.configurations.requirepass:
                return RESP_OK
            if password_hash not in self.acl[username].passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            return RESP_OK

        if self.configurations.requirepass and password_hash == self.configurations.requirepass:
            return RESP_OK
        return RespError(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if not parameters or len(parameters) > 2:
            return RedisWrongNumberOfArguments()
        if len(parameters) > 1:
            username = parameters.pop(0)
            password = parameters.pop(0)
            return password, username
        return parameters.pop(0), None
