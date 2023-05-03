from dataclasses import dataclass
from os import urandom

from r3dis.acl import ACL
from r3dis.commands.core import CommandHandler
from r3dis.consts import Command
from r3dis.errors import RedisSyntaxError
from r3dis.resp import RESP_OK


@dataclass
class AclHelp(CommandHandler):
    def handle(self):
        return ["genpass"]

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if parameters:
            raise RedisSyntaxError()


@dataclass
class AclGeneratePassword(CommandHandler):
    def handle(self, length: int):
        return urandom(length)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            raise RedisSyntaxError()
        if len(parameters) == 1:
            return int(parameters.pop(0))
        return 64


@dataclass
class AclCategory(CommandHandler):
    def handle(self, category: bytes | None):
        if category is not None:
            return ACL.get_category_commands(category)
        return ACL.get_categories()

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            raise RedisSyntaxError()
        if len(parameters) == 1:
            return parameters.pop(0)
        return None


@dataclass
class AclDeleteUser(CommandHandler):
    def handle(self, user_names: list[bytes]):
        user_deleted = 0
        for user_name in user_names:
            if user_name == b"default":
                pass
            user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
        return user_deleted

    @classmethod
    def parse(cls, parameters: list[bytes]):
        return parameters


@dataclass
class AclGetUser(CommandHandler):
    def handle(self, user_name: bytes):
        if user_name not in self.acl:
            return
        return self.acl[user_name].info

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            raise RedisSyntaxError()
        return parameters.pop(0)


@dataclass
class AclSetUser(CommandHandler):
    def handle(self, user_name: bytes, rules: list[bytes]):
        acl_user = self.acl.get_or_create_user(user_name)

        for rule in rules:
            if rule == b"reset":
                acl_user.reset()
                continue
            if rule == b"on":
                acl_user.is_active = True
                continue
            if rule == b"off":
                acl_user.is_active = False
                continue
            if rule.startswith(b">"):
                acl_user.add_password(rule[1:])
                continue
            if rule.startswith(b"+"):
                command = rule[1:].lower()
                first_parameter = None
                try:
                    command = Command(command)
                except TypeError:
                    if b"|" in command:
                        command, first_parameter = command.split(b"|")
                acl_user.add_allowed_command(command, first_parameter)
                continue
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        user_name = parameters.pop(0)

        return user_name, parameters
