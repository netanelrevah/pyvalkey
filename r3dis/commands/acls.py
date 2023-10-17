from os import urandom

from r3dis.acl import ACL
from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.consts import Commands
from r3dis.resp import RESP_OK

acl_commands_router = RedisCommandsRouter()


@acl_commands_router.command(Commands.AclHelp)
class AclHelp(Command):
    def execute(self):
        return ["genpass"]


@acl_commands_router.command(Commands.AclGeneratePassword)
class AclGeneratePassword(Command):
    length: int = redis_positional_parameter(default=64)

    def execute(self):
        return urandom(self.length)


@acl_commands_router.command(Commands.AclCategory)
class AclCategory(Command):
    category: bytes | None = redis_positional_parameter(default=None)

    def execute(self):
        if self.category is not None:
            return ACL.get_category_commands(self.category)
        return ACL.get_categories()


@acl_commands_router.command(Commands.AclDeleteUser)
class AclDeleteUser(Command):
    acl: ACL = redis_command_dependency()
    user_names: list[bytes] = redis_positional_parameter()

    def execute(self):
        user_deleted = 0
        for user_name in self.user_names:
            if user_name == b"default":
                pass
            user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
        return user_deleted


@acl_commands_router.command(Commands.AclGetUser)
class AclGetUser(Command):
    acl: ACL = redis_command_dependency()
    user_name: bytes = redis_positional_parameter()

    def execute(self):
        if self.user_name not in self.acl:
            return
        return self.acl[self.user_name].info


@acl_commands_router.command(Commands.AclSetUser)
class AclSetUser(Command):
    acl: ACL = redis_command_dependency()
    user_name: bytes = redis_positional_parameter()
    rules: list[bytes] = redis_positional_parameter()

    def execute(self):
        acl_user = self.acl.get_or_create_user(self.user_name)

        for rule in self.rules:
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
                    command = Commands(command)
                except TypeError:
                    if b"|" in command:
                        command, first_parameter = command.split(b"|")
                acl_user.add_allowed_command(command, first_parameter)
                continue
        return RESP_OK
