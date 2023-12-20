from os import urandom

from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.acl import ACL


@RedisCommandsRouter.command(b"help", [b"slow", b"connection"], b"acl")
class AclHelp(Command):
    def execute(self):
        return ["genpass"]


@RedisCommandsRouter.command(b"genpass", [b"slow"], b"acl")
class AclGeneratePassword(Command):
    length: int = redis_positional_parameter(default=64)

    def execute(self):
        return urandom(self.length)


@RedisCommandsRouter.command(b"cat", [b"slow"], b"acl")
class AclCategory(Command):
    category: bytes | None = redis_positional_parameter(default=None)

    def execute(self):
        if self.category is not None:
            return ACL.get_category_commands(self.category)
        return ACL.get_categories()


@RedisCommandsRouter.command(b"deluser", [b"admin", b"slow", b"dangerous"], b"acl")
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


@RedisCommandsRouter.command(b"getuser", [b"admin", b"slow", b"dangerous"], b"acl")
class AclGetUser(Command):
    acl: ACL = redis_command_dependency()
    user_name: bytes = redis_positional_parameter()

    def execute(self):
        if self.user_name not in self.acl:
            return
        return self.acl[self.user_name].info
