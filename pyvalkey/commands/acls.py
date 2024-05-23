from os import urandom

from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL
from pyvalkey.resp import ValueType


@ServerCommandsRouter.command(b"help", [b"slow", b"connection"], b"acl")
class AclHelp(Command):
    def execute(self) -> ValueType:
        return ["genpass"]


@ServerCommandsRouter.command(b"genpass", [b"slow"], b"acl")
class AclGeneratePassword(Command):
    length: int = positional_parameter(default=64)

    def execute(self) -> ValueType:
        return urandom(self.length)


@ServerCommandsRouter.command(b"cat", [b"slow"], b"acl")
class AclCategory(Command):
    category: bytes | None = positional_parameter(default=None)

    def execute(self) -> ValueType:
        if self.category is not None:
            return ACL.get_category_commands(self.category)
        return ACL.get_categories()


@ServerCommandsRouter.command(b"deluser", [b"admin", b"slow", b"dangerous"], b"acl")
class AclDeleteUser(Command):
    acl: ACL = server_command_dependency()
    user_names: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        user_deleted = 0
        for user_name in self.user_names:
            if user_name == b"default":
                pass
            user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
        return user_deleted


@ServerCommandsRouter.command(b"getuser", [b"admin", b"slow", b"dangerous"], b"acl")
class AclGetUser(Command):
    acl: ACL = server_command_dependency()
    user_name: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if self.user_name not in self.acl:
            return None
        return self.acl[self.user_name].info
