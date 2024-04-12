from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL, ACLUser
from pyvalkey.resp import RESP_OK


@ServerCommandsRouter.command(b"dryrun", [b"admin", b"slow", b"dangerous"], b"acl")
class AclDryRun(Command):
    acl: ACL = server_command_dependency()

    username: bytes = positional_parameter()
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self):
        acl_user = self.acl[self.username]
        if not acl_user.acl_list.is_allowed(self.command, self.args[0]):
            return f"This user has no permissions to run the '{self.command.decode()}' command".encode()
        return RESP_OK


@ServerCommandsRouter.command(b"setuser", [b"admin", b"slow", b"dangerous"], b"acl")
class AclSetUser(Command):
    acl: ACL = server_command_dependency()
    user_name: bytes = positional_parameter()
    rules: list[bytes] = positional_parameter()

    def execute(self):
        acl_user: ACLUser = self.acl.get_or_create_user(self.user_name)

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
            if rule == b"nopass":
                acl_user.passwords = set()
                continue
            if rule.startswith(b">"):
                acl_user.add_password(rule[1:])
                continue
            if rule.startswith(b"-"):
                pass

            if rule.startswith(b"+"):
                command = rule[1:].lower()

                first_parameter = None
                if b"|" not in command:
                    if command not in ServerCommandsRouter.ROUTES and (
                        not command.startswith(b"@") or command[1:] not in ACL.CATEGORIES
                    ):
                        raise TypeError()
                    acl_user.acl_list.allow(command)
                    continue

                left, right = command.split(b"|")
                if left not in ServerCommandsRouter.ROUTES:
                    raise TypeError()

                if isinstance(ServerCommandsRouter.ROUTES[left], dict):
                    command, first_parameter = left, right
                acl_user.acl_list.allow(command, first_parameter)
                continue
            raise NotImplementedError()
        return RESP_OK
