from r3dis.commands.core import Command
from r3dis.commands.dependencies import redis_command_dependency
from r3dis.commands.parameters import redis_positional_parameter
from r3dis.commands.router import RedisCommandsRouter
from r3dis.database_objects.acl import ACL, ACLUser
from r3dis.database_objects.errors import RedisException
from r3dis.resp import RESP_OK


@RedisCommandsRouter.command(b"dryrun", [b"admin", b"slow", b"dangerous"], b"acl")
class AclDryRun(Command):
    acl: ACL = redis_command_dependency()

    username: bytes = redis_positional_parameter()
    command: bytes = redis_positional_parameter()
    args: list[bytes] = redis_positional_parameter()

    def execute(self):
        acl_user = self.acl[self.username]
        if acl_user.acl_list.is_allowed(self.command, self.args[0]):
            raise RedisException(f"This user has no permissions to run the '{self.command}' command".encode())
        return RESP_OK


@RedisCommandsRouter.command(b"setuser", [b"admin", b"slow", b"dangerous"], b"acl")
class AclSetUser(Command):
    acl: ACL = redis_command_dependency()
    user_name: bytes = redis_positional_parameter()
    rules: list[bytes] = redis_positional_parameter()

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
            if rule.startswith(b">"):
                acl_user.add_password(rule[1:])
                continue
            if rule.startswith(b"+"):
                command = rule[1:].lower()

                first_parameter = None
                if b"|" not in command:
                    if command not in RedisCommandsRouter.ROUTES:
                        raise TypeError()
                    acl_user.acl_list.allow(command, b"")
                    continue

                left, right = command.split(b"|")
                if left not in RedisCommandsRouter.ROUTES:
                    raise TypeError()

                if isinstance(RedisCommandsRouter.ROUTES[left], dict):
                    command, first_parameter = left, right
                acl_user.acl_list.allow(command, first_parameter)
                continue
        return RESP_OK
