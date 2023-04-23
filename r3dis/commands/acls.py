from r3dis.commands.core import CommandHandler
from r3dis.consts import Command
from r3dis.resp import RESP_OK


@dataclass
class AclSetUser(CommandHandler):
    def handle(self, user_name: bytes, rules: list[bytes]):
        acl_user = self.acl.get_or_create_user(user_name)

        for rule in rules:
            if rule == b'reset':
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
                    if b'|' in command:
                        command, first_parameter = command.split(b'|')
                acl_user.allowed_commands[command] = first_parameter
                continue
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        user_name = parameters.pop(0)

        return user_name, parameters
