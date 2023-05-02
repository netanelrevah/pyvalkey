from dataclasses import dataclass, field
from hashlib import sha256

from r3dis.consts import COMMANDS_PER_CATEGORY, ACLCategories, Command


@dataclass
class ACLUser:
    name: bytes

    is_active: bool = False
    passwords: set[bytes] = field(default_factory=set)
    allowed_commands: dict[Command, bytes] = field(default_factory=dict)

    def add_password(self, password: bytes):
        self.passwords.add(sha256(password).hexdigest().encode())

    @property
    def info(self):
        return {
            b"flags": [b"on" if self.is_active else b"off"],
            b"passwords": list(self.passwords),
        }


class ACL(dict[bytes, ACLUser]):
    def get_or_create_user(self, user_name: bytes) -> ACLUser:
        if user_name not in self:
            self[user_name] = ACLUser(user_name)
        return self[user_name]

    def delete_user(self, user_name: bytes):
        return self.pop(user_name, None)

    def delete_users(self, user_names: list[bytes]):
        deleted_users = 0
        for user_name in user_names:
            if user_name == b"default":
                continue
            deleted_users += 1 if self.delete_user(user_name) is not None else 0
        return deleted_users

    def add_allowed_command(self, command: Command, first_parameter: bytes):
        pass

    @classmethod
    def get_categories(cls):
        return [acl_category.encode() for acl_category in ACLCategories]

    @classmethod
    def get_category_commands(cls, category: bytes):
        return [
            command_name.encode() for command_name in COMMANDS_PER_CATEGORY.get(ACLCategories(category.decode()), [])
        ]

    @classmethod
    def create(cls):
        return ACL({b"default": ACLUser(b"default")})
