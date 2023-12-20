from collections import defaultdict
from dataclasses import dataclass, field
from hashlib import sha256


class ACLList:
    def is_allowed(self, command, sub_command_or_first_argument=None):
        raise NotImplementedError()

    def allow(self, command, sub_command_or_first_argument=None):
        raise NotImplementedError()


@dataclass
class ACLWhitelist(ACLList):
    acl_list: dict[bytes, set[bytes]] = field(default_factory=lambda: defaultdict(set))

    def is_allowed(self, command, sub_command_or_first_argument=None):
        if command not in self.acl_list:
            return False
        if sub_command_or_first_argument and sub_command_or_first_argument not in self.acl_list[command]:
            return False
        return True

    def allow(self, command, sub_command_or_first_argument=None):
        self.acl_list[command].add(sub_command_or_first_argument)


@dataclass
class ACLBlacklist(ACLList):
    acl_list: dict[bytes, set[bytes]] = field(default_factory=dict)

    def is_allowed(self, command, sub_command_or_first_argument=None):
        if command not in self.acl_list:
            return True
        if sub_command_or_first_argument and sub_command_or_first_argument not in self.acl_list[command]:
            return True
        return False

    def allow(self, command, sub_command_or_first_argument=None):
        if command not in self.acl_list:
            return

        if sub_command_or_first_argument:
            self.acl_list[command].discard(sub_command_or_first_argument)
            return

        self.acl_list.pop(command, None)


@dataclass
class ACLUser:
    name: bytes

    is_active: bool = False
    passwords: set[bytes] = field(default_factory=set)
    acl_list: ACLList = field(default_factory=ACLWhitelist)

    def add_password(self, password: bytes):
        self.passwords.add(sha256(password).hexdigest().encode())

    @property
    def info(self):
        return {
            b"flags": [b"on" if self.is_active else b"off"],
            b"passwords": list(self.passwords),
        }


class ACL(dict[bytes, ACLUser]):
    CATEGORIES: dict[bytes, set[bytes]] = defaultdict(set)

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

    @classmethod
    def get_categories(cls):
        return list(cls.CATEGORIES.keys())

    @classmethod
    def get_category_commands(cls, category: bytes):
        return list(cls.CATEGORIES.get(category, []))

    @classmethod
    def create(cls):
        return ACL({b"default": ACLUser(b"default")})
