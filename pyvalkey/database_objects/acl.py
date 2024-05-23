from __future__ import annotations

import fnmatch
from collections import defaultdict
from dataclasses import dataclass, field, fields
from hashlib import sha256
from typing import TYPE_CHECKING, ClassVar, Self

from pyvalkey.commands.parameters import ParameterMetadata
from pyvalkey.database_objects.errors import CommandPermissionError, KeyPermissionError, NoPermissionError

if TYPE_CHECKING:
    from pyvalkey.commands.core import Command


def check_command_by_category(command_name: bytes, category: bytes) -> bool:
    return category in ACL.COMMAND_CATEGORIES[command_name]


@dataclass(unsafe_hash=True)
class KeyPattern:
    pattern: bytes
    mode: bytes = b""

    def check(self, key: bytes, key_mode: bytes) -> bool:
        if self.mode and self.mode != key_mode:
            return False
        return fnmatch.fnmatch(key, self.pattern)

    @classmethod
    def create(cls, rule: bytes) -> Self:
        mode = b""
        if rule.startswith(b"%"):
            mode = rule[1 : rule.index(b"~")]
        return cls(rule[rule.index(b"~") + 1 :], mode)

    def to_bytes(self) -> bytes:
        return (b"%" + self.mode if self.mode else b"") + b"~" + self.pattern


@dataclass(unsafe_hash=True)
class CommandRule:
    rule: bytes
    allowed: bool
    is_category: bool

    def check(self, command_name: bytes) -> bool:
        if self.is_category:
            if self.allowed:
                return self.rule == b"all" or check_command_by_category(command_name, self.rule)
            else:
                return self.rule != b"all" and check_command_by_category(command_name, self.rule)
        if command_name == self.rule:
            return self.allowed
        return not self.allowed

    @classmethod
    def create(cls, rule: bytes) -> Self:
        return cls(rule.lstrip(b"+@-"), rule[:1] == b"+", rule.startswith(b"+@") or rule.startswith(b"-@"))

    def to_bytes(self) -> bytes:
        return (b"+" if self.allowed else b"-") + (b"@" if self.is_category else b"") + self.rule


@dataclass
class Permission:
    keys_patterns: set[KeyPattern] = field(default_factory=set)
    command_rules: list[CommandRule] = field(default_factory=lambda: [CommandRule.create(b"-@all")])
    channel_rules: set[bytes] = field(default_factory=set)

    def check_keys_patterns(self, key: bytes, key_mode: bytes) -> bool:
        for key_pattern in self.keys_patterns:
            if key_pattern.check(key, key_mode):
                return True
        return False

    def check_permissions(self, command: Command) -> None:
        command_name = ACL.COMMANDS_NAMES[command.__class__]

        is_fields_allowed = {}
        for command_field in fields(command):
            key_mode = command_field.metadata.get(ParameterMetadata.KEY_MODE, None)
            if not key_mode:
                continue

            is_fields_allowed[command_field.name] = False
            if self.check_keys_patterns(getattr(command, command_field.name), key_mode):
                is_fields_allowed[command_field.name] = True
        key_allowed = not self.keys_patterns or not is_fields_allowed or any(is_fields_allowed.values())

        results = []
        for command_rule in self.command_rules:
            results.append(command_rule.check(command_name))
        command_allowed = (results[0] and all(results)) or (not results[0] and any(results))

        if not command_allowed:
            raise CommandPermissionError(command_name)
        if not key_allowed:
            raise KeyPermissionError()

    def info(self) -> dict[bytes, bytes]:
        return {
            b"commands": b" ".join(map(CommandRule.to_bytes, self.command_rules)),
            b"keys": b" ".join(map(KeyPattern.to_bytes, self.keys_patterns)),
            b"channels": b" ".join(self.channel_rules),
        }


@dataclass
class ACLUser:
    name: bytes

    is_active: bool = False
    is_no_password_user: bool = False
    passwords: set[bytes] = field(default_factory=set)
    root_permissions: Permission = field(default_factory=lambda: Permission())
    selectors: list[Permission] = field(default_factory=list)

    def add_password(self, password: bytes) -> None:
        self.passwords.add(sha256(password).hexdigest().encode())

    @property
    def info(self) -> dict[bytes, list | bytes]:
        flags = [b"on" if self.is_active else b"off"]
        if not self.passwords:
            flags.append(b"nopass")

        return {
            b"flags": flags,
            b"passwords": list(self.passwords),
            **self.root_permissions.info(),
            b"selectors": [selector.info() for selector in self.selectors],
        }

    def reset(self) -> None:
        self.is_active = False
        self.is_no_password_user = False
        self.passwords = set()
        self.root_permissions = Permission()
        self.selectors = []

    def reset_keys(self) -> None:
        self.root_permissions.keys_patterns = set()

    def clear_selectors(self) -> None:
        self.selectors = []

    def no_password(self) -> None:
        self.is_no_password_user = True
        self.passwords = set()

    def check_permissions(self, command: Command) -> None:
        exception = None
        for selector in [self.root_permissions, *self.selectors]:
            try:
                selector.check_permissions(command)
                return
            except NoPermissionError as e:
                if exception is None or isinstance(e, KeyPermissionError):
                    exception = e
        if exception:
            raise exception


class ACL(dict[bytes, ACLUser]):
    CATEGORIES: ClassVar[dict[bytes, set[bytes]]] = defaultdict(set)
    COMMANDS_NAMES: ClassVar[dict[type[Command], bytes]] = {}
    COMMAND_CATEGORIES: ClassVar[dict[bytes, set]] = {}

    def get_or_create_user(self, user_name: bytes) -> ACLUser:
        if user_name not in self:
            self[user_name] = ACLUser(user_name)
        return self[user_name]

    def delete_user(self, user_name: bytes) -> ACLUser | None:
        return self.pop(user_name, None)

    def delete_users(self, user_names: list[bytes]) -> int:
        deleted_users = 0
        for user_name in user_names:
            if user_name == b"default":
                continue
            deleted_users += 1 if self.delete_user(user_name) is not None else 0
        return deleted_users

    @classmethod
    def get_categories(cls) -> list[bytes]:
        return list(cls.CATEGORIES.keys())

    @classmethod
    def get_category_commands(cls, category: bytes) -> list[bytes]:
        return list(cls.CATEGORIES.get(category, set()))

    @classmethod
    def create(cls) -> ACL:
        return ACL({b"default": ACLUser(b"default")})
