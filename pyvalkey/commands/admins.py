from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.database_objects.acl import ACL, ACLUser, CommandRule, KeyPattern, Permission
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ValueType


@ServerCommandsRouter.command(b"dryrun", [b"admin", b"slow", b"dangerous"], b"acl")
class AclDryRun(Command):
    acl: ACL = server_command_dependency()

    username: bytes = positional_parameter()
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return RESP_OK


@ServerCommandsRouter.command(b"setuser", [b"admin", b"slow", b"dangerous"], b"acl")
class AclSetUser(Command):
    acl: ACL = server_command_dependency()
    user_name: bytes = positional_parameter()
    rules: list[bytes] = positional_parameter()

    def parse_selector(self, selector: list[bytes], permission: Permission | None = None) -> Permission:
        permission = permission or Permission()
        for rule in selector:
            if rule == b"allcommands":
                rule = b"+@all"
            if rule == b"+@all" or rule == b"-@all":
                permission.command_rules.clear()
            if rule.startswith(b"-") or rule.startswith(b"+"):
                permission.command_rules.append(CommandRule.create(rule))
                continue
            if rule == b"allkeys":
                rule = b"~*"
            if rule.startswith(b"%R~") or rule.startswith(b"%RW~") or rule.startswith(b"%W~") or rule.startswith(b"~"):
                if KeyPattern.create(b"~*") in permission.keys_patterns:
                    raise ValueError(
                        b"Adding a pattern after the * pattern (or the 'allkeys' flag)"
                        b" is not valid and does not have any effect."
                        b" Try 'resetkeys' to start with an empty list of patterns"
                    )
                permission.keys_patterns.add(KeyPattern.create(rule))
                continue
            if rule == b"allchannels":
                rule = b"&*"
            if rule.startswith(b"&"):
                if b"&*" in permission.channel_rules:
                    raise ValueError(
                        b"Adding a pattern after the * pattern (or the 'allchannels' flag)"
                        b" is not valid and does not have any effect."
                        b" Try 'resetchannels' to start with an empty list of channels"
                    )
                permission.channel_rules.add(rule)
                continue

            raise ValueError(b"Syntax error")
        return permission

    def execute(self) -> ValueType:
        callbacks = []

        root_permission_role = []
        while self.rules:
            rule = self.rules.pop(0)
            if rule == b"resetkeys":
                callbacks.append(ACLUser.reset_keys)
                continue
            if rule == b"reset":
                callbacks.append(ACLUser.reset)
                continue
            if rule == b"clearselectors":
                callbacks.append(ACLUser.clear_selectors)
                continue
            if rule == b"on":
                callbacks.append(lambda _acl_user: setattr(_acl_user, "is_active", True))  # type: ignore[arg-type]
                continue
            if rule == b"off":
                callbacks.append(lambda _acl_user: setattr(_acl_user, "is_active", False))  # type: ignore[arg-type]
                continue
            if rule == b"nopass":
                callbacks.append(ACLUser.no_password)
                continue
            if rule.startswith(b">"):
                callbacks.append(lambda _acl_user, password=rule[1]: _acl_user.add_password(password))  # type: ignore[misc]
                continue
            if rule.startswith(b"("):
                full_rule = rule
                while not full_rule.endswith(b")"):
                    try:
                        full_rule += b" " + self.rules.pop(0)
                    except IndexError:
                        raise ServerError(b"ERR Unmatched parenthesis in acl selector starting at '" + rule + b"'.")

                try:
                    selector = self.parse_selector(full_rule[1:-1].split())
                except ValueError as e:
                    raise ServerError(b"ERR Error in ACL SETUSER modifier '" + full_rule + b"': " + e.args[0])
                callbacks.append(lambda _acl_user, s=selector: _acl_user.selectors.append(s))  # type: ignore[misc]
                continue
            if rule.startswith(b"-@") or rule.startswith(b"+@") or rule.startswith(b"+") or rule.startswith(b"-"):
                root_permission_role.append(rule)
                continue
            if rule.startswith(b"~"):
                root_permission_role.append(rule)
                continue
            if rule.startswith(b"%"):
                root_permission_role.append(rule)
                continue
            raise ServerError(b"ERR Error in ACL SETUSER modifier '" + rule + b"': Syntax error")

        acl_user: ACLUser = self.acl.get_or_create_user(self.user_name)
        for callback in callbacks:
            callback(acl_user)
        if root_permission_role:
            self.parse_selector(root_permission_role, acl_user.root_permissions)
        return RESP_OK
