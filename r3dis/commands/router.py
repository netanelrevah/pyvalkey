from dataclasses import dataclass, field

from r3dis.commands.acls import (
    AclCategory,
    AclDeleteUser,
    AclGeneratePassword,
    AclGetUser,
    AclSetUser,
)
from r3dis.commands.clients import (
    ClientGetName,
    ClientId,
    ClientKill,
    ClientList,
    ClientPause,
    ClientReply,
    ClientSetName,
    ClientUnpause,
)
from r3dis.commands.configs import ConfigGet, ConfigSet
from r3dis.commands.context import ClientContext
from r3dis.commands.core import CommandParser
from r3dis.commands.database_context.routes import fill_database_string_commands
from r3dis.consts import Commands
from r3dis.errors import RouterKeyError


@dataclass
class RouteParser(CommandParser):
    routes: dict[Commands, CommandParser] = field(default_factory=dict)
    parent_command: Commands | None = None

    def parse(self, parameters: list[bytes]):
        command = parameters.pop(0).upper()

        command_to_route = Commands(command)
        if self.parent_command:
            command_to_route = Commands(self.parent_command.value + b"|" + command)

        try:
            command_parser = self.routes[command_to_route]
        except KeyError:
            raise RouterKeyError()
        if not command_parser:
            raise RouterKeyError()
        command = command_parser.parse(parameters)
        return command


def create_base_router(client_context: ClientContext):
    router = RouteParser()

    fill_database_string_commands(router, client_context.database)
    # ACL
    router.routes[Commands.Acl] = create_acl_router(client_context)
    # Config
    router.routes[Commands.Config] = create_config_router(client_context)
    # Client
    router.routes[Commands.Client] = create_client_router(client_context)

    return router


def create_acl_router(command_context: ClientContext):
    router = RouteParser(command_context, parent_command=Commands.Acl)
    # Acl
    router.routes[Commands.AclCategory] = AclCategory(command_context)
    router.routes[Commands.AclDelUser] = AclDeleteUser(command_context)
    router.routes[Commands.AclDryRun] = None
    router.routes[Commands.AclGenPass] = AclGeneratePassword(command_context)
    router.routes[Commands.AclGetUser] = AclGetUser(command_context)
    router.routes[Commands.AclList] = None
    router.routes[Commands.AclLoad] = None
    router.routes[Commands.AclLog] = None
    router.routes[Commands.AclSave] = None
    router.routes[Commands.AclSetUser] = AclSetUser(command_context)
    router.routes[Commands.AclUsers] = None
    router.routes[Commands.AclWhoAmI] = None
    router.routes[Commands.AclHelp] = None

    return router


def create_client_router(command_context: ClientContext):
    router = RouteParser(command_context, parent_command=Commands.Client)
    # Acl
    router.routes[Commands.ClientSetName] = ClientSetName(command_context)
    router.routes[Commands.ClientPause] = ClientPause(command_context)
    router.routes[Commands.ClientUnpause] = ClientUnpause(command_context)
    router.routes[Commands.ClientReply] = ClientReply(command_context)
    router.routes[Commands.ClientKill] = ClientKill(command_context)
    router.routes[Commands.ClientGetName] = ClientGetName(command_context)
    router.routes[Commands.ClientId] = ClientId(command_context)
    router.routes[Commands.ClientList] = ClientList(command_context)

    return router


def create_config_router(command_context: ClientContext):
    router = RouteParser(command_context, parent_command=Commands.Config)
    # Acl
    router.routes[Commands.ConfigGet] = ConfigGet(command_context)
    router.routes[Commands.ConfigSet] = ConfigSet(command_context)

    return router
