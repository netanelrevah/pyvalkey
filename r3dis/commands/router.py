from dataclasses import dataclass, field

from r3dis.commands.acl.core import Authorize
from r3dis.commands.acls import (
    AclCategory,
    AclDeleteUser,
    AclGeneratePassword,
    AclGetUser,
    AclSetUser,
)
from r3dis.commands.client.core import Echo, SelectDatabase
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
from r3dis.commands.core import BytesParametersParser, ClientContext, CommandParser
from r3dis.commands.database.core import DatabaseSize, FlushDatabase, Keys
from r3dis.commands.database.routes import fill_database_string_commands
from r3dis.commands.information.core import Information
from r3dis.commands.lists import (
    ListIndex,
    ListInsert,
    ListLength,
    ListPop,
    ListPush,
    ListRange,
    ListRemove,
)
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
    # List
    router.routes[Commands.ListRange] = ListRange(client_context)
    router.routes[Commands.ListPush] = ListPush(client_context)
    router.routes[Commands.ListPop] = ListPop(client_context)
    router.routes[Commands.ListRemove] = ListRemove(client_context)
    router.routes[Commands.ListPushAtTail] = ListPush(client_context, at_tail=True)
    router.routes[Commands.ListLength] = ListLength(client_context)
    router.routes[Commands.ListIndex] = ListIndex(client_context)
    router.routes[Commands.ListInsert] = ListInsert(client_context)
    # ACL
    router.routes[Commands.Acl] = create_acl_router(client_context)
    # Config
    router.routes[Commands.Config] = create_config_router(client_context)
    # Client
    router.routes[Commands.Client] = create_client_router(client_context)
    # Database
    router.routes[Commands.FlushDatabase] = FlushDatabase(client_context)
    router.routes[Commands.Select] = SelectDatabase(client_context)
    router.routes[Commands.Keys] = BytesParametersParser(client_context, Keys, 1)
    router.routes[Commands.DatabaseSize] = BytesParametersParser(client_context, DatabaseSize, 0)
    # Management
    router.routes[Commands.Authorize] = Authorize(client_context)
    router.routes[Commands.Information] = Information(client_context)
    router.routes[Commands.Ping] = Echo(client_context, ping_mode=True)
    router.routes[Commands.Echo] = Echo(client_context)

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
