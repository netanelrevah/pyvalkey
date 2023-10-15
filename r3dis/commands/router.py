from dataclasses import dataclass, field

from r3dis.commands.context import ClientContext
from r3dis.commands.core import Command
from r3dis.commands.parsers import redis_command
from r3dis.consts import Commands
from r3dis.errors import RouterKeyError


@dataclass
class RedisCommandsRouter:
    routes: dict[Commands, type[Command] | "RedisCommandsRouter"] = field(default_factory=dict)
    parent_command: Commands | None = None

    def route(self, parameters: list[bytes], client_context: ClientContext):
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
        if isinstance(command_parser, RedisCommandsRouter):
            return command_parser.route(parameters, client_context)
        return command_parser.create(parameters, client_context)

    def command(self, command: Commands):
        def _command_wrapper(command_cls: Command):
            command_cls = redis_command(command_cls)
            self.routes[command] = command_cls
            return command_cls

        return _command_wrapper

    def extend(self, router: "RedisCommandsRouter"):
        for command, command_cls in router.routes.items():
            if isinstance(command_cls, RedisCommandsRouter):
                self.child(command).extend(command_cls)
            else:
                self.routes[command] = command_cls

    def child(self, command: Commands):
        if command not in self.routes:
            self.routes[command] = RedisCommandsRouter()
        return self.routes[command]


# def create_base_router():
#     router = RedisCommandsRouter()
#
#     fill_database_string_commands(router, client_context.database)
#     # ACL
#     router.routes[Commands.Acl] = create_acl_router(client_context)
#     # Config
#     router.routes[Commands.Config] = create_config_router(client_context)
#     # Client
#     router.routes[Commands.Client] = create_client_router(client_context)
#
#     return router
#
#
# def create_acl_router(command_context: ClientContext):
#     router = RedisCommandsRouter(command_context, parent_command=Commands.Acl)
#     # Acl
#     router.routes[Commands.AclCategory] = AclCategory(command_context)
#     router.routes[Commands.AclDelUser] = AclDeleteUser(command_context)
#     router.routes[Commands.AclDryRun] = None
#     router.routes[Commands.AclGenPass] = AclGeneratePassword(command_context)
#     router.routes[Commands.AclGetUser] = AclGetUser(command_context)
#     router.routes[Commands.AclList] = None
#     router.routes[Commands.AclLoad] = None
#     router.routes[Commands.AclLog] = None
#     router.routes[Commands.AclSave] = None
#     router.routes[Commands.AclSetUser] = AclSetUser(command_context)
#     router.routes[Commands.AclUsers] = None
#     router.routes[Commands.AclWhoAmI] = None
#     router.routes[Commands.AclHelp] = None
#
#     return router
#
#
# def create_client_router(command_context: ClientContext):
#     router = RedisCommandsRouter(command_context, parent_command=Commands.Client)
#     # Acl
#     router.routes[Commands.ClientSetName] = ClientSetName(command_context)
#     router.routes[Commands.ClientPause] = ClientPause(command_context)
#     router.routes[Commands.ClientUnpause] = ClientUnpause(command_context)
#     router.routes[Commands.ClientReply] = ClientReply(command_context)
#     router.routes[Commands.ClientKill] = ClientKill(command_context)
#     router.routes[Commands.ClientGetName] = ClientGetName(command_context)
#     router.routes[Commands.ClientId] = ClientId(command_context)
#     router.routes[Commands.ClientList] = ClientList(command_context)
#
#     return router
#
#
# def create_config_router(command_context: ClientContext):
#     router = RedisCommandsRouter(command_context, parent_command=Commands.Config)
#     # Acl
#     router.routes[Commands.ConfigGet] = ConfigGet(command_context)
#     router.routes[Commands.ConfigSet] = ConfigSet(command_context)
#
#     return router
