from dataclasses import dataclass, field

from r3dis.commands.context import ClientContext
from r3dis.commands.core import Command
from r3dis.commands.parsers import redis_command
from r3dis.consts import Commands
from r3dis.errors import RouterKeyError


@dataclass
class RedisCommandsRouter:
    routes: dict = field(default_factory=dict)
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

    def child(self, command: Commands) -> "RedisCommandsRouter":
        if command not in self.routes:
            self.routes[command] = RedisCommandsRouter()
        if not isinstance(command, RedisCommandsRouter):
            raise TypeError()
        return self.routes[command]
