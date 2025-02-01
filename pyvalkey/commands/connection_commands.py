from dataclasses import fields

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import ParameterMetadata, positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import RespError, RespProtocolVersion, ValueType


@ServerCommandsRouter.command(b"hello", [b"connection", b"fast"])
class Hello(Command):
    client_context: ClientContext = server_command_dependency()
    protocol_version: RespProtocolVersion | None = positional_parameter(
        default=None, parse_error=b"NOPROTO unsupported protocol version"
    )

    def execute(self) -> ValueType:
        if self.protocol_version is not None:
            self.client_context.protocol = self.protocol_version

        response = {
            b"server": b"redis",
            b"version": self.client_context.server_context.information.server_version,
            b"proto": self.client_context.protocol,
            b"id": self.client_context.current_client.client_id,
            b"mode": b"standalone",
            b"role": b"master",
            b"modules": [],
        }

        if self.client_context.server_context.configurations.availability_zone != b"":
            response[b"availability_zone"] = self.client_context.server_context.configurations.availability_zone

        return response


@ServerCommandsRouter.command(b"getkeys", [b"connection", b"fast"], b"command")
class CommandGetKeys(Command):
    command: bytes = positional_parameter()
    args: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        parameters = [self.command, *self.args]
        command_cls: type[Command] = ServerCommandsRouter().internal_route(
            parameters=parameters, routes=ServerCommandsRouter.ROUTES
        )
        parsed_command = command_cls.parse(parameters)

        keys = []
        for field in fields(command_cls):
            if ParameterMetadata.KEY_MODE in field.metadata:
                if isinstance(parsed_command[field.name], list):
                    keys.extend(parsed_command[field.name])
                else:
                    keys.append(parsed_command[field.name])

        if not keys:
            return RespError(b"The command has no key arguments")

        return keys
