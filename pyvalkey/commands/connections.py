from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import server_command_dependency
from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.resp import RespProtocolVersion, ValueType


@ServerCommandsRouter.command(b"hello", [b"connection", b"fast"])
class Hello(Command):
    client_context: ClientContext = server_command_dependency()
    protocol_version: RespProtocolVersion | None = positional_parameter(
        default=None, parse_error=b"NOPROTO unsupported protocol version"
    )

    def execute(self) -> ValueType:
        if self.protocol_version is not None:
            self.client_context.protocol = self.protocol_version

        return {
            b"server": b"redis",
            b"version": self.client_context.server_context.information.server_version,
            b"proto": self.client_context.protocol,
            b"id": self.client_context.current_client.client_id,
            b"mode": b"standalone",
            b"role": b"master",
            b"modules": [],
        }
