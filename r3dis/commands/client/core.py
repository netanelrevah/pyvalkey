from dataclasses import dataclass

from r3dis.commands.handlers import CommandHandler
from r3dis.errors import RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK


@dataclass
class SelectDatabase(CommandHandler):
    def handle(self, number: int):
        self.command_context.current_database = number
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            return RedisWrongNumberOfArguments()
        return int(parameters.pop(0))


@dataclass
class Echo(CommandHandler):
    ping_mode: bool = True

    def handle(self, message: bytes | None = None):
        if message:
            return message
        return b"PONG"

    def parse(self, parameters: list[bytes]):
        if len(parameters) == 1:
            return parameters.pop(0)
        if self.ping_mode and len(parameters) == 0:
            return (None,)
        return RedisWrongNumberOfArguments()
