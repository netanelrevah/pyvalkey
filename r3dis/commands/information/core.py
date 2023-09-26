from dataclasses import dataclass

from r3dis.commands.handlers import CommandHandler
from r3dis.errors import RedisWrongNumberOfArguments


@dataclass
class Information(CommandHandler):
    def handle(self):
        return self.information.all()

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if parameters:
            return RedisWrongNumberOfArguments()
