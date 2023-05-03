from dataclasses import dataclass

from r3dis.commands.core import CommandHandler
from r3dis.configurations import Configurations
from r3dis.errors import RedisSyntaxError, RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK


@dataclass
class ConfigGet(CommandHandler):
    def handle(self, parameters: list[bytes]):
        names = self.configurations.get_names(*parameters)
        return self.configurations.info(names)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        return parameters


@dataclass
class ConfigSet(CommandHandler):
    def handle(self, parameters_dict: dict[bytes, bytes]):
        for name, values in parameters_dict.items():
            self.configurations.set_values(name, *values)
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) == 0:
            raise RedisWrongNumberOfArguments()

        parameters_dict = {}
        while parameters:
            name = parameters.pop(0)
            number_of_values = Configurations.get_number_of_values(name)
            if number_of_values <= 0:
                return RedisSyntaxError()
            parameters_dict[name] = [parameters.pop(0) for _ in range(number_of_values)]

        return parameters_dict
