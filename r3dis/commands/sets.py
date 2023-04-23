import functools
from dataclasses import dataclass
from typing import Callable

from r3dis.commands.core import CommandHandler


@dataclass
class SetMove(CommandHandler):
    def handle(self, source: bytes, destination: bytes, member: bytes):
        source_set = self.database.get_set(source)
        destination_set = self.database.get_or_create_set(destination)
        if member not in source_set:
            return False
        source_set.remove(member)
        destination_set.add(member)
        return True

    @classmethod
    def parse(cls, parameters: list[bytes]):
        source = parameters.pop(0)
        destination = parameters.pop(0)
        member = parameters.pop(0)
        return source, destination, member


@dataclass
class SetIsMembers(CommandHandler):
    def handle(self, key: bytes, members: list[bytes]):
        a_set = self.database.get_set(key)
        return list(map(lambda m: m in a_set, members))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        return key, parameters


@dataclass
class SetIsMember(CommandHandler):
    def handle(self, key: bytes, member: bytes):
        return member in self.database.get_set(key)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        member = parameters.pop(0)
        return key, member


@dataclass
class SetMembers(CommandHandler):
    def handle(self, key: bytes):
        return list(self.database.get_set(key))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        return (key,)


@dataclass
class SetCardinality(CommandHandler):
    def handle(self, key: bytes):
        return len(self.database.get_set(key))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        return (key,)


@dataclass
class SetAdd(CommandHandler):
    def handle(self, key: bytes, members: list[bytes]):
        a_set = self.database.get_or_create_set(key)
        length_before = len(a_set)
        for member in members:
            a_set.add(member)
        return len(a_set) - length_before

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        return key, parameters


@dataclass
class SetPop(CommandHandler):
    def handle(self, key: bytes, count: int | None = None):
        a_set = self.database.get_set(key).pop()
        if count is None:
            return a_set.pop() if a_set else None
        return [a_set.pop() for _ in range(min(len(a_set), count))]

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        try:
            count = int(parameters.pop(0))
        except KeyError:
            count = None
        return key, count


@dataclass
class SetRemove(CommandHandler):
    def handle(self, key: bytes, members: set[bytes]):
        a_set = self.database.get_set(key)

        self.database[key] = a_set - members

        return len(a_set.intersection(members))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        return key, set(parameters)


@dataclass
class SetOperation(CommandHandler):
    operation: Callable[[set], set]

    def handle(self, keys: list[bytes]):
        return list(functools.reduce(self.operation, map(self.database.get_set, keys)))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        return (parameters,)


@dataclass
class SetStoreOperation(CommandHandler):
    operation: Callable[[set], set]

    def handle(self, destination, keys: list[bytes]):
        self.database[destination] = functools.reduce(self.operation, map(self.database.get_set, keys))
        return len(self.database[destination])

    @classmethod
    def parse(cls, parameters: list[bytes]):
        return parameters.pop(0), parameters
