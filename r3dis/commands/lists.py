from dataclasses import dataclass

from r3dis.commands.handlers import CommandHandler
from r3dis.commands.utils import parse_range_parameters
from r3dis.errors import RedisSyntaxError


@dataclass
class ListLength(CommandHandler):
    def handle(self, key: bytes):
        return len(self.database.get_list(key))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)

        return (key,)


@dataclass
class ListIndex(CommandHandler):
    def handle(self, key: bytes, index: int):
        redis_list = self.database.get_list(key)

        try:
            return redis_list[index]
        except IndexError:
            return None

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        index = parameters.pop(0)

        return key, int(index)


@dataclass
class ListRange(CommandHandler):
    def handle(self, key: bytes, range_slice: slice):
        return self.database.get_list(key)[range_slice]

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        start = parameters.pop(0)
        stop = parameters.pop(0)

        range_slice = parse_range_parameters(int(start), int(stop))

        return key, range_slice


@dataclass
class ListInsert(CommandHandler):
    def handle(self, key: bytes, offset: int, pivot: bytes, element: bytes):
        a_list = self.database.get_or_create_list(key)

        if not a_list:
            return 0
        try:
            index = a_list.index(pivot)
        except ValueError:
            return -1
        a_list.insert(index + offset, element)
        return len(a_list)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        direction = parameters.pop(0)
        if direction.upper() == b"BEFORE":
            offset = 0
        elif direction.upper() == b"AFTER":
            offset = 1
        else:
            raise RedisSyntaxError()
        pivot = parameters.pop(0)
        element = parameters.pop(0)

        return key, offset, pivot, element


@dataclass
class ListPush(CommandHandler):
    at_tail: bool = False

    def handle(self, key: bytes, values: list[bytes]):
        a_list = self.database.get_or_create_list(key)

        for v in values:
            if self.at_tail:
                a_list.append(v)
            else:
                a_list.insert(0, v)
        return len(a_list)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)

        return key, parameters


@dataclass
class ListPop(CommandHandler):
    def handle(self, key: bytes, count: int | None = None):
        a_list = self.database.get_or_create_list(key)

        if not a_list:
            return None
        if count:
            return [a_list.pop(0) for _ in range(min(len(a_list), count))]
        return a_list.pop(0)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        if parameters:
            count = parameters.pop(0)
            return key, int(count)
        return key, None


@dataclass
class ListRemove(CommandHandler):
    def handle(self, key: bytes, count: int, element: bytes):
        a_list = self.database.get_or_create_list(key)

        if not a_list:
            return 0
        count = int(count)
        to_delete = abs(count)
        if count < 0:
            a_list.reverse()

        deleted = 0
        for _ in range(to_delete if to_delete > 0 else a_list.count(element)):
            try:
                a_list.remove(element)
                deleted += 1
            except ValueError:
                break
        if count < 0:
            a_list.reverse()
        return deleted

    @classmethod
    def parse(cls, parameters: list[bytes]):
        key = parameters.pop(0)
        count = parameters.pop(0)
        element = parameters.pop(0)

        return key, int(count), element
