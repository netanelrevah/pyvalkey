import fnmatch
import operator
from dataclasses import dataclass
from functools import reduce
from hashlib import sha256

from r3dis.commands.core import CommandHandler
from r3dis.databases import RedisString
from r3dis.errors import RedisException, RedisSyntaxError, RedisWrongNumberOfArguments
from r3dis.resp import RESP_OK, RespError


@dataclass
class Information(CommandHandler):
    def handle(self):
        return self.information.all()

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if parameters:
            return RedisWrongNumberOfArguments()


@dataclass
class Authorize(CommandHandler):
    def handle(self, password: bytes, username: bytes | None = None):
        password_hash = sha256(password).hexdigest().encode()
        if username is not None:
            if username not in self.acl:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            if username == b"default" and password_hash == self.configurations.requirepass:
                return RESP_OK
            if password_hash not in self.acl[username].passwords:
                return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
            return RESP_OK

        if self.configurations.requirepass and password_hash == self.configurations.requirepass:
            return RESP_OK
        return RespError(
            b"ERR AUTH "
            b"<password> called without any password configured for the default user. "
            b"Are you sure your configuration is correct?"
        )

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if not parameters or len(parameters) > 2:
            return RedisWrongNumberOfArguments()
        if len(parameters) > 1:
            username = parameters.pop(0)
            password = parameters.pop(0)
            return password, username
        return parameters.pop(0), None


@dataclass
class FlushDatabase(CommandHandler):
    def handle(self):
        self.database.clear()
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if parameters:
            return RedisWrongNumberOfArguments()


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
class Get(CommandHandler):
    def handle(self, key: bytes):
        s = self.database.get_string_or_none(key)
        if s is None:
            return None
        return s.bytes_value

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            return RedisWrongNumberOfArguments()
        return parameters.pop(0)


@dataclass
class GetBit(CommandHandler):
    def handle(self, key: bytes, offset: int):
        s = self.database.get_or_create_string(key)

        bytes_offset = offset // 8
        byte_offset = offset - (bytes_offset * 8)

        return (s.bytes_value[bytes_offset] >> byte_offset) & 1

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) == 2:
            key = parameters.pop(0)
            offset = parameters.pop(0)
            return key, int(offset)
        return RedisWrongNumberOfArguments()


@dataclass
class SetBit(CommandHandler):
    def handle(self, key: bytes, offset: int, value: bool):
        s = self.database.get_or_create_string(key)

        offset = int(offset)
        bytes_offset = offset // 8
        byte_offset = offset - (bytes_offset * 8)

        if len(s.bytes_value) <= bytes_offset:
            s.bytes_value = s.bytes_value.ljust(bytes_offset + 1, b"\0")
        previous_value = (s.bytes_value[bytes_offset] >> byte_offset) & 1

        if value:
            new_byte = s.bytes_value[bytes_offset] | 1 << byte_offset
        else:
            new_byte = s.bytes_value[bytes_offset] & ~(1 << byte_offset)

        s.update_with_bytes_value(s.bytes_value[:bytes_offset] + bytes([new_byte]) + s.bytes_value[bytes_offset + 1 :])

        return previous_value

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) == 2:
            key = parameters.pop(0)
            offset = parameters.pop(0)
            bytes_value = parameters.pop(0)

            if bytes_value not in (b"1", b"0"):
                raise RedisSyntaxError()

            value = True if b"1" else False

            return key, int(offset), value
        return RedisWrongNumberOfArguments()


@dataclass
class BitOperation(CommandHandler):
    OPERATION_TO_OPERATOR = {
        b"AND": operator.and_,
        b"OR": operator.or_,
        b"XOR": operator.xor,
    }

    def handle(self, operation: bytes, destination_key: int, source_keys: list[bytes]):
        if operation in self.OPERATION_TO_OPERATOR:
            result = reduce(
                self.OPERATION_TO_OPERATOR[operation],
                (self.database.get_string(source_key).int_value for source_key in source_keys),
            )
            s = self.database.get_or_create_string(destination_key)
            s.update_with_int_value(result)
            return len(s)

        (source_key,) = source_keys

        source_s = self.database.get_string(source_key)
        destination_s = self.database.get_or_create_string(destination_key)
        destination_s.update_with_int_value(~source_s.int_value)
        return len(destination_s)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 3:
            operation = parameters.pop(0).upper()
            destination_key = parameters.pop(0)
            source_keys = parameters

            if operation not in (b"AND", b"OR", b"XOR", b"NOT"):
                raise RedisSyntaxError()

            return operation, destination_key, source_keys
        return RedisWrongNumberOfArguments()


@dataclass
class BitCount(CommandHandler):
    def handle_byte_mode(self, key: bytes, start: int, end: int):
        s = self.database.get_string(key)

        length = len(s.bytes_value)
        redis_start = start
        redis_stop = end

        if redis_start >= 0:
            start = min(length, redis_start)
        else:
            start = max(length + int(redis_start), 0)

        if redis_stop >= 0:
            stop = min(length, redis_stop)
        else:
            stop = max(length + int(redis_stop), 0)

        return sum(map(int.bit_count, s.bytes_value[start : stop + 1]))

    def handle_bit_mode(self, key: bytes, start: int, end: int):
        s = self.database.get_string(key)
        value: int = s.int_value

        length = value.bit_length()

        if start < 0:
            start = length + start

        if end < 0:
            end = length + (end + 1)

        bit_count = ((value & ((2**end) - 1)) >> start).bit_count()
        return bit_count

    def handle(self, key: bytes, count_range: tuple[int, int] | None = None, bit_mode: bool = False):
        if not count_range:
            s = self.database.get_string(key)
            return sum(map(int.bit_count, s.bytes_value))

        start, end = count_range

        if bit_mode:
            self.handle_bit_mode(key, start, end)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) == 1:
            return parameters.pop(0), None
        if len(parameters) == 3:
            return parameters.pop(0), (int(parameters.pop(0)), int(parameters.pop(0)))
        if len(parameters) == 4:
            key = parameters.pop(0)
            start = int(parameters.pop(0))
            end = int(parameters.pop(0))
            mode = parameters.pop(0).upper()

            if mode not in (b"BYTE", b"BIT"):
                raise RedisSyntaxError()

            return key, (start, end), mode == b"BIT"
        return RedisWrongNumberOfArguments()


@dataclass
class Set(CommandHandler):
    def handle(self, key: bytes, value: bytes):
        s = self.database.get_or_create_string(key)
        s.update_with_bytes_value(value)
        return RESP_OK

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 2:
            return RedisWrongNumberOfArguments()
        return parameters.pop(0), parameters.pop(0)


@dataclass
class Delete(CommandHandler):
    def handle(self, keys: list[bytes]):
        return len([1 for _ in filter(None, [self.database.pop(key, None) for key in keys])])

    @classmethod
    def parse(cls, parameters: list[bytes]):
        return parameters


@dataclass
class Keys(CommandHandler):
    def handle(self, pattern: bytes):
        return list(fnmatch.filter(self.database.keys(), pattern))

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 1:
            return RedisWrongNumberOfArguments()
        return parameters.pop(0)


@dataclass
class DatabaseSize(CommandHandler):
    def handle(self):
        return len(self.database.keys())

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) == 0:
            return
        return RedisWrongNumberOfArguments()


@dataclass
class Append(CommandHandler):
    def handle(self, key: bytes, value: bytes):
        s = self.database.get_or_create_string(key)
        s.update_with_bytes_value(s.bytes_value + value)
        return len(s)

    @classmethod
    def parse(cls, parameters: list[bytes]):
        if len(parameters) > 2:
            return RedisWrongNumberOfArguments()
        return parameters.pop(0), parameters.pop(0)


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


@dataclass
class IncrementBy(CommandHandler):
    default_increment: int | None = None
    increment_sign: int = 1
    float_allowed: bool = False

    def handle(self, key: bytes, increment: int | None = None):
        calculated_increment = (increment * self.increment_sign) if increment is not None else self.default_increment

        s = self.database.get_or_create_string(key)
        if s.numeric_value is None:
            raise RedisException(b"ERR value is not an integer or out of range")
        s.update_with_numeric_value(s.numeric_value + calculated_increment)
        return s.bytes_value

    def parse(self, parameters: list[bytes]):
        if self.default_increment is not None and len(parameters) == 1:
            return parameters.pop(0)
        if self.default_increment is None and len(parameters) == 2:
            key = parameters.pop(0)
            increment = parameters.pop(0)
            if self.float_allowed:
                if not RedisString.is_float(increment):
                    raise RedisException(b"ERR value is not a valid float")
                else:
                    return key, float(increment)
            if not increment.isdigit():
                raise RedisException(b"ERR value is not an integer or out of range")
            else:
                return key, int(increment)
        return RedisWrongNumberOfArguments()
