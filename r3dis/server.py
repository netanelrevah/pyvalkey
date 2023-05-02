import fnmatch
import itertools
import logging
import operator
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import reduce
from hashlib import sha256
from io import BytesIO
from os import urandom
from socket import socket
from socketserver import StreamRequestHandler, ThreadingTCPServer

from r3dis.acl import ACL
from r3dis.clients import Client, ClientList
from r3dis.commands.core import ClientContext
from r3dis.commands.router import Router, create_base_router
from r3dis.configurations import Configurations
from r3dis.databases import Database, RedisString
from r3dis.errors import (
    RedisInvalidIntegerError,
    RedisSyntaxError,
    RedisWrongType,
    RouterKeyError,
)
from r3dis.information import Information
from r3dis.resp import RESP_OK, RespError, dump, load

logger = logging.getLogger(__name__)


@dataclass
class RedisHandler:
    OPERATION_TO_OPERATOR = {
        b"AND": operator.and_,
        b"OR": operator.or_,
        b"XOR": operator.xor,
    }

    client: Client
    database: Database
    configurations: Configurations
    acl: ACL

    information: Information
    clients: ClientList
    databases: dict[int, Database]

    commands_router: Router

    pause_timeout: float = 0

    def handle_bit_operation(self, *command: bytes):
        operation, *parameters = command
        match operation.upper(), *parameters:
            case [(b"AND" | b"OR" | b"XOR") as operation, destination, *source_keys]:
                result = reduce(
                    self.OPERATION_TO_OPERATOR[operation],
                    (self.database.get_string(source_key).int_value for source_key in source_keys),
                )
                s = self.database.get_or_create_string(destination)
                s.update_with_int_value(result)
                return len(s)
            case [b"NOT", destination, source_key]:
                source_s = self.database.get_string(source_key)
                destination_s = self.database.get_or_create_string(destination)
                destination_s.update_with_int_value(~source_s.int_value)
                return len(destination_s)
            case _:
                return RespError(b"ERR syntax error")

    def handle_config_command(self, *command: bytes):
        match command:
            case b"SET", *parameters:
                parameters_dict = {}
                parameters_list = list(parameters)
                while parameters_list:
                    name = parameters_list.pop(0)
                    number_of_values = Configurations.get_number_of_values(name)
                    if number_of_values <= 0:
                        return RespError(b"ERR syntax error")
                    parameters_dict[name] = [parameters_list.pop(0) for _ in range(number_of_values)]
                if not parameters_dict:
                    return

                for name, values in parameters_dict.items():
                    self.configurations.set_values(name, *values)
                return RESP_OK
            case b"GET", *parameters:
                names = self.configurations.get_names(*parameters)
                return self.configurations.info(names)

    def handle_command(self, *command: bytes):
        try:
            return self.commands_router.execute(list(command))
        except RouterKeyError:
            pass

        match command:
            case [b"CONFIG", *sub_command]:
                return self.handle_config_command(*sub_command)
            case [b"INFO"]:
                return self.information.all()
            case [b"AUTH", password]:
                password_hash = sha256(password).hexdigest().encode()

                if self.configurations.requirepass and password_hash == self.configurations.requirepass:
                    return RESP_OK
                return RespError(
                    b"ERR AUTH "
                    b"<password> called without any password configured for the default user. "
                    b"Are you sure your configuration is correct?"
                )
            case b"AUTH", username, password:
                password_hash = sha256(password).hexdigest().encode()

                if username not in self.acl:
                    return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
                if username == b"default" and password_hash == self.configurations.requirepass:
                    return RESP_OK
                if password_hash not in self.acl[username].passwords:
                    return RespError(b"WRONGPASS invalid username-password pair or user is disabled.")
                return RESP_OK
            case b"FLUSHDB",:
                self.database.clear()
                return RESP_OK
            case b"SELECT", number:
                self.database = self.databases[int(number)]
                self.commands_router.command_context.current_database = int(number)
                return RESP_OK
            case [b"DEL", *names]:
                deleted = len([1 for _ in filter(None, [self.database.pop(name, None) for name in names])])
                return deleted
            case [b"GET", key]:
                s = self.database.get_string_or_none(key)
                if s is None:
                    return None
                return s.bytes_value
            case [b"SET", key, value]:
                s = self.database.get_or_create_string(key)
                s.update_with_bytes_value(value)
                return RESP_OK
            case [b"INCR", key]:
                s = self.database.get_or_create_string(key)
                if s.numeric_value is None:
                    return RespError(b"ERR value is not an integer or out of range")
                s.update_with_numeric_value(s.numeric_value + 1)
                return s.bytes_value
            case [b"DECR", key]:
                s = self.database.get_or_create_string(key)
                if s.numeric_value is None:
                    return RespError(b"ERR value is not an integer or out of range")
                s.update_with_numeric_value(s.numeric_value - 1)
                return s.bytes_value
            case [b"INCRBY", key, increment]:
                s = self.database.get_or_create_string(key)
                if s.numeric_value is None or not increment.isdigit():
                    return RespError(b"ERR value is not an integer or out of range")
                s.update_with_numeric_value(s.numeric_value + int(increment))
                return s.bytes_value
            case [b"DECRBY", key, decrement]:
                s = self.database.get_or_create_string(key)
                if s.numeric_value is None or not decrement.isdigit():
                    return RespError(b"ERR value is not an integer or out of range")
                s.update_with_numeric_value(s.numeric_value - int(decrement))
                return s.bytes_value
            case [b"INCRBYFLOAT", key, increment]:
                s = self.database.get_or_create_string(key)
                if s.numeric_value is None or not RedisString.is_float(increment):
                    return RespError(b"ERR value is not a valid float")
                s.update_with_numeric_value(s.numeric_value + float(increment))
                return s.bytes_value
            case b"KEYS", pattern:
                keys = list(fnmatch.filter(self.database.keys(), pattern))
                return keys
            case b"APPEND", key, value:
                s = self.database.get_or_create_string(key)
                s.update_with_bytes_value(s.bytes_value + value)
                return len(s)
            case b"PING",:
                return "PONG"
            case b"PING", message:
                return message
            case b"DBSIZE",:
                return len(self.database.keys())
            case b"ECHO", message:
                return message
            case b"GETBIT", key, offset:
                s = self.database.get_or_create_string(key)

                offset = int(offset)
                bytes_offset = offset // 8
                byte_offset = offset - (bytes_offset * 8)

                return (s.bytes_value[bytes_offset] >> byte_offset) & 1

            case b"SETBIT", key, offset, value:
                s = self.database.get_or_create_string(key)

                offset = int(offset)
                bytes_offset = offset // 8
                byte_offset = offset - (bytes_offset * 8)

                if len(s.bytes_value) <= bytes_offset:
                    s.bytes_value = s.bytes_value.ljust(bytes_offset + 1, b"\0")
                previous_value = (s.bytes_value[bytes_offset] >> byte_offset) & 1

                if value == b"1":
                    new_byte = s.bytes_value[bytes_offset] | 1 << byte_offset
                elif value == b"0":
                    new_byte = s.bytes_value[bytes_offset] & ~(1 << byte_offset)
                else:
                    return RespError(b"ERR syntax error")

                s.update_with_bytes_value(
                    s.bytes_value[:bytes_offset] + bytes([new_byte]) + s.bytes_value[bytes_offset + 1 :]
                )

                return previous_value
            case [b"BITCOUNT", key]:
                s = self.database.get_string(key)
                return sum(map(int.bit_count, s.bytes_value))
            case [b"BITCOUNT", key, start, end] | [b"BITCOUNT", key, start, end, b"BYTE"]:
                s = self.database.get_string(key)

                length = len(s.bytes_value)
                redis_start = int(start)
                redis_stop = int(end)

                if redis_start >= 0:
                    start = min(length, redis_start)
                else:
                    start = max(length + int(redis_start), 0)

                if redis_stop >= 0:
                    stop = min(length, redis_stop)
                else:
                    stop = max(length + int(redis_stop), 0)

                return sum(map(int.bit_count, s.bytes_value[start : stop + 1]))
            case [b"BITCOUNT", key, start, end, b"BIT"]:
                s = self.database.get_string(key)
                value: int = s.int_value

                length = value.bit_length()

                start = int(start)
                end = int(end)

                if start < 0:
                    start = length + start

                if end < 0:
                    end = length + (end + 1)

                bit_count = ((value & ((2**end) - 1)) >> start).bit_count()
                return bit_count
            case [b"BITOP", *parameters]:
                return self.handle_bit_operation(*parameters)
            case unknown, *args:
                return RespError(
                    f"ERR unknown command '{unknown}', with args beginning with: {args[0] if args else ''}".encode()
                )


class RedisConnectionHandler(StreamRequestHandler):
    def __init__(self, request, client_address, server: "RedisServer"):
        super().__init__(request, client_address, server)
        self.server: RedisServer = server
        self.request: socket

        self.current_database = self.databases[0]
        self.current_client: Client

    @property
    def configurations(self):
        return self.server.configurations

    @property
    def databases(self):
        return self.server.databases

    @property
    def clients(self):
        return self.server.clients

    @property
    def acl(self):
        return self.server.acl

    def setup(self) -> None:
        super().setup()

        client = self.clients.create_client(
            host=self.client_address[0].encode(),
            port=self.client_address[1],
        )

        self.handler = RedisHandler(
            client,
            self.databases[0],
            self.configurations,
            self.acl,
            self.server.information,
            self.clients,
            self.databases,
            create_base_router(
                ClientContext(databases=self.databases, acl=self.acl, clients=self.clients, current_client=client)
            ),
        )

    def dump(self, value):
        dumped = BytesIO()
        dump(value, dumped)
        print(self.handler.client.client_id, "result", dumped.getvalue())

        if self.handler.client.reply_mode == "skip":
            self.handler.client.reply_mode = "on"
            return

        if self.handler.client.reply_mode == "off":
            return

        dump(value, self.wfile)

    def handle(self):
        while not self.handler.client.is_killed:
            command = load(self.rfile)

            if command is None:
                break
            if command[0] == b"QUIT":
                self.dump(RESP_OK)
                break

            self.server.information.total_commands_processed += 1

            print(self.handler.client.client_id, command)

            try:
                self.dump(self.handler.handle_command(*command))
                if self.handler.pause_timeout:
                    while self.handler.client.is_paused and time.time() < self.handler.pause_timeout:
                        time.sleep(0.1)
                    self.handler.pause_timeout = 0
            except RedisWrongType:
                self.dump(RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value"))
            except RedisSyntaxError:
                self.dump(RespError(b"ERR syntax error"))
            except RedisInvalidIntegerError:
                self.dump(RespError(b"ERR hash value is not an integer"))
            except Exception as e:
                self.dump(RespError(b"ERR internal"))
                raise e

        print(self.handler.client.client_id, "exited")

    def finish(self) -> None:
        del self.clients[self.handler.client.client_id]
        super().finish()


class RedisServer(ThreadingTCPServer):
    def __init__(self, server_address, bind_and_activate=True):
        super().__init__(server_address, RedisConnectionHandler, bind_and_activate)
        self.databases: defaultdict[int, Database] = defaultdict(Database, {0: {}})
        self.acl: ACL = ACL.create()
        self.client_ids = itertools.count(0)
        self.clients: ClientList = ClientList()
        self.configurations: Configurations = Configurations()
        self.information: Information = Information()
