import fnmatch
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from itertools import count
from os import urandom
from socket import socket
from socketserver import StreamRequestHandler, ThreadingTCPServer
from typing import Any

from r3dis.acl import ACL
from r3dis.clients import Client, ClientList
from r3dis.configurations import Configurations
from r3dis.information import Information
from r3dis.resp import RESP_OK, RespError, dump, load

logger = logging.getLogger(__name__)


@dataclass
class RedisHandler:
    client: Client
    database: dict[bytes, Any]
    configurations: Configurations
    acl: ACL

    information: Information
    clients: ClientList
    databases: dict[int, dict[bytes, Any]]

    pause_timeout: float = 0

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

    def handle_client_command(self, *command: bytes):
        match command:
            case [b"LIST"]:
                return self.clients.info
            case [b"LIST", b"TYPE", type_]:
                return self.clients.filter_(client_type=type_).info
            case [b"ID"]:
                return self.client.client_id
            case [b"SETNAME", name]:
                self.client.name = name
                return RESP_OK
            case [b"GETNAME"]:
                return self.client.name or None
            case [b"KILL", *filters]:
                if len(filters) == 1:
                    (addr,) = filters
                    clients = self.clients.filter_(address=addr).values()
                    if not clients:
                        return RespError(b"ERR No such client")
                    (client,) = clients
                    client.is_killed = True
                    return RESP_OK
                else:
                    filters_dict = {}
                    for filter_ in zip(filters[::2], filters[1::2]):
                        match filter_:
                            case b"ID", id_:
                                filters_dict["client_id"] = int(id_)
                            case b"ADDR", addr:
                                filters_dict["address"] = addr

                    clients = self.clients.filter_(**filters_dict).values()
                    for client in clients:
                        client.is_killed = True
                    return len(clients)
            case [b"PAUSE", timeout_seconds]:
                if not timeout_seconds.isdigit():
                    return RespError(b"ERR timeout is not an integer or out of range")
                self.pause_timeout: float = time.time() + int(timeout_seconds)
                self.client.is_paused = True
                return RESP_OK
            case [b"UNPAUSE"]:
                for client in self.clients.values():
                    client.is_paused = False
            case [b"REPLY", mode]:
                if mode not in (b"ON", b"OFF", b"SKIP"):
                    return RespError(b"ERR syntax error")
                self.client.reply_mode = mode.decode().lower()
                if mode == b"ON":
                    return RESP_OK

    def handle_acl_command(self, *command: bytes):
        match command:
            case [b"HELP"]:
                return [b"genpass"]
            case [b"GENPASS"]:
                return urandom(64)
            case [b"GENPASS", length]:
                return urandom(length)
            case [b"CAT"]:
                return ACL.get_categories()
            case [b"CAT", category]:
                return ACL.get_category_commands(category)
            case [b"DELUSER", *user_names]:
                user_deleted = 0
                for user_name in user_names:
                    if user_name == b"default":
                        pass
                    user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
                return user_deleted
            case [b"SETUSER", user_name, *rules]:
                acl_user = self.acl.get_or_create_user(user_name)

                for rule in rules:
                    if rule == b"on":
                        acl_user.is_active = True
                        continue
                    if rule == b"off":
                        acl_user.is_active = False
                        continue
                    if rule.startswith(b">"):
                        acl_user.add_password(rule[1:])
                        continue
                    if rule.startswith(b"+"):
                        # Todo: Implement
                        continue
                return RESP_OK
            case [b"GETUSER", user_name]:
                if user_name not in self.acl:
                    return
                return self.acl[user_name].info

    def handle_command(self, *command):
        match command:
            case [b"CONFIG", *sub_command]:
                return self.handle_config_command(*sub_command)
            case [b"ACL", *sub_command]:
                return self.handle_acl_command(*sub_command)
            case [b"CLIENT", *sub_command]:
                return self.handle_client_command(*sub_command)
            case b"LPUSH", name, value:
                if name not in self.database:
                    self.database[name] = []
                if not isinstance(self.database[name], list):
                    return RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
                self.database[name].insert(0, value)
                return len(self.database[name])

            case b"INFO",:
                return self.information.all()
            case b"AUTH", password:
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
                return RESP_OK
            case b"SET", name, value:
                self.database[name] = value
                return RESP_OK
            case b"GET", name:
                value: bytes = self.database.get(name, None)
                if value is None:
                    value = b""
                if not isinstance(value, bytes):
                    return RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value")
                return value
            case b"HSET", name, *key_values:
                number_of_items = len(key_values) // 2
                h = {key_values[i * 2]: key_values[i * 2 + 1] for i in range(number_of_items)}
                if name not in self.database:
                    self.database[name] = {}
                self.database[name].update(h)
                return number_of_items
            case b"HGETALL", name:
                h = self.database.get(name, {})
                response = []
                for k, v in h.items():
                    response.extend([k, v])
                return response
            case b"DEL", *names:
                deleted = len([1 for _ in filter(None, [self.database.pop(name, None) for name in names])])
                return deleted
            case b"KEYS", pattern:
                keys = list(fnmatch.filter(self.database.keys(), pattern))
                return keys
            case b"APPEND", name, value:
                if name not in self.database:
                    self.database[name] = ""
                self.database[name] += value
                return len(self.database[name])
            case b"PING",:
                return "PONG"
            case b"PING", message:
                return message
            case b"DBSIZE",:
                return len(self.database.keys())
            case b"ECHO", message:
                return message
            case b"SETBIT", key, offset, value:
                previous_value = 0
                if key not in self.database:
                    self.database[key] = 0
                else:
                    previous_value = self.database[key]
                if value == b"1":
                    self.database[key] |= 1 << int(offset)
                else:
                    self.database[key] &= ~(1 << int(offset))
                return f"{previous_value:b}".encode()
            case b"BITCOUNT", key:
                value: int = self.database[key]
                return value.bit_count()
            case [b"BITCOUNT", key, start, end] | [b"BITCOUNT", key, start, end, b"BYTE"]:
                value: int = self.database[key]
                length = value.bit_length()
                last_bit_start = (length // 8) * 8

                start = int(start)
                end = int(end)

                if start >= 0:
                    start = min(start * 8, last_bit_start)
                elif start < 0:
                    start = min(last_bit_start, last_bit_start + ((start + 1) * 8))

                if end >= 0:
                    end = min(length, ((end + 1) * 8) - 1)
                elif end < 0:
                    end = min(length, last_bit_start + ((end + 1) * 8) + 7)

                bit_count = ((value & ((2**end) - 1)) >> start).bit_count()
                return bit_count
            case [b"BITCOUNT", key, start, end, b"BIT"]:
                value: int = self.database[key]
                length = value.bit_length()

                start = int(start)
                end = int(end)

                if start < 0:
                    start = length + start

                if end < 0:
                    end = length + (end + 1)

                bit_count = ((value & ((2**end) - 1)) >> start).bit_count()
                return bit_count
            case [b"BITOP", b"AND", destination, *source_keys]:
                result = source_keys[0]
                for source_key in source_keys[1:]:
                    result &= source_key
                self.database[destination] = result
                return len(result)
            case [b"BITOP", b"OR", destination, *source_keys]:
                result = source_keys[0]
                for source_key in source_keys[1:]:
                    result |= source_key
                self.database[destination] = result
                return len(result)
            case [b"BITOP", b"XOR", destination, *source_keys]:
                result = source_keys[0]
                for source_key in source_keys[1:]:
                    result ^= source_key
                self.database[destination] = result
                return len(result)
            case [b"BITOP", b"NOT", destination, source_key]:
                self.database[destination] = ~self.database[source_key]
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

        current_client_id = next(self.server.client_ids)
        self.handler = RedisHandler(
            Client(
                client_id=current_client_id,
                host=self.client_address[0].encode(),
                port=self.client_address[1],
            ),
            self.databases[0],
            self.configurations,
            self.acl,
            self.server.information,
            self.clients,
            self.databases,
        )
        self.clients[current_client_id] = self.handler.client

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
            except Exception as e:
                print(e)
                self.dump(RespError(str(e).encode()))

        print(self.handler.client.client_id, "exited")

    def finish(self) -> None:
        del self.clients[self.handler.client.client_id]
        super().finish()


class RedisServer(ThreadingTCPServer):
    def __init__(self, server_address, bind_and_activate=True):
        super().__init__(server_address, RedisConnectionHandler, bind_and_activate)
        self.databases: defaultdict[int, dict] = defaultdict(dict, {0: {}})
        self.acl: ACL = ACL.create()
        self.client_ids = count(0)
        self.clients: ClientList = ClientList()
        self.configurations: Configurations = Configurations()
        self.information: Information = Information()
