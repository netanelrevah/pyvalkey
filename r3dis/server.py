import fnmatch
import itertools
import logging
import operator
import time
from collections import defaultdict
from dataclasses import dataclass, field
from functools import reduce
from hashlib import sha256
from io import BytesIO
from os import urandom
from socket import socket
from socketserver import StreamRequestHandler, ThreadingTCPServer
from typing import Any

from sortedcontainers import SortedSet

from r3dis.acl import ACL
from r3dis.clients import Client, ClientList
from r3dis.configurations import Configurations
from r3dis.information import Information
from r3dis.resp import RESP_OK, RespError, dump, load
from r3dis.utils import chunks, flatten

logger = logging.getLogger(__name__)


class RedisWrongType(Exception):
    pass


@dataclass
class RedisString:
    bytes_value: bytes = b""
    int_value: int = 0

    @classmethod
    def int_to_bytes(cls, value: int) -> bytes:
        return value.to_bytes(length=(8 + (value + (value < 0)).bit_length()) // 8, byteorder="big", signed=True)

    @classmethod
    def int_from_bytes(cls, value: bytes) -> int:
        return int.from_bytes(value, byteorder="big", signed=True)

    def update(self, value: int | bytes):
        if isinstance(value, int):
            self.int_value = value
            self.bytes_value = self.int_to_bytes(value)
        elif isinstance(value, bytes):
            self.int_value = self.int_from_bytes(value)
            self.bytes_value = value

    def __len__(self):
        return len(self.bytes_value)


@dataclass
class RedisSortedSet:
    members_scores: dict[bytes, float] = field(default_factory=dict)
    members: SortedSet = field(default_factory=SortedSet)

    def add(self, score: float, member: bytes):
        if member in self.members_scores:
            old_score = self.members_scores[member]
            self.members.remove((old_score, member))
        self.members_scores[member] = score
        self.members.add((score, member))

    def range(self, redis_start: int, redis_stop: int, with_scores=False, is_reversed=False):
        if redis_stop >= 0:
            stop = min(len(self.members), redis_stop)
        else:
            stop = max(len(self.members) + int(redis_stop) + 1, 0)

        if redis_start >= 0:
            start = min(len(self.members), redis_start)
        else:
            start = max(len(self.members) + int(redis_start) + 1, 0)

        result = self.members[start : stop + 1]
        if is_reversed:
            result.reverse()

        if with_scores:
            return list(flatten(result, reverse_sub_lists=True))
        return [member for score, member in result]

    def __len__(self):
        return len(self.members)


class Database(dict[bytes, Any]):
    def get_string(self, key, default=None):
        s = default
        if key in self:
            s = self[key]
        if not isinstance(s, RedisString):
            raise RedisWrongType()
        return s

    def get_or_create_string(self, key):
        if key not in self:
            self[key] = RedisString()
        s = self[key]
        if not isinstance(s, RedisString):
            raise RedisWrongType()
        return s

    def get_hash_table(self, key):
        h = {}
        if key in self:
            h = self[key]
        if not isinstance(h, dict):
            raise RedisWrongType()
        return h

    def get_or_create_hash_table(self, key):
        if key not in self:
            self[key] = {}
        h = self[key]
        if not isinstance(h, dict):
            raise RedisWrongType()
        return h

    def get_list(self, key):
        l = []
        if key in self:
            l = self[key]
        if not isinstance(l, list):
            raise RedisWrongType()
        return l

    def get_or_create_list(self, key):
        if key not in self:
            self[key] = []
        l = self[key]
        if not isinstance(l, list):
            raise RedisWrongType()
        return l

    def get_sorted_set(self, key):
        l = RedisSortedSet()
        if key in self:
            l = self[key]
        if not isinstance(l, RedisSortedSet):
            raise RedisWrongType()
        return l

    def get_or_create_sorted_set(self, key):
        if key not in self:
            self[key] = RedisSortedSet()
        l = self[key]
        if not isinstance(l, RedisSortedSet):
            raise RedisWrongType()
        return l


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

    pause_timeout: float = 0

    def handle_bit_operation(self, *command: bytes):
        operation, *parameters = command
        match operation.upper(), *parameters:
            case [(b"AND", b"OR", b"XOR") as operation, destination, *source_keys]:
                result = reduce(
                    self.OPERATION_TO_OPERATOR[operation],
                    (
                        self.database.get_string(source_key, default=RedisString()).int_value
                        for source_key in source_keys
                    ),
                )
                s = self.database.get_or_create_string(destination)
                s.update(result)
                return len(s)
            case [b"NOT", destination, source_key]:
                source_s = self.database.get_string(source_key, default=RedisString())
                destination_s = self.database.get_or_create_string(destination)
                destination_s.update(~source_s.int_value)
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

    def handle_command(self, *command: bytes):
        match command:
            case [b"CONFIG", *sub_command]:
                return self.handle_config_command(*sub_command)
            case [b"ACL", *sub_command]:
                return self.handle_acl_command(*sub_command)
            case [b"CLIENT", *sub_command]:
                return self.handle_client_command(*sub_command)
            case [b"LRANGE", key, start, stop]:
                l = self.database.get_list(key)

                stop = int(stop)
                start = int(start)

                if stop >= 0:
                    stop = min(len(l), stop)
                else:
                    stop = max(len(l) + int(stop) + 1, 0)

                if start >= 0:
                    start = min(len(l), start)
                else:
                    start = max(len(l) + int(start) + 1, 0)

                return l[start : stop + 1]
            case [b"LPUSH", key, *value]:
                l = self.database.get_or_create_list(key)

                for v in value:
                    l.insert(0, v)
                return len(l)
            case [b"LPOP", key]:
                l = self.database.get_or_create_list(key)

                if not l:
                    return None
                return l.pop(0)
            case [b"LPOP", key, count]:
                l = self.database.get_or_create_list(key)

                if not l:
                    return None
                return [l.pop() for _ in min(count, len(l))]
            case [b"LREM", key, count, element]:
                l = self.database.get_or_create_list(key)

                if not l:
                    return 0
                count = int(count)
                to_delete = abs(count)
                if count < 0:
                    l.reverse()

                deleted = 0
                for _ in range(to_delete if to_delete > 0 else l.count(element)):
                    try:
                        l.remove(element)
                        deleted += 1
                    except ValueError:
                        break
                if count < 0:
                    l.reverse()
                return deleted
            case [b"RPUSH", key, *value]:
                l = self.database.get_or_create_list(key)

                l.extend(value)
                return len(l)
            case [b"LLEN", key]:
                l = self.database.get_list(key)

                return len(l)
            case [b"LINDEX", key, index]:
                l = self.database.get_list(key)

                try:
                    return l[int(index)]
                except IndexError:
                    return None
            case [b"LINSERT", key, direction, pivot, element]:
                if direction.upper() == b"BEFORE":
                    offset = 0
                elif direction.upper() == b"AFTER":
                    offset = 1
                else:
                    return RespError(b"ERR syntax error")
                l = self.database.get_list(key)

                if not l:
                    return 0
                try:
                    index = l.index(pivot)
                except ValueError:
                    return -1
                l.insert(index + offset, element)
                return len(l)
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
                return RESP_OK
            case b"SET", key, value:
                s = self.database.get_or_create_string(key)
                s.update(value)
                return RESP_OK
            case b"GET", key:
                return self.database.get_string(key).bytes_value
            case b"HGET", key, field:
                h = self.database.get_hash_table(key)

                return h.get(field)
            case b"HVALS", key:
                h = self.database.get_hash_table(key)

                return list(h.values())
            case b"HSTRLEN", key, field:
                h = self.database.get_hash_table(key)

                return len(h.get(field, ""))
            case b"HDEL", key, *fields:
                h = self.database.get_hash_table(key)

                return sum([1 if h.pop(f, None) is not None else 0 for f in fields])
            case b"HSET", key, *fields_values:
                h = self.database.get_or_create_hash_table(key)

                added_fields = 0
                for field, value in chunks(fields_values, 2):
                    if field not in h:
                        added_fields += 1
                    h[field] = value
                return added_fields
            case b"HGETALL", key:
                h = self.database.get_hash_table(key)

                response = []
                for k, v in h.items():
                    response.extend([k, v])
                return response
            case b"HEXISTS", key, field:
                h = self.database.get_hash_table(key)

                return field in h
            case b"HINCRBY", key, field, increment:
                try:
                    increment = int(increment)
                except ValueError:
                    return RespError(b"ERR value is not a valid integer")
                h = self.database.get_or_create_hash_table(key)

                if field not in h:
                    h[field] = 0
                if not isinstance(h[field], (int, float)):
                    return RespError(b"ERR hash value is not an integer")
                h[field] += increment
                return h[field]
            case b"HINCRBYFLOAT", key, field, increment:
                try:
                    increment = float(increment)
                except ValueError:
                    return RespError(b"ERR value is not a valid float")
                h = self.database.get_or_create_hash_table(key)

                if field not in h:
                    h[field] = 0
                if not isinstance(h[field], (int, float)):
                    return RespError(b"ERR hash value is not an float")
                h[field] = float(h[field]) + increment
                return h[field]
            case b"HKEYS", key:
                h = self.database.get_hash_table(key)

                return list(h.keys())
            case b"HLEN", key:
                h = self.database.get_hash_table(key)

                return len(h.keys())
            case b"HMGET", key, *field:
                h = self.database.get_hash_table(key)

                return [h.get(f, None) for f in field]
            case b"HMSET", key, *parameters:
                if len(parameters) % 2 != 0:
                    return RespError(b"ERR wrong number of arguments for command")
                h = self.database.get_or_create_hash_table(key)

                for field, value in chunks(parameters, 2):
                    h[field] = value
                return RESP_OK
            case b"DEL", *names:
                deleted = len([1 for _ in filter(None, [self.database.pop(name, None) for name in names])])
                return deleted
            case b"KEYS", pattern:
                keys = list(fnmatch.filter(self.database.keys(), pattern))
                return keys
            case b"APPEND", key, value:
                s = self.database.get_or_create_string(key)
                s.update(s.bytes_value + value)
                return len(s)
            case b"PING",:
                return "PONG"
            case b"PING", message:
                return message
            case b"DBSIZE",:
                return len(self.database.keys())
            case b"ECHO", message:
                return message
            case b"SETBIT", key, offset, value:
                s = self.database.get_or_create_string(key)

                previous_value = s.int_value

                if value == b"1":
                    s.update(s.int_value | 1 << int(offset))
                else:
                    s.update(s.int_value & ~(1 << int(offset)))
                return f"{previous_value:b}".encode()
            case b"BITCOUNT", key:
                value: int = self.database[key]
                return value.bit_count()
            case [b"BITCOUNT", key, start, end] | [b"BITCOUNT", key, start, end, b"BYTE"]:
                s = self.database.get_string(key, default=RedisString())
                value: int = s.int_value

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
                s = self.database.get_string(key, default=RedisString())
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
            case [b"ZADD", key, *scores_members]:
                z = self.database.get_or_create_sorted_set(key)
                length_before = len(z)
                for score, member in chunks(scores_members, 2):
                    z.add(float(score), member)
                return len(z) - length_before
            case [b"ZRANGE", key, start, stop, *options]:
                range_kwargs = {}
                while options:
                    option = options.pop()
                    match option:
                        case b"WITHSCORES" if "with_scores" not in range_kwargs:
                            range_kwargs["with_scores"] = True
                        case b"REV" if "is_reversed" not in range_kwargs:
                            range_kwargs["is_reversed"] = True
                        case _:
                            return RespError(b"ERR syntax error")

                z = self.database.get_or_create_sorted_set(key)

                start = int(start)
                stop = int(stop)

                return z.range(start, stop, **range_kwargs)
            case [b"ZREVRANGE", key, start, stop, *options]:
                range_kwargs = {}
                while options:
                    option = options.pop()
                    match option:
                        case b"WITHSCORES" if "with_scores" not in range_kwargs:
                            range_kwargs["with_scores"] = True
                        case _:
                            return RespError(b"ERR syntax error")

                z = self.database.get_or_create_sorted_set(key)

                start = int(start)
                stop = int(stop)

                return z.range(start, stop, is_reversed=True, **range_kwargs)
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
            except RedisWrongType:
                self.dump(RespError(b"WRONGTYPE Operation against a key holding the wrong kind of value"))
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
        self.databases: defaultdict[int, Database] = defaultdict(Database, {0: {}})
        self.acl: ACL = ACL.create()
        self.client_ids = itertools.count(0)
        self.clients: ClientList = ClientList()
        self.configurations: Configurations = Configurations()
        self.information: Information = Information()
