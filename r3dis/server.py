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

from r3dis.resp import dump, load

logger = logging.getLogger(__name__)


@dataclass
class Client:
    client_id: int
    host: bytes
    port: int
    name: bytes = b""

    is_normal: bool = True
    is_replica: bool = False

    is_killed: bool = False
    is_paused: bool = False
    reply_mode: bytes = "on"

    @property
    def address(self) -> bytes:
        return self.host + b":" + str(self.port).encode()

    @property
    def flags(self) -> bytes:
        return b"".join([b"N" if self.is_normal else b"", b"S" if self.is_replica else b""])

    @property
    def info(self):
        items = {b"id": self.client_id, b"addr": self.address, b"flags": self.flags, b"name": self.name}.items()
        return b" ".join([k + b"=" + to_bytes(v) for k, v in items])


class ClientList(dict[int, Client]):
    def all(self):
        return ClientList({id_: c for id_, c in self.items() if not c.is_killed})

    def filter_(self, client_id: int = None, address: bytes = None, client_type: bytes = None) -> "ClientList":
        filtered = ClientList()
        for c in self.all().values():
            if client_id is not None and c.client_id != client_id:
                continue
            if address is not None and c.address != address:
                continue
            if client_type is not None:
                if client_type == b"normal" and not c.is_normal:
                    continue
                if client_type == b"replica" and not c.is_replica:
                    continue
            filtered[c.client_id] = c
        return filtered

    @property
    def info(self) -> bytes:
        return b"\r".join([c.info for c in self.all().values()])


class ACLUser:
    pass


def to_bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


class RedisHandler(StreamRequestHandler):
    def __init__(self, request, client_address, server: "RedisServer"):
        super().__init__(request, client_address, server)
        self.server: RedisServer = server
        self.current_client = None
        self.request: socket

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

    def dump(self, value, bulk_string=False):
        dumped = BytesIO()
        dump(value, dumped, dump_bulk=bulk_string)
        print(self.current_client.client_id, "result", dumped.getvalue())

        if self.current_client.reply_mode == "skip":
            self.current_client.reply_mode = "on"
            return

        if self.current_client.reply_mode == "off":
            return

        dump(value, self.wfile, dump_bulk=bulk_string)

    def dump_ok(self):
        dump(b"OK", self.wfile)

    def setup(self) -> None:
        super().setup()

        current_client_id = next(self.server.client_ids)
        self.current_client = Client(
            client_id=current_client_id,
            host=self.client_address[0].encode(),
            port=self.client_address[1],
        )
        self.clients[self.current_client.client_id] = self.current_client

    def handle(self):
        current_database = self.databases[0]

        while not self.current_client.is_killed:
            command = load(self.rfile)

            print(self.current_client.client_id, command)

            match command:
                case None:
                    break
                case b"CONFIG", b"SET", *parameters:
                    if len(parameters) % 2 != 0:
                        self.dump(Exception("ERR syntax error"))
                    for name, value in zip(parameters[::2], parameters[1::2]):
                        if name == b"requirepass":
                            self.configurations[name] = sha256(value).hexdigest().encode()
                    self.dump_ok()
                case b"CONFIG", b"GET", *parameters:
                    keys = set()
                    for parameter in parameters:
                        keys.update(set(fnmatch.filter(self.configurations.keys(), parameter)))
                    self.dump(
                        {
                            config_name: config_value
                            for config_name, config_value in self.configurations.items()
                            if config_name in keys
                        }
                    )
                case b"ACL", b"HELP":
                    self.dump([b"genpass"])
                case b"ACL", b"GENPASS":
                    self.dump(urandom(64))
                case b"ACL", b"GENPASS", length:
                    self.dump(urandom(length))
                case b"ACL", b"CAT":
                    self.dump([b"read"])
                case b"ACL", b"CAT", category:
                    if category == b"read":
                        self.dump([b"get"])
                        continue
                    self.dump([])
                case b"ACL", b"DELUSER", *user_names:
                    user_deleted = 0
                    for user_name in user_names:
                        if user_name == b"default":
                            pass
                        user_deleted += 1 if self.acl.pop(user_name, None) is not None else 0
                    self.dump(user_deleted)
                case b"ACL", b"DRYRUN", user_name, command, *arg:
                    # Todo: Implement
                    self.dump_ok()
                case b"ACL", b"SETUSER", user_name, *rules:
                    if user_name not in self.acl:
                        self.acl[user_name] = {b"passwords": set(), b"flags": set()}
                    for rule in rules:
                        if rule == b"on":
                            self.acl[user_name][b"flags"] -= set(b"off")
                            self.acl[user_name][b"flags"] |= set(b"on")
                            continue
                        if rule == b"off":
                            self.acl[user_name][b"flags"] -= set(b"on")
                            self.acl[user_name][b"flags"] |= set(b"off")
                            continue
                        if rule.startswith(b">"):
                            self.acl[user_name][b"passwords"].add(sha256(rule[1:]).hexdigest().encode())
                            continue
                        if rule.startswith(b"+"):
                            # Todo: Implement
                            continue
                    self.dump_ok()
                case b"ACL", b"GETUSER", user_name:
                    if user_name not in self.acl:
                        self.dump(None)
                        continue
                    self.dump(self.acl[user_name])
                case b"LPUSH", name, value:
                    if name not in current_database:
                        current_database[name] = []
                    if not isinstance(current_database[name], list):
                        self.dump(Exception("WRONGTYPE Operation against a key holding the wrong kind of value"))
                        continue
                    current_database[name].insert(0, value)
                    self.dump(len(current_database[name]))
                case b"CLIENT", b"LIST":
                    self.dump(self.clients.info, bulk_string=True)
                case b"CLIENT", b"LIST", b"TYPE", type_:
                    self.dump(self.clients.filter_(client_type=type_).info, bulk_string=True)
                case b"CLIENT", b"ID":
                    self.dump(self.current_client.client_id)
                case b"CLIENT", b"SETNAME", name:
                    self.current_client.name = name
                    self.dump_ok()
                case b"CLIENT", b"GETNAME":
                    self.dump(self.current_client.name or None)
                case b"CLIENT", b"KILL", *filters:
                    if len(filters) == 1:
                        (addr,) = filters
                        clients = self.clients.filter_(address=addr).values()
                        if not clients:
                            self.dump(Exception("ERR No such client"))
                            continue
                        (client,) = clients
                        client.is_killed = True
                        self.dump_ok()
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
                        self.dump(len(clients))
                case b"CLIENT", b"PAUSE", timeout_seconds:
                    timeout_seconds: bytes
                    if not timeout_seconds.isdigit():
                        self.dump(Exception("ERR timeout is not an integer or out of range"))
                        continue
                    timeout_seconds: int = int(timeout_seconds)
                    self.current_client.is_paused = True
                    self.dump_ok()
                    timeout = time.time() + timeout_seconds
                    while self.current_client.is_paused and time.time() < timeout:
                        time.sleep(0.1)
                case b"CLIENT", b"UNPAUSE":
                    for client in self.clients.values():
                        client.is_paused = False
                case b"CLIENT", b"REPLY", mode:
                    if mode not in (b"ON", b"OFF", b"SKIP"):
                        self.dump(Exception("ERR syntax error"))
                    self.current_client.reply_mode = mode.decode().lower()
                    if mode == b"ON":
                        self.dump_ok()
                case b"INFO",:
                    self.dump(
                        "redis_version:4.9.0\r\n" "arch_bits:64\r\n" "cluster_enabled:0\r\n" "enterprise:0\r\n",
                        bulk_string=True,
                    )
                case b"AUTH", password:
                    if (
                        b"requirepass" in self.server.configurations
                        and sha256(password).hexdigest().encode() == self.server.configurations[b"requirepass"]
                    ):
                        self.dump_ok()
                        continue
                    self.dump(
                        Exception(
                            "ERR AUTH "
                            "<password> called without any password configured for the default user. "
                            "Are you sure your configuration is correct?"
                        )
                    )
                case b"AUTH", username, password:
                    if username not in self.acl:
                        self.dump(Exception("WRONGPASS invalid username-password pair or user is disabled."))
                        continue
                    if (
                        username == b"default"
                        and sha256(password).hexdigest().encode() == self.server.configurations[b"requirepass"]
                    ):
                        self.dump_ok()
                        continue
                    if sha256(password).hexdigest().encode() not in self.acl[username][b"passwords"]:
                        self.dump(Exception("WRONGPASS invalid username-password pair or user is disabled."))
                        continue
                    self.dump_ok()
                case b"FLUSHDB",:
                    current_database.clear()
                    self.dump_ok()
                case b"SELECT", number:
                    current_database = self.databases[int(number)]
                    self.dump_ok()
                case b"SET", name, value:
                    current_database[name] = value
                    self.dump_ok()
                case b"GET", name:
                    value = current_database.get(name, None)
                    if value is None:
                        value = ""
                    if not isinstance(value, bytes):
                        self.dump(Exception("WRONGTYPE Operation against a key holding the wrong kind of value"))
                        continue
                    self.dump(value)
                case b"HSET", name, *key_values:
                    number_of_items = len(key_values) // 2
                    h = {key_values[i * 2]: key_values[i * 2 + 1] for i in range(number_of_items)}
                    if name not in current_database:
                        current_database[name] = {}
                    current_database[name].update(h)
                    self.dump(number_of_items)
                case b"HGETALL", name:
                    h = current_database.get(name, {})
                    response = []
                    for k, v in h.items():
                        response.extend([k, v])
                    self.dump(response)
                case b"DEL", *names:
                    deleted = len([1 for _ in filter(None, [current_database.pop(name) for name in names])])
                    self.dump(deleted)
                case b"KEYS", pattern:
                    keys = list(fnmatch.filter(current_database.keys(), pattern))
                    self.dump(keys)
                case b"APPEND", name, value:
                    if name not in current_database:
                        current_database[name] = ""
                    current_database[name] += value
                    self.dump(len(current_database[name]))
                case b"PING",:
                    self.dump("PONG")
                    break
                case b"PING", message:
                    self.dump(message, bulk_string=True)
                    break
                case b"QUIT",:
                    self.dump_ok()
                    break
                case b"DBSIZE",:
                    self.dump(len(current_database.keys()))
                case b"ECHO", message:
                    self.dump(message, bulk_string=True)
                case unknown, *args:
                    print("exception")

                    self.dump(
                        Exception(
                            f"ERR unknown command '{unknown}', with args beginning with: {args[0] if args else ''}"
                        )
                    )

        print(self.current_client.client_id, "exited")

    def finish(self) -> None:
        del self.clients[self.current_client.client_id]
        super().finish()


class RedisServer(ThreadingTCPServer):
    def __init__(self, server_address, bind_and_activate=True):
        super().__init__(server_address, RedisHandler, bind_and_activate)
        self.databases: defaultdict[int, dict] = defaultdict(dict, {0: {}})
        self.acl: dict[bytes, dict] = {b"default": {b"passwords": [], b"flags": []}}
        self.client_ids = count(0)
        self.clients: ClientList = ClientList()
        self.configurations: dict[bytes, bytes] = {}
