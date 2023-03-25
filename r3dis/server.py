import fnmatch
import logging
from collections import defaultdict
from io import BytesIO
from itertools import count
from os import urandom
from socket import socket
from socketserver import StreamRequestHandler, ThreadingTCPServer
from typing import Any

from r3dis.resp import dump, load

logger = logging.getLogger(__name__)


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
        self.current_client_id = None
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
        print(self.current_client_id, "result", dumped.getvalue())
        dump(value, self.wfile, dump_bulk=bulk_string)

    def dump_ok(self):
        dump(b"OK", self.wfile)

    def setup(self) -> None:
        super().setup()

        self.current_client_id = next(self.server.client_ids)
        self.clients[self.current_client_id] = {
            b"id": self.current_client_id,
            b"addr": f"{self.client_address[0]}:{self.client_address[1]}".encode(),
            b"flags": b"N",
        }

    def handle(self):
        current_database = self.databases[0]

        while self.clients[self.current_client_id]:
            command = load(self.rfile)

            print(self.current_client_id, command)

            match command:
                case None:
                    break
                case b"CONFIG", b"SET", config_name, config_value:
                    self.configurations[config_name] = config_value
                    self.dump_ok()
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
                            self.acl[user_name][b"passwords"].add(rule[1:])
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
                    results = []
                    for client in self.clients.values():
                        if client is None:
                            continue
                        results.append(b" ".join([k + b"=" + to_bytes(v) for k, v in client.items()]))
                    self.dump(b"\r".join(results), bulk_string=True)
                case b"CLIENT", b"LIST", b"TYPE", type_:
                    results = []
                    for client in self.clients.values():
                        if client is None:
                            continue
                        if type_ == b"replica" and b"S" not in client[b"flags"]:
                            continue
                        results.append(b" ".join([k + b"=" + to_bytes(v) for k, v in client.items()]))
                    self.dump(b"\r".join(results), bulk_string=True)
                case b"CLIENT", b"ID":
                    self.dump(self.current_client_id)
                case b"CLIENT", b"SETNAME", name:
                    self.clients[self.current_client_id][b"name"] = name
                    self.dump_ok()
                case b"CLIENT", b"GETNAME":
                    name = self.clients[self.current_client_id].get(b"name", b"")
                    self.dump(name or None)
                case b"CLIENT", b"KILL", *filters:
                    if len(filters) == 1:
                        (addr,) = filters
                        client_to_kill = None
                        for client, client_configurations in self.clients.items():
                            if client_configurations[b"addr"] != addr:
                                continue
                            client_to_kill = client
                            break
                        if not client_to_kill:
                            self.dump(Exception("ERR No such client"))
                            continue
                        self.clients[client_to_kill] = None
                        self.dump_ok()
                    else:
                        filters_dict = {}
                        for filter_ in zip(filters[::2], filters[1::2]):
                            match filter_:
                                case b"ID", id_:
                                    filters_dict[b"id"] = int(id_)
                                case b"ADDR", addr:
                                    filters_dict[b"addr"] = addr

                        killed_clients = 0
                        for client, client_configurations in self.clients.items():
                            for filter_key, filter_value in filters_dict.items():
                                if client_configurations[filter_key] != filter_value:
                                    break
                            else:
                                self.clients[client] = None
                                killed_clients += 1
                        self.dump(killed_clients)

                case b"INFO",:
                    self.dump(
                        "redis_version:4.9.0\r\n" "arch_bits:64\r\n" "cluster_enabled:0\r\n" "enterprise:0\r\n",
                        bulk_string=True,
                    )
                case b"AUTH", password:
                    if (
                        b"requirepass" in self.server.configurations
                        and password == self.server.configurations[b"requirepass"]
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
                    if username == b"default" and password == self.server.configurations[b"requirepass"]:
                        self.dump_ok()
                        continue
                    if password not in self.acl[username][b"passwords"]:
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
                case b"PING":
                    self.dump("PONG")
                    break
                case b"PING", message:
                    self.dump(message, bulk_string=True)
                    break
                case b"QUIT":
                    self.dump_ok()
                    break
                case unknown, *args:
                    print("exception")

                    self.dump(
                        Exception(
                            f"ERR unknown command '{unknown}', with args beginning with: {args[0] if args else ''}"
                        )
                    )

        print(self.current_client_id, "exited")

    def finish(self) -> None:
        del self.clients[self.current_client_id]
        super().finish()


class RedisServer(ThreadingTCPServer):
    def __init__(self, server_address, bind_and_activate=True):
        super().__init__(server_address, RedisHandler, bind_and_activate)
        self.databases: defaultdict[int, dict] = defaultdict(dict, {0: {}})
        self.acl: dict[bytes, dict] = {b"default": {b"passwords": [], b"flags": []}}
        self.client_ids = count(0)
        self.clients: dict[int, None | dict[bytes, Any]] = {}
        self.configurations: dict[bytes, bytes] = {}
