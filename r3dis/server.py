import fnmatch
from socketserver import StreamRequestHandler, TCPServer

from r3dis.resp import dump, load


class RedisHandler(StreamRequestHandler):
    def __init__(self, request, client_address, server: "RedisServer"):
        super().__init__(request, client_address, server)
        self.server: RedisServer = server

    @property
    def data(self) -> dict:
        return self.server.server_data

    def handle(self):
        while True:
            command = load(self.rfile)
            print(command)

            match command:
                case b"SET", name, value:
                    self.data[name] = value
                    dump("OK", self.wfile)
                case b"GET", name:
                    value = self.data.get(name, "")
                    dump(value, self.wfile)
                case b"HSET", name, *key_values:
                    number_of_items = len(key_values) // 2
                    h = {key_values[i * 2]: key_values[i * 2 + 1] for i in range(number_of_items)}
                    if name not in self.data:
                        self.data[name] = {}
                    self.data[name].update(h)
                    dump(number_of_items, self.wfile)
                case b"HGETALL", name:
                    h = self.data.get(name, {})
                    response = []
                    for k, v in h.items():
                        response.extend([k, v])
                    dump(response, self.wfile)
                case b"DEL", *names:
                    deleted = len([1 for _ in filter(None, [self.data.pop(name) for name in names])])
                    dump(deleted, self.wfile)
                case b"KEYS", pattern:
                    keys = list(fnmatch.filter(self.data.keys(), pattern))
                    dump(keys, self.wfile)
                case b"APPEND", name, value:
                    if name not in self.data:
                        self.data[name] = ""
                    self.data[name] += value
                    dump(len(self.data[name]), self.wfile)
                case _:
                    print(self.data)
                    return


class RedisServer(TCPServer):
    def __init__(self, server_address, bind_and_activate=True):
        super().__init__(server_address, RedisHandler, bind_and_activate)
        self.server_data = {}
