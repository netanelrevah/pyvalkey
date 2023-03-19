import fnmatch
from io import BytesIO
from socketserver import StreamRequestHandler, TCPServer


class RedisHandler(StreamRequestHandler):
    def handle_array(self, length: int):
        array = [None] * length
        for i in range(length):
            array[i] = self.read_generic()
        return array

    def read_generic(self):
        command = self.rfile.readline().strip()
        match command[0:1], command[1:]:
            case b"*", length:
                return self.handle_array(int(length))
            case b"$", _:
                return self.rfile.readline().strip()

    def handle(self):
        while True:
            writer = BytesIO()

            command = self.read_generic()
            print(command)

            match command:
                case b"SET", name, value:
                    self.server.server_data[name] = value
                    writer.write(b"+OK\r\n")
                case b"GET", name:
                    value = self.server.server_data.get(name)
                    if value is None:
                        writer.write(b"+")
                    else:
                        writer.write(b"+" + value)
                    writer.write(b"\r\n")
                case b"HSET", name, *key_values:
                    number_of_items = len(key_values) // 2
                    h = {key_values[i * 2]: key_values[i * 2 + 1] for i in range(number_of_items)}
                    if name not in self.server.server_data:
                        self.server.server_data[name] = {}
                    self.server.server_data[name].update(h)
                    writer.write(f":{number_of_items}\r\n".encode())
                case b"HGETALL", name:
                    h = self.server.server_data.get(name, {})
                    writer.write(f"*{len(h) * 2}\r\n".encode())
                    for k, v in h.items():
                        writer.write(f"${len(k)}\r\n{k.decode()}\r\n".encode())
                        writer.write(f"${len(v)}\r\n{v.decode()}\r\n".encode())
                case b"DEL", *names:
                    deleted = len([1 for _ in filter(None, [self.server.server_data.pop(name) for name in names])])

                    writer.write(f":{deleted}\r\n".encode())
                case b"KEYS", pattern:
                    keys = fnmatch.filter(self.server.server_data.keys(), pattern)
                    writer.write(f"*{len(keys)}\r\n".encode())
                    for key in keys:
                        writer.write(f"${len(key)}\r\n{key.decode()}\r\n".encode())
                case _:
                    print(self.server.server_data)
                    return

            self.wfile.write(writer.getvalue())


class RedisServer(TCPServer):
    def __init__(self, server_address, bind_and_activate=True):
        super().__init__(server_address, RedisHandler, bind_and_activate)
        self.server_data = {}


if __name__ == "__main__":
    with RedisServer(("127.0.0.1", 6379), RedisHandler) as server:
        server.serve_forever()
