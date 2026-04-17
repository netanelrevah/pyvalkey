import socket


class ValkeyError(Exception):
    pass


def _encode_command(*args) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        if isinstance(arg, int):
            arg = str(arg).encode()
        elif isinstance(arg, str):
            arg = arg.encode()
        parts.append(f"${len(arg)}\r\n".encode())
        parts.append(arg)
        parts.append(b"\r\n")
    return b"".join(parts)


class _SocketReader:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._buffer = b""

    def _fill(self):
        data = self._sock.recv(65536)
        if not data:
            raise ConnectionError("Server closed connection")
        self._buffer += data

    def readline(self) -> bytes:
        while b"\r\n" not in self._buffer:
            self._fill()
        idx = self._buffer.index(b"\r\n")
        line, self._buffer = self._buffer[:idx], self._buffer[idx + 2 :]
        return line

    def read(self, n: int) -> bytes:
        while len(self._buffer) < n + 2:
            self._fill()
        data, self._buffer = self._buffer[:n], self._buffer[n + 2 :]
        return data


def _read_response(reader: _SocketReader):
    line = reader.readline()
    prefix, payload = line[0:1], line[1:]

    if prefix == b"+":
        return payload

    if prefix == b"-":
        raise ValkeyError(payload.decode())

    if prefix == b":":
        return int(payload)

    if prefix == b"$":
        length = int(payload)
        if length == -1:
            return None
        return reader.read(length)

    if prefix == b"*":
        count = int(payload)
        if count == -1:
            return None
        return [_read_response(reader) for _ in range(count)]

    raise ValkeyError(f"Unknown RESP prefix: {prefix!r}")


class DeferredCommand:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._reader = _SocketReader(sock)

    def read(self):
        return _read_response(self._reader)


class ValkeyTestClient:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._reader = _SocketReader(sock)

    def run(self, *args):
        self._sock.sendall(_encode_command(*args))
        return _read_response(self._reader)

    def error(self, *args) -> str:
        try:
            self.run(*args)
            raise AssertionError(f"Expected error but got success for: {args}")
        except ValkeyError as e:
            return str(e)

    @staticmethod
    def error_from(deferred: "DeferredCommand") -> str:
        try:
            deferred.read()
            raise AssertionError("Expected error but got success")
        except ValkeyError as e:
            return str(e)

    def send(self, *args) -> DeferredCommand:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer = self._sock.getpeername()
        conn.connect(peer)
        conn.settimeout(5)
        conn.sendall(_encode_command("select", 9))
        select_reader = _SocketReader(conn)
        _read_response(select_reader)
        conn.sendall(_encode_command(*args))
        deferred = DeferredCommand(conn)
        deferred._reader = select_reader
        return deferred
