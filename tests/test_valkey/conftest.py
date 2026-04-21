import socket
import time
from multiprocessing import Process
from random import randrange

from pytest import fixture

from pyvalkey.server import ValkeyServer
from tests.valkey_test_client import ValkeyTestClient, _encode_command, _read_response, _SocketReader


def _next_free_port(min_port=57343, max_port=65535):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = randrange(min_port, max_port)
    for _ in range(10):
        try:
            sock.bind(("localhost", port))
            sock.close()
            return port
        except OSError:
            port = randrange(min_port, max_port)
    raise OSError("no free ports after 10 retries")


def _run_server(host, port):
    server = ValkeyServer.create(host, port)
    server.run()


def _wait_for_server(host, port, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            sock = socket.create_connection((host, port), timeout=0.1)
            sock.close()
            return
        except OSError:
            time.sleep(0.01)
    raise TimeoutError(f"Server did not start on {host}:{port}")


def _create_client_socket(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.settimeout(5)
    sock.sendall(_encode_command("select", 9))
    _read_response(_SocketReader(sock))
    return sock


@fixture(scope="session")
def valkey_server_session(external):
    if external:
        yield {"host": "127.0.0.1", "port": 6379}
        return
    port = _next_free_port()
    host = "127.0.0.1"
    p = Process(target=_run_server, args=(host, port), daemon=True)
    p.start()
    _wait_for_server(host, port)
    yield {"host": host, "port": port}
    p.terminate()
    p.join(timeout=3)
    if p.is_alive():
        p.kill()


@fixture()
def r(valkey_server_session):
    sock = _create_client_socket(valkey_server_session["host"], valkey_server_session["port"])
    try:
        yield ValkeyTestClient(sock)
    finally:
        sock.close()


@fixture()
def rd(valkey_server_session):
    sock = _create_client_socket(valkey_server_session["host"], valkey_server_session["port"])
    try:
        yield ValkeyTestClient(sock)
    finally:
        sock.close()
