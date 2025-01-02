import socket
import time
from random import randrange
from threading import Thread

import valkey
from pytest import fixture

from pyvalkey.server import ValkeyServer


def pytest_addoption(parser):
    parser.addoption("--external", action="store_true")


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.external
    if "external" in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("external", [option_value])


def next_free_port(min_port=57343, max_port=65535):
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


@fixture()
def connection(external):
    if external:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect(("localhost", 6379))
        try:
            yield connection
        finally:
            connection.close()
        return

    port = next_free_port()
    server = ValkeyServer(("127.0.0.1", port))
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()

    time.sleep(1)
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect(("localhost", port))
    yield connection
    connection.close()
    server.shutdown()


@fixture()
def s(external):
    if external:
        c = valkey.Valkey(db=9)
        yield c
        try:
            c.flushall()
        finally:
            c.close()
        return

    port = next_free_port()
    server = ValkeyServer(("127.0.0.1", port))
    t = Thread(target=server.serve_forever, daemon=True)
    t.start()

    time.sleep(1)
    c = valkey.Valkey(port=port, db=9)
    yield c
    c.close()
    server.shutdown()


@fixture
def c(s: valkey.Valkey):
    port = s.connection_pool.connection_kwargs["port"]
    c = valkey.Valkey(port=port, db=9)
    yield c
    c.close()
