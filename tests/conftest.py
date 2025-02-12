import socket
import time
from random import randrange
from threading import Thread
from typing import ClassVar

import valkey
from pytest import fixture

from pyvalkey.server import ValkeyServer


def pytest_addoption(parser):
    parser.addoption("--external", action="store_true")


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.external
    if "external" in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("external", [option_value], scope="session")


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
    server = None
    port = 6379
    if not external:
        port = next_free_port()
        server = ValkeyServer("127.0.0.1", port)
        t = Thread(target=server.run)
        t.start()

        time.sleep(0.1)

    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.connect(("localhost", port))
    connection.settimeout(3)
    try:
        yield connection
    finally:
        connection.close()

    if server:
        server.shutdown()


class ValkeyPool:
    _pool: ClassVar[list] = []

    @classmethod
    def fill_pool(cls, count):
        for _ in range(count):
            port = next_free_port()
            server = ValkeyServer("127.0.0.1", port)
            t = Thread(target=server.run)
            t.start()

            c = valkey.Valkey(port=port, db=9)

            cls._pool.append({"server": server, "thread": t, "client": c})

    @classmethod
    def drain_pool(cls):
        while cls._pool:
            server = cls._pool.pop(0)
            server["client"].close()
            server["server"].shutdown()

    @classmethod
    def get_server(cls):
        return cls._pool.pop(0)

    @classmethod
    def return_server(cls, server: ValkeyServer, thread, client):
        server.context.reset()
        cls._pool.append({"server": server, "thread": thread, "client": client})


@fixture(scope="session")
def valkey_pool(external):
    if external:
        yield
        return
    ValkeyPool.fill_pool(5)
    yield
    ValkeyPool.drain_pool()


@fixture()
def s(external, valkey_pool):
    if external:
        c = valkey.Valkey(db=9)
        yield c
        try:
            c.flushall()
        finally:
            c.close()
        return

    server = ValkeyPool.get_server()

    try:
        yield server["client"]
    finally:
        ValkeyPool.return_server(**server)


@fixture
def c(s: valkey.Valkey):
    port = s.connection_pool.connection_kwargs["port"]
    c = valkey.Valkey(port=port, db=9)
    yield c
    c.close()
