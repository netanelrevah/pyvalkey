from datetime import datetime, timedelta
from socket import socket

from parametrization import Parametrization


def test_handle_an_empty_query(connection: socket):
    connection.send(b"\r\n")
    connection.send(b"*1\r\n$4\r\nPING\r\n")
    a = connection.recv(20)

    assert a == b"$4\r\nPONG\r\n"


@Parametrization.autodetect_parameters()
@Parametrization.case(name=0, sequence=b"\x00")
@Parametrization.case(name=1, sequence=b"*\x00")
@Parametrization.case(name=2, sequence=b"$\x00")
def test_protocol_desync_regression_test(connection: socket, sequence: bytes):
    connection.send(sequence)

    payload = (b"A" * 1024) + b"\n"
    test_start = datetime.now()
    test_time_limit = timedelta(seconds=30)

    while True:
        try:
            connection.send(payload)
        except OSError:
            break

        if datetime.now() - test_start > test_time_limit:
            connection.close()
            assert False, "assertion:Valkey did not closed connection after protocol desync"
