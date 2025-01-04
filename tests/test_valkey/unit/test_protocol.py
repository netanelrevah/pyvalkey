from datetime import datetime, timedelta
from socket import socket

from parametrization import Parametrization


def test_handle_an_empty_query(connection: socket):
    connection.send(b"\r\n")

    connection.send(b"*1\r\n$4\r\nPING\r\n")
    a = connection.recv(100)
    assert a == b"$4\r\nPONG\r\n"


def test_negative_multibulk_length(connection: socket):
    connection.send(b"*-10\r\n")

    connection.send(b"*1\r\n$4\r\nPING\r\n")
    a = connection.recv(100)
    assert a == b"$4\r\nPONG\r\n"


def test_out_of_range_multibulk_length(connection: socket):
    connection.send(b"*3000000000\r\n")

    a = connection.recv(100)
    assert a == b"-invalid multibulk length\r\n"


def test_wrong_multibulk_payload_header(connection: socket):
    connection.send(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n")

    a = connection.recv(100)
    assert a == b"-expected '$', got 'f'\r\n"


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
            assert False, "Valkey did not closed connection after protocol desync"
