from datetime import datetime, timedelta
from socket import socket

import pytest
import valkey
from parametrization import Parametrization
from valkey import ResponseError


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
    assert a == b"-Protocol error: invalid multibulk length\r\n"


def test_wrong_multibulk_payload_header(connection: socket):
    connection.send(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n")

    a = connection.recv(100)
    assert a == b"-Protocol error: expected '$', got 'f'\r\n"


def test_negative_multibulk_payload_length(connection: socket):
    connection.send(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n")

    a = connection.recv(100)
    assert a == b"-Protocol error: invalid bulk length\r\n"


def test_multi_bulk_request_not_followed_by_bulk_arguments(connection: socket):
    connection.send(b"*1\r\nfoo\r\n")

    a = connection.recv(100)
    assert a == b"-Protocol error: expected '$', got 'f'\r\n"


def test_generic_wrong_number_of_args(s: valkey.Valkey):
    with pytest.raises(ResponseError, match="ERR wrong number of arguments for 'PING' command"):
        s.execute_command("ping", "x", "y", "z")


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
