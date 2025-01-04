from __future__ import annotations

from dataclasses import dataclass
from io import IOBase
from typing import AnyStr, BinaryIO


class RespFatalError(Exception):
    pass


class RespSyntaxError(Exception):
    pass


class RespSimpleString(bytes):
    pass


RESP_OK = RespSimpleString(b"OK")


class RespError(bytes):
    pass


ValueType = (
    bool
    | int
    | float
    | RespSimpleString
    | RespError
    | str
    | bytes
    | list["ValueType"]
    | set["ValueType"]
    | dict["ValueType", "ValueType"]
)


@dataclass
class RespQueryLoader:
    reader: BinaryIO | IOBase

    def read_line(self) -> bytes:
        line = self.reader.readline().strip(b"\r\n")
        if b"\x00" in line:
            raise RespFatalError()
        return line

    def load_array(self, length_bytes: bytes) -> list[bytes]:
        try:
            length = int(length_bytes)
        except ValueError:
            raise RespSyntaxError()
        if length <= 0:
            raise RespSyntaxError()
        if length > 1024 * 1024:
            raise RespSyntaxError(b"invalid multibulk length")

        array: list[bytes] = [b""] * length
        for i in range(length):
            array[i] = self.load_bulk_string()
        return array

    def load_bulk_string(self) -> bytes:
        line = self.read_line()
        if line[0:1] != b"$":
            raise RespSyntaxError(f"expected '$', got '{line[0:1].decode()}'".encode())

        length_bytes = line[1:]
        try:
            length = int(length_bytes)
        except ValueError:
            raise RespSyntaxError()

        bulk_string = self.reader.read(int(length) + 2)[:-2]
        if len(bulk_string) != int(length):
            raise ValueError()
        return bulk_string

    def load_dynamic_array(self) -> list[bytes]:
        line = self.read_line()
        if not line:
            return []
        if line[0:1] != b"*":
            raise RespSyntaxError()

        return self.load_array(line[1:])


def load(stream: BinaryIO | IOBase) -> list[bytes]:
    return RespQueryLoader(stream).load_dynamic_array()


@dataclass
class RespDumper:
    writer: BinaryIO | IOBase

    def dump_bulk_string(self, value: AnyStr) -> None:
        if isinstance(value, str):
            bytes_value = value.encode()
        else:
            bytes_value = value
        self.writer.write(b"$" + str(len(bytes_value)).encode() + b"\r\n" + bytes_value + b"\r\n")

    def dump_string(self, value: AnyStr) -> None:
        if isinstance(value, str):
            bytes_value = value.encode()
        else:
            bytes_value = value
        self.writer.write(b"+" + bytes_value + b"\r\n")

    def dump_array(self, value: list) -> None:
        self.writer.write(f"*{len(value)}\r\n".encode())
        for item in value:
            self.dump(item)

    def dump(self, value: ValueType) -> None:
        if isinstance(value, bool):
            if value:
                self.dump(1)
            else:
                self.dump(0)
        elif isinstance(value, int):
            self.writer.write(f":{value}\r\n".encode())
        elif isinstance(value, float):
            self.dump_bulk_string(f"{value:g}")
        elif isinstance(value, RespSimpleString):
            self.dump_string(value)
        elif isinstance(value, RespError):
            self.writer.write(f"-{value.decode()}\r\n".encode())
        elif isinstance(value, str | bytes):
            if isinstance(value, str):
                value = value.encode()
            self.dump_bulk_string(value)
        elif isinstance(value, list):
            self.dump_array(value)
        elif isinstance(value, set):
            self.dump_array(list(value))
        elif isinstance(value, dict):
            result = []
            for k, v in value.items():
                result += [k, v]
            self.dump_array(result)
        elif value is None:
            self.writer.write(b"$-1\r\n")


def dump(value: ValueType, stream: BinaryIO | IOBase) -> None:
    RespDumper(stream).dump(value)
