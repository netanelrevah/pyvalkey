from __future__ import annotations

import asyncio
from asyncio import Transport
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from io import IOBase
from typing import Any, AnyStr, BinaryIO, Self


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
    bool | int | float | RespSimpleString | RespError | str | bytes | list[Any] | set[Any] | dict[bytes, Any] | None
)


@dataclass
class BufferedLineReader:
    _buffer: bytes = b""
    _fed: asyncio.Event = field(default_factory=asyncio.Event)

    def feed(self, data: bytes) -> None:
        self._buffer += data
        self._fed.set()

    def return_line(self, line: bytes) -> None:
        self._buffer = line + b"\r\n" + self._buffer

    def __aiter__(self) -> Self:
        return self

    async def wait_for_new_line(self) -> None:
        while b"\r\n" not in self._buffer:
            await self._fed.wait()
            self._fed.clear()

    async def wait_for_enough_data(self, expected_size: int) -> None:
        while len(self._buffer) < expected_size:
            await self._fed.wait()
            self._fed.clear()

    async def __anext__(self) -> bytes:
        await self.wait_for_new_line()
        end_of_line = self._buffer.index(b"\r\n")
        line, self._buffer = self._buffer[:end_of_line], self._buffer[end_of_line + 2 :]
        return line

    async def next_expected_size(self, expected_size: int) -> bytes:
        await self.wait_for_enough_data(expected_size)
        data, self._buffer = self._buffer[:expected_size], self._buffer[expected_size:]
        await anext(self)
        return data


@dataclass
class RespQueryParser:
    buffered_reader: BufferedLineReader = field(default_factory=BufferedLineReader)

    async def parse(self) -> list[bytes]:
        query_length: int = await self.read_query_length()

        query: list[bytes] = [b""] * query_length
        for i in range(query_length):
            bulk_string_length = await self.read_bulk_string_length()
            query[i] = await self.read_bulk_string(bulk_string_length)

        return query

    async def read_bulk_string(self, bulk_string_length: int) -> bytes:
        line = await self.buffered_reader.next_expected_size(bulk_string_length)

        if len(line) != bulk_string_length:
            raise RespSyntaxError()

        return line[:bulk_string_length]

    async def read_bulk_string_length(self) -> int:
        line = await anext(self.buffered_reader)

        if line[0:1] != b"$":
            raise RespSyntaxError(f"Protocol error: expected '$', got '{line[0:1].decode()}'".encode())

        length_bytes = line[1:]

        try:
            length = int(length_bytes)
        except ValueError:
            raise RespSyntaxError(b"Protocol error: invalid bulk length")

        if length < 0:
            raise RespSyntaxError(b"Protocol error: invalid bulk length")
        if length > 512 * 1024 * 1024:
            raise RespSyntaxError(b"Protocol error: invalid bulk length")

        return length

    async def read_query_length(self) -> int:
        line = await anext(self.buffered_reader)

        if line[0:1] != b"*":
            raise RespSyntaxError()

        length_bytes = line[1:]

        try:
            length = int(length_bytes)
        except ValueError:
            raise RespSyntaxError()

        if length > 1024 * 1024:
            raise RespSyntaxError(b"Protocol error: invalid multibulk length")

        return length


@dataclass
class RespParser:
    buffered_reader: BufferedLineReader = field(default_factory=BufferedLineReader)

    async def __aiter__(self) -> AsyncIterator[list[bytes]]:
        line: bytes
        async for line in self.buffered_reader:
            if line[0:1] == b"*":
                self.buffered_reader.return_line(line)
                yield await RespQueryParser(self.buffered_reader).parse()
                continue
            yield line.split()

    def feed(self, data: bytes) -> None:
        if b"\x00" in data:
            raise RespFatalError()
        self.buffered_reader.feed(data)


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
        if length < 0:
            raise RespSyntaxError()
        if length > 1024 * 1024:
            raise RespSyntaxError(b"Protocol error: invalid multibulk length")

        array: list[bytes] = [b""] * length
        for i in range(length):
            array[i] = self.load_bulk_string()
        return array

    def load_bulk_string(self) -> bytes:
        line = self.read_line()
        if line[0:1] != b"$":
            raise RespSyntaxError(f"Protocol error: expected '$', got '{line[0:1].decode()}'".encode())

        length_bytes = line[1:]
        try:
            length = int(length_bytes)
        except ValueError:
            raise RespSyntaxError(b"Protocol error: invalid bulk length")

        if length < 0:
            raise RespSyntaxError(b"Protocol error: invalid bulk length")
        if length > 512 * 1024 * 1024:
            raise RespSyntaxError(b"Protocol error: invalid bulk length")

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
    writer: BinaryIO | IOBase | Transport

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


def dump(value: ValueType, stream: BinaryIO | IOBase | Transport) -> None:
    RespDumper(stream).dump(value)
