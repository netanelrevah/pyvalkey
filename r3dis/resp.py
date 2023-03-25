from dataclasses import dataclass
from typing import Any, AnyStr, BinaryIO


@dataclass
class RespLoader:
    reader: BinaryIO

    def load_array(self, length: int):
        array = [None] * length
        for i in range(length):
            array[i] = self.load()
        return array

    def load(self):
        line = self.reader.readline().strip()
        match line[0:1], line[1:]:
            case b"*", length:
                return self.load_array(int(length))
            case b"$", length:
                bulk_string = self.reader.read(int(length) + 2).strip()
                if len(bulk_string) != int(length):
                    raise ValueError()
                return bulk_string
            case b":", value:
                return int(value)
            case b"+", value:
                return value
            case b"-", value:
                return Exception(value)
            case _:
                return None


def load(stream: BinaryIO):
    return RespLoader(stream).load()


@dataclass
class RespDumper:
    writer: BinaryIO

    def dump_bulk_string(self, value: AnyStr):
        if isinstance(value, str):
            value = value.encode()
        self.writer.write(b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n")

    def dump_string(self, value: AnyStr):
        if isinstance(value, str):
            value = value.encode()
        self.writer.write(b"+" + value + b"\r\n")

    def dump_array(self, value: list):
        self.writer.write(f"*{len(value)}\r\n".encode())
        for item in value:
            self.dump(item, dump_bulk=True)

    def dump(self, value, dump_bulk=False):
        if isinstance(value, int):
            self.writer.write(f":{value}\r\n".encode())
        elif isinstance(value, str) or isinstance(value, bytes):
            if isinstance(value, str):
                value = value.encode()
            if dump_bulk or b"\r" in value or b"\n" in value:
                self.dump_bulk_string(value)
            else:
                self.dump_string(value)
        elif isinstance(value, list):
            self.dump_array(value)
        elif isinstance(value, set):
            self.dump_array(list(value))
        elif value is None:
            self.writer.write("$-1\r\n".encode())
        elif isinstance(value, Exception):
            self.writer.write(f"-{value.args[0]}\r\n".encode())


def dump(value: Any, stream: BinaryIO, dump_bulk=False):
    RespDumper(stream).dump(value, dump_bulk=dump_bulk)
