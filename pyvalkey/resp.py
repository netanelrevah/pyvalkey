from dataclasses import dataclass
from typing import Any, AnyStr, BinaryIO


class RespSimpleString(bytes):
    pass


RESP_OK = RespSimpleString(b"OK")


class RespError(bytes):
    pass


ValueType = bool | int | float | RespSimpleString | RespError | str | list | set | dict


@dataclass
class RespLoader:
    reader: BinaryIO

    def load_array(self, length: int) -> list:
        array = [None] * length
        for i in range(length):
            array[i] = self.load()
        return array

    def load(self) -> list | bytes | None | int:
        line = self.reader.readline().strip(b"\r\n")
        match line[0:1], line[1:]:
            case b"*", length:
                return self.load_array(int(length))
            case b"$", length:
                bulk_string = self.reader.read(int(length) + 2).strip(b"\r\n")
                if len(bulk_string) != int(length):
                    raise ValueError()
                return bulk_string
            case b":", value:
                return int(value)
            case b"+", value:
                return RespSimpleString(value)
            case b"-", value:
                return RespError(value)
            case _:
                return None


def load(stream: BinaryIO) -> list | bytes | int | None:
    return RespLoader(stream).load()


@dataclass
class RespDumper:
    writer: BinaryIO

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
        elif isinstance(value, (str, bytes)):
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
            self.writer.write("$-1\r\n".encode())


def dump(value: ValueType, stream: BinaryIO) -> None:
    RespDumper(stream).dump(value)
