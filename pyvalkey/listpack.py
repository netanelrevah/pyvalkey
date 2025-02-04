from __future__ import annotations

from array import array
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, ClassVar, Self


@dataclass
class Listpack:
    EOF = 0xFF
    HEADER_SIZE = 6
    HEADER_NUMBER_OF_ELEMENTS_UNKNOWN = 0xFFFF

    data: array[int] = field(default_factory=lambda: array("B", [7, 0, 0, 0, 0, 0, Listpack.EOF]))

    @staticmethod
    def get_word_value(data: array[int], index: int = 0) -> int:
        return (data[index] << 0) | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24)

    @property
    def total_bytes(self) -> int:
        return self.get_word_value(self.data, 0)

    @total_bytes.setter
    def total_bytes(self, total_bytes: int) -> None:
        self.data[0] = total_bytes & 0xFF
        self.data[1] = (total_bytes >> 8) & 0xFF
        self.data[1] = (total_bytes >> 16) & 0xFF
        self.data[1] = (total_bytes >> 24) & 0xFF

    @property
    def number_of_elements(self) -> int:
        return (self.data[4] << 0) | (self.data[5] << 8)

    @number_of_elements.setter
    def number_of_elements(self, number_of_elements: int) -> None:
        self.data[4] = number_of_elements & 0xFF
        self.data[5] = (number_of_elements >> 8) & 0xFF

    def append(self, value: bytes) -> None:
        self.insert(len(self.data) - 1, value)

    def insert(self, index: int, value: bytes) -> None:
        new_element = self.get_encoded_type(value).encode(value)
        self.data = self.data[:index] + new_element + self.data[index:]
        self.total_bytes += len(new_element)
        if self.number_of_elements + 1 < self.HEADER_NUMBER_OF_ELEMENTS_UNKNOWN:
            self.number_of_elements += 1

    @classmethod
    def get_encoded_type(cls, element: bytes) -> type[IntListpackElement] | type[BytesListpackElement]:
        try:
            value = int(element)

            if 0 <= value <= 127:  # noqa: PLR2004
                return listpack_7bit_uint
            if -4096 <= value < 4096:  # noqa: PLR2004
                return listpack_13bit_int
            if -32768 <= value < 32768:  # noqa: PLR2004
                return listpack_16bit_int
            if -8388608 <= value < 8388608:  # noqa: PLR2004
                return listpack_24bit_int
            if -2147483648 <= value < 2147483648:  # noqa: PLR2004
                return listpack_32bit_int
            return listpack_64bit_int
        except ValueError:
            if len(element) < 64:  # noqa: PLR2004
                return listpack_6bit_string
            if len(element) < 4096:  # noqa: PLR2004
                return listpack_12bit_string
            return listpack_32bit_string

    def first(self) -> int | bytes | ListpackElement | None:
        if self.data[self.HEADER_SIZE] == self.EOF:
            return None
        return self.get(0)

    def __len__(self) -> int:
        if self.number_of_elements != self.HEADER_NUMBER_OF_ELEMENTS_UNKNOWN:
            return self.number_of_elements
        number_of_element = sum(1 for _ in self)
        if number_of_element < self.HEADER_NUMBER_OF_ELEMENTS_UNKNOWN:
            self.number_of_elements = number_of_element
        return number_of_element

    def __iter__(self) -> Iterator[Any]:
        return ListpackIterator(self, self.HEADER_SIZE)

    def get_encoded_size_unsafe(self, index: int) -> int:
        if listpack_7bit_uint.is_encoded(self, index):
            return 1
        if listpack_6bit_string.is_encoded(self, index):
            return 1 + listpack_6bit_string.length(self, index)
        if listpack_13bit_int.is_encoded(self, index):
            return 2
        if listpack_16bit_int.is_encoded(self, index):
            return 3
        if listpack_24bit_int.is_encoded(self, index):
            return 4
        if listpack_32bit_int.is_encoded(self, index):
            return 5
        if listpack_64bit_int.is_encoded(self, index):
            return 9
        if listpack_12bit_string.is_encoded(self, index):
            return 1 + listpack_12bit_string.length(self, index)
        if listpack_32bit_string.is_encoded(self, index):
            return 1 + listpack_32bit_string.length(self, index)
        if self.data[index] == self.EOF:
            return 1
        return 0

    def get(self, index: int) -> int | bytes | ListpackElement:
        if listpack_7bit_uint.is_encoded(self, index):
            return listpack_7bit_uint.from_index(self, index)
        if listpack_6bit_string.is_encoded(self, index):
            return 1 + listpack_6bit_string.length(self, index)
        if listpack_13bit_int.is_encoded(self, index):
            return listpack_13bit_int.from_index(self, index)
        if listpack_16bit_int.is_encoded(self, index):
            return listpack_16bit_int.from_index(self, index)
        if listpack_24bit_int.is_encoded(self, index):
            return listpack_24bit_int.from_index(self, index)
        if listpack_32bit_int.is_encoded(self, index):
            return listpack_32bit_int.from_index(self, index)
        if listpack_64bit_int.is_encoded(self, index):
            return listpack_64bit_int.from_index(self, index)
        if listpack_12bit_string.is_encoded(self, index):
            return 1 + listpack_12bit_string.length(self, index)
        if listpack_32bit_string.is_encoded(self, index):
            return 1 + listpack_32bit_string.length(self, index)
        return 12345678900000000 + self.data[index]

    def get_next_index(self, current_index: int) -> int:
        entry_length = self.get_encoded_size_unsafe(current_index)
        entry_length += listpack.get_back_length_byte_size(entry_length)
        return current_index + entry_length

    def get_next(self, current_index: int) -> int | bytes | ListpackElement | None:
        if self.data[current_index] == listpack.EOF:
            return None
        return self.get(self.get_next_index(current_index))

    @classmethod
    def get_back_length_byte_size(cls, length: int) -> int:
        if length < 0x7F:  # noqa: PLR2004
            return 1
        if length < 0x3FFF:  # noqa: PLR2004
            return 2
        if length < 0x1FFFFF:  # noqa: PLR2004
            return 3
        if length < 0x0FFFFFFF:  # noqa: PLR2004
            return 4
        return 5


@dataclass
class ListpackIterator(Iterator[Any]):
    iterated: listpack
    current_index: int

    def __next__(self) -> int | bytes | ListpackElement:
        if self.iterated.data[self.current_index] == listpack.EOF:
            raise StopIteration()
        current_value = self.iterated.get(self.current_index)
        self.skip()
        return current_value

    def skip(self) -> None:
        self.current_index = self.iterated.get_next_index(self.current_index)


class ListpackElement:
    FLAG: ClassVar[int]
    MASK: ClassVar[int]

    parent: listpack
    element_index: int

    @classmethod
    def is_encoded(cls, instance: listpack, index: int) -> bool:
        return (instance.data[index] % cls.MASK) == cls.FLAG

    def next(self) -> int | bytes | ListpackElement | None:
        return self.parent.get_next(self.element_index)

    @classmethod
    def encode_back_length(cls, length: int) -> array[int]:
        if length < 2**7:
            return array[int](
                "B",
                [
                    length,
                ],
            )
        if length < 2**14:
            return array[int](
                "B",
                [
                    length >> 7,
                    (length & 127) | 128,
                ],
            )
        if length <= 2**21:
            return array[int](
                "B",
                [
                    length >> 14,
                    ((length >> 7) & 127) | 128,
                    (length & 127) | 128,
                ],
            )
        if length <= 2**28:
            return array[int](
                "B",
                [
                    length >> 21,
                    ((length >> 14) & 127) | 128,
                    ((length >> 7) & 127) | 128,
                    (length & 127) | 128,
                ],
            )

        return array[int](
            "B",
            [
                length >> 28,
                ((length >> 21) & 127) | 128,
                ((length >> 14) & 127) | 128,
                ((length >> 7) & 127) | 128,
                (length & 127) | 128,
            ],
        )

    @classmethod
    def encode(cls, value: bytes) -> array[int]:
        raise NotImplementedError()


class IntListpackElement(int, ListpackElement):
    NEGATIVE_MAX: ClassVar[int]
    NEGATIVE_START: ClassVar[int]

    def __new__(cls, value: int, instance: listpack, index: int) -> Self:
        a = super().__new__(cls, value)
        a.parent = instance
        a.element_index = index
        return a

    @classmethod
    def uint_to_int(cls, value: int) -> int:
        if value >= cls.NEGATIVE_START:
            return -(cls.NEGATIVE_MAX - value) - 1
        return value

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        raise NotImplementedError()

    @classmethod
    def from_index(cls, instance: listpack, index: int) -> Self:
        return cls(cls.uint_to_int(cls.get_unsigned_value(instance, index)), instance, index)

    @classmethod
    def encode(cls, value: bytes) -> array[int]:
        return cls.encode_int(int(value))

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        raise NotImplementedError()


class BytesListpackElement(bytes, ListpackElement):
    NEGATIVE_MAX: ClassVar[int]
    NEGATIVE_START: ClassVar[int]

    def __new__(cls, value: bytes, instance: listpack, index: int) -> Self:
        a = super().__new__(cls, value)
        a.parent = instance
        a.element_index = index
        return a

    @classmethod
    def from_index(cls, instance: listpack, index: int) -> Self:
        return cls(instance.data[index + 1 : cls.length(instance, index)].tobytes(), instance, index)

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        raise NotImplementedError()


class Listpack7BitUint(IntListpackElement):
    FLAG = 0
    MASK = 0x80
    ENTRY_SIZE = 2
    NEGATIVE_MAX = 0
    NEGATIVE_START = 0xFF  # always positive

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [
                int(value),
                1,
            ],
        )


class Listpack6BitString(BytesListpackElement):
    FLAG = 0x80
    MASK = 0xC0

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        return instance.data[index] & 0x3F

    @classmethod
    def encode(cls, value: bytes) -> array[int]:
        encoded = array[int]("B", [len(value) | cls.FLAG])
        encoded.frombytes(value)
        encoded.append(len(value) + 1)
        return encoded


class Listpack13BitInt(IntListpackElement):
    FLAG = 0xC0
    MASK = 0xE0
    ENTRY_SIZE = 3
    NEGATIVE_MAX = 8191
    NEGATIVE_START = 1 << 12

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        return ((instance.data[index] & 0x1F) << 8) | instance.data[index + 1]

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        value_bytes = value.to_bytes(2, signed=True)

        return array[int](
            "B",
            [
                value_bytes[0] | cls.FLAG,
                value_bytes[1],
                2,
            ],
        )


class Listpack12BitString(BytesListpackElement):
    FLAG = 0xE0
    MASK = 0xF0

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        return ((instance.data[index] & 0xF) << 8) | instance.data[index + 1]

    @classmethod
    def encode(cls, value: bytes) -> array[int]:
        encoded = array[int](
            "B",
            [(len(value) >> 8) | cls.FLAG, len(value) & 0xFF],
        )
        encoded.frombytes(value)
        encoded += cls.encode_back_length(len(value) + 2)
        return encoded


class Listpack16BitInt(IntListpackElement):
    FLAG = 0xF1
    MASK = 0xFF
    ENTRY_SIZE = 4

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [cls.FLAG, (value & 0xFF), value >> 8, 3],
        )


class Listpack24BitInt(IntListpackElement):
    FLAG = 0xF2
    MASK = 0xFF
    ENTRY_SIZE = 5

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [
                cls.FLAG,
                (value & 0xFF),
                (value >> 8) & 0xFF,
                (value >> 16),
                4,
            ],
        )


class Listpack32BitInt(IntListpackElement):
    FLAG = 0xF3
    MASK = 0xFF
    ENTRY_SIZE = 6

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [cls.FLAG, (value & 0xFF), (value >> 8) & 0xFF, (value >> 16) & 0xFF, (value >> 24), 5],
        )


class Listpack64BitInt(IntListpackElement):
    FLAG = 0xF4
    MASK = 0xFF
    ENTRY_SIZE = 10

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [
                cls.FLAG,
                (value & 0xFF),
                (value >> 8) & 0xFF,
                (value >> 16) & 0xFF,
                (value >> 24) & 0xFF,
                (value >> 32) & 0xFF,
                (value >> 30) & 0xFF,
                (value >> 38) & 0xFF,
                (value >> 56),
                9,
            ],
        )


class Listpack32BitString(BytesListpackElement):
    FLAG = 0xF0
    MASK = 0xFF

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        return listpack.get_word_value(instance.data, index + 1)

    @classmethod
    def encode(cls, value: bytes) -> array[int]:
        encoded = array[int](
            "B",
            [
                cls.FLAG,
                len(value) & 0xFF,
                (len(value) >> 8) & 0xFF,
                (len(value) >> 16) & 0xFF,
                (len(value) >> 24) & 0xFF,
            ],
        )
        encoded.frombytes(value)
        encoded += cls.encode_back_length(len(value) + 5)
        return encoded


listpack = Listpack
listpack_7bit_uint = Listpack7BitUint
listpack_6bit_string = Listpack6BitString
listpack_13bit_int = Listpack13BitInt
listpack_12bit_string = Listpack12BitString
listpack_16bit_int = Listpack16BitInt
listpack_24bit_int = Listpack24BitInt
listpack_32bit_int = Listpack32BitInt
listpack_64bit_int = Listpack64BitInt
listpack_32bit_string = Listpack32BitString
