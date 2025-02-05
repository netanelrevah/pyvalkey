from __future__ import annotations

from array import array
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, ClassVar


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

    def append(self, value: bytes | int) -> None:
        self.insert_by_data_index(len(self.data) - 1, value)

    def pop_by_data_index(self, data_index: int) -> bytes | int:
        element_index = data_index
        element = self.get_by_index(element_index)

        element_size = self.get_encoded_size_unsafe(element_index)
        element_size += self.get_back_length_byte_size(element_size)

        if element_size >= 0:
            self.data = self.data[:element_index] + self.data[element_index + element_size :]
            self.total_bytes -= element_size
            self.number_of_elements -= 1

        return element

    def pop(self, element_index: int) -> bytes | int:
        reversed_iteration = element_index < 0
        iteration_count = element_index if element_index >= 0 else abs(element_index + 1)

        iterator = ListpackIterator(self, reversed=reversed_iteration)

        for _ in range(iteration_count):
            iterator.skip()

        return self.pop_by_data_index(iterator.current_index)

    def insert_by_data_index(self, data_index: int, value: bytes | int) -> None:
        new_element = self.get_encoded_type(value).encode(value)
        self.data = self.data[:data_index] + new_element + self.data[data_index:]
        self.total_bytes += len(new_element)
        if self.number_of_elements + 1 < self.HEADER_NUMBER_OF_ELEMENTS_UNKNOWN:
            self.number_of_elements += 1

    def prepend(self, value: bytes | int) -> None:
        self.insert_by_data_index(self.HEADER_SIZE, value)

    def seek(self, element_index: int) -> bytes | int | None:
        reversed_iteration = element_index < 0
        iteration_count = element_index if element_index >= 0 else abs(element_index + 1)

        iterator = ListpackIterator(self, reversed=reversed_iteration)

        for _ in range(iteration_count):
            iterator.skip()

        return next(iterator, None)

    @classmethod
    def get_encoded_type(cls, element: bytes | int) -> type[IntListpackElement] | type[BytesListpackElement]:
        try:
            element = int(element)
        except ValueError:
            pass

        if isinstance(element, int):
            if 0 <= element <= 127:  # noqa: PLR2004
                return listpack_7bit_uint
            if -4096 <= element < 4096:  # noqa: PLR2004
                return listpack_13bit_int
            if -32768 <= element < 32768:  # noqa: PLR2004
                return listpack_16bit_int
            if -8388608 <= element < 8388608:  # noqa: PLR2004
                return listpack_24bit_int
            if -2147483648 <= element < 2147483648:  # noqa: PLR2004
                return listpack_32bit_int
            return listpack_64bit_int
        else:
            if len(element) < 64:  # noqa: PLR2004
                return listpack_6bit_string
            if len(element) < 4096:  # noqa: PLR2004
                return listpack_12bit_string
            return listpack_32bit_string

    def first(self) -> int | bytes | ListpackElement | None:
        if self.data[self.HEADER_SIZE] == self.EOF:
            return None
        return self.get_by_index(0)

    def __len__(self) -> int:
        if self.number_of_elements != self.HEADER_NUMBER_OF_ELEMENTS_UNKNOWN:
            return self.number_of_elements
        number_of_element = sum(1 for _ in self)
        if number_of_element < self.HEADER_NUMBER_OF_ELEMENTS_UNKNOWN:
            self.number_of_elements = number_of_element
        return number_of_element

    def __iter__(self) -> Iterator[Any]:
        return ListpackIterator(self)

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

    def get_by_index(self, index: int) -> int | bytes:
        if listpack_7bit_uint.is_encoded(self, index):
            return listpack_7bit_uint.from_index(self, index)
        if listpack_6bit_string.is_encoded(self, index):
            return listpack_6bit_string.from_index(self, index)
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
            return listpack_12bit_string.from_index(self, index)
        if listpack_32bit_string.is_encoded(self, index):
            return listpack_32bit_string.from_index(self, index)
        return 12345678900000000 + self.data[index]

    def get_next_index(self, current_index: int) -> int:
        entry_length = self.get_encoded_size_unsafe(current_index)
        entry_length += self.get_back_length_byte_size(entry_length)
        return current_index + entry_length

    def get_next(self, current_index: int) -> int | bytes | None:
        if self.data[current_index] == listpack.EOF:
            return None
        return self.get_by_index(self.get_next_index(current_index))

    def get_previous_index(self, current_index: int) -> int:
        entry_length = self.get_back_length(current_index)
        entry_length += listpack.get_back_length_byte_size(entry_length)
        return current_index - entry_length

    def get_previous(self, current_index: int) -> int | bytes | None:
        if current_index == self.HEADER_SIZE:
            return None
        return self.get_by_index(self.get_previous_index(current_index))

    def get_back_length(self, current_index: int) -> int:
        length = 0
        for index in range(5):
            length += self.data[current_index - index - 1] & BytesListpackElement.BACK_LENGTH_DATA_MASK
            if not (self.data[current_index - index - 1] & BytesListpackElement.BACK_LENGTH_HAS_MORE_FLAG):
                break
            length = length << (index * 7)
        return length

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
    reversed: bool = False

    _current_index: int | None = None

    @property
    def current_index(self) -> int:
        if self._current_index is None:
            if not self.reversed:
                self._current_index = listpack.HEADER_SIZE
            else:
                self._current_index = listpack.get_previous_index(self.iterated, len(self.iterated.data) - 1)
        return self._current_index

    @current_index.setter
    def current_index(self, value: int) -> None:
        self._current_index = value

    def __next__(self) -> int | bytes:
        if (self.reversed and self.current_index < listpack.HEADER_SIZE) or (
            (not self.reversed) and self.iterated.data[self.current_index] == listpack.EOF
        ):
            raise StopIteration()

        current_value = self.iterated.get_by_index(self.current_index)
        self.skip()
        return current_value

    def skip(self) -> None:
        if self.reversed:
            self.current_index = self.iterated.get_previous_index(self.current_index)
        else:
            self.current_index = self.iterated.get_next_index(self.current_index)


class ListpackElement:
    FLAG: ClassVar[int]
    MASK: ClassVar[int]

    parent: listpack
    element_index: int

    @classmethod
    def is_encoded(cls, instance: listpack, index: int) -> bool:
        return (instance.data[index] & cls.MASK) == cls.FLAG

    def next(self) -> int | bytes | ListpackElement | None:
        return self.parent.get_next(self.element_index)

    @classmethod
    def encode(cls, value: bytes | int) -> array[int]:
        raise NotImplementedError()


class IntListpackElement(ListpackElement):
    NEGATIVE_MAX: ClassVar[int]
    NEGATIVE_START: ClassVar[int]

    @classmethod
    def uint_to_int(cls, value: int) -> int:
        if value >= cls.NEGATIVE_START:
            return -(cls.NEGATIVE_MAX - value) - 1
        return value

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        raise NotImplementedError()

    @classmethod
    def from_index(cls, instance: listpack, index: int) -> int:
        return int(cls.uint_to_int(cls.get_unsigned_value(instance, index)))

    @classmethod
    def encode(cls, value: bytes | int) -> array[int]:
        return cls.encode_int(int(value))

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        raise NotImplementedError()


class BytesListpackElement(ListpackElement):
    HEADER_LENGTH: ClassVar[int]

    BACK_LENGTH_DATA_MASK = 0b01111111  # 127
    BACK_LENGTH_HAS_MORE_FLAG = 0b10000000  # 128

    BACK_LENGTH_MAX_SIZE_BYTE = (1 << (7 * 1)) - 1
    BACK_LENGTH_MAX_SIZE_2_BYTES = (1 << (7 * 2)) - 1
    BACK_LENGTH_MAX_SIZE_3_BYTES = (1 << (7 * 3)) - 1
    BACK_LENGTH_MAX_SIZE_4_BYTES = (1 << (7 * 4)) - 1
    BACK_LENGTH_MAX_SIZE_5_BYTES = (1 << (7 * 5)) - 1

    @classmethod
    def from_index(cls, instance: listpack, index: int) -> bytes:
        return bytes(
            instance.data[
                index + cls.HEADER_LENGTH : index + cls.HEADER_LENGTH + cls.length(instance, index)
            ].tobytes(),
        )

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        raise NotImplementedError()

    @classmethod
    def encode(cls, value: bytes | int) -> array[int]:
        if isinstance(value, int):
            raise ValueError()
        return cls.encode_string(value)

    @classmethod
    def encode_string(cls, value: bytes) -> array[int]:
        raise NotImplementedError()

    @classmethod
    def encode_back_length(cls, length: int) -> array[int]:
        if length <= cls.BACK_LENGTH_MAX_SIZE_BYTE:
            return array[int](
                "B",
                [
                    length,
                ],
            )
        if length <= cls.BACK_LENGTH_MAX_SIZE_2_BYTES:
            return array[int](
                "B",
                [
                    length >> 7,
                    (length & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                ],
            )
        if length <= cls.BACK_LENGTH_MAX_SIZE_3_BYTES:
            return array[int](
                "B",
                [
                    length >> 14,
                    ((length >> 7) & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                    (length & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                ],
            )
        if length <= cls.BACK_LENGTH_MAX_SIZE_4_BYTES:
            return array[int](
                "B",
                [
                    length >> 21,
                    ((length >> 14) & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                    ((length >> 7) & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                    (length & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                ],
            )

        return array[int](
            "B",
            [
                length >> 28,
                ((length >> 21) & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                ((length >> 14) & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                ((length >> 7) & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
                (length & cls.BACK_LENGTH_DATA_MASK) | cls.BACK_LENGTH_HAS_MORE_FLAG,
            ],
        )


class Listpack7BitUint(IntListpackElement):
    FLAG = 0
    MASK = 0b10000000  # 0x80
    ENTRY_SIZE = 2
    NEGATIVE_MAX = 0
    NEGATIVE_START = 0xFF  # always positive

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        return instance.data[index]

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
    HEADER_LENGTH = 1

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        return instance.data[index] & 0x3F

    @classmethod
    def encode_string(cls, value: bytes) -> array[int]:
        encoded = array[int]("B", [len(value) | cls.FLAG])
        encoded.frombytes(value)
        encoded.append(len(value) + cls.HEADER_LENGTH)
        return encoded


class Listpack13BitInt(IntListpackElement):
    FLAG = 0b11000000  # 0xC0
    MASK = 0b11100000  # 0xE0
    UNMASK = 0xFF - MASK  # 0x00011111
    ENTRY_SIZE = 3
    NEGATIVE_MAX = (1 << 13) - 1  # 8191
    NEGATIVE_START = 1 << 12  # 4096

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        return ((instance.data[index] & cls.UNMASK) << 8) | instance.data[index + 1]

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
    FLAG = 0b11100000  # 0xE0
    MASK = 0x11110000  # 0xF0
    HEADER_LENGTH = 2

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        return ((instance.data[index] & 0xF) << 8) | instance.data[index + 1]

    @classmethod
    def encode_string(cls, value: bytes) -> array[int]:
        encoded = array[int](
            "B",
            [(len(value) >> 8) | cls.FLAG, len(value) & 0xFF],
        )
        encoded.frombytes(value)
        encoded += cls.encode_back_length(len(value) + cls.HEADER_LENGTH)
        return encoded


class Listpack16BitInt(IntListpackElement):
    FLAG = 0b11110001  # 0xF1
    MASK = 0b11111111  # 0xFF
    ENTRY_SIZE = 4

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [cls.FLAG, (value & 0xFF), value >> 8, 3],
        )


class Listpack24BitInt(IntListpackElement):
    FLAG = 0b11110010  # 0xF1
    MASK = 0b11111111  # 0xFF
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
    FLAG = 0b11110011  # 0xF1
    MASK = 0b11111111  # 0xFF
    ENTRY_SIZE = 6

    NEGATIVE_MAX = (1 << 32) - 1
    NEGATIVE_START = 1 << 31

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        return listpack.get_word_value(instance.data, index)

    @classmethod
    def encode_int(cls, value: int) -> array[int]:
        return array[int](
            "B",
            [cls.FLAG, (value & 0xFF), (value >> 8) & 0xFF, (value >> 16) & 0xFF, (value >> 24), 5],
        )


class Listpack64BitInt(IntListpackElement):
    FLAG = 0b11110100  # 0xF1
    MASK = 0b11111111  # 0xFF
    ENTRY_SIZE = 10
    NEGATIVE_MAX = (1 << 64) - 1
    NEGATIVE_START = 1 << 63

    @classmethod
    def get_unsigned_value(cls, instance: listpack, index: int) -> int:
        return (
            (instance.data[index + 1])
            | (instance.data[index + 2] << 8)
            | (instance.data[index + 3] << 16)
            | (instance.data[index + 4] << 24)
            | (instance.data[index + 5] << 32)
            | (instance.data[index + 6] << 40)
            | (instance.data[index + 7] << 48)
            | (instance.data[index + 8] << 56)
        )

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
                (value >> 40) & 0xFF,
                (value >> 48) & 0xFF,
                (value >> 56),
                9,
            ],
        )


class Listpack32BitString(BytesListpackElement):
    FLAG = 0b11110000  # 0xF1
    MASK = 0b11111111  # 0xFF
    HEADER_LENGTH = 5

    @classmethod
    def length(cls, instance: listpack, index: int) -> int:
        return listpack.get_word_value(instance.data, index + 1)

    @classmethod
    def encode_string(cls, value: bytes) -> array[int]:
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
        encoded += cls.encode_back_length(len(value) + cls.HEADER_LENGTH)
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
