from collections import defaultdict
from collections.abc import Hashable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Generic, TypeVar

HashableKeyType = TypeVar("HashableKeyType", bound=Hashable)
HashableValueType = TypeVar("HashableValueType", bound=Hashable)


@dataclass
class SetMapping(Generic[HashableKeyType, HashableValueType]):
    mapping: defaultdict[HashableKeyType, list[HashableValueType]] = field(default_factory=lambda: defaultdict(list))
    reverse_mapping: dict[HashableValueType, set[HashableKeyType]] = field(default_factory=lambda: defaultdict(set))

    def add(self, key: HashableKeyType, value: HashableValueType) -> None:
        if value not in self.mapping[key]:
            self.mapping[key].append(value)
        self.reverse_mapping[value].add(key)

    def add_multiple(self, keys: Iterable[HashableKeyType], value: HashableValueType) -> None:
        for key in keys:
            self.add(key, value)

    def remove(self, key: HashableKeyType, value: HashableValueType) -> None:
        self.mapping[key].remove(value)
        if not self.mapping[key]:
            del self.mapping[key]
        self.reverse_mapping[value].remove(key)
        if not self.reverse_mapping[value]:
            del self.reverse_mapping[value]

    def remove_all(self, value: HashableValueType) -> None:
        keys = list(self.reverse_mapping[value])
        for key in keys:
            self.remove(key, value)

    def iter_values(self, key: HashableKeyType) -> Iterator[HashableValueType]:
        yield from self.mapping.get(key, [])

    @property
    def values_count(self) -> int:
        return len(self.reverse_mapping)
