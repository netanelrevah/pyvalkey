from __future__ import annotations

import functools
import itertools
import math
import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from sortedcontainers import SortedDict, SortedSet

from pyvalkey.commands.parameters import positional_parameter
from pyvalkey.database_objects.utils import flatten


@functools.total_ordering
class MaxBytes(bytes):
    def less(self, other: Any) -> bool:  # noqa: ANN401
        if isinstance(other, bytes):
            return False
        raise NotImplementedError()

    def more(self, other: Any) -> bool:  # noqa: ANN401
        if isinstance(other, bytes):
            return True
        raise NotImplementedError()

    __eq__ = less
    __le__ = less
    __lt__ = less
    __ge__ = more
    __gt__ = more


MAX_BYTES = MaxBytes()


@dataclass(eq=True)
class RangeLimit:
    offset: int = positional_parameter()
    count: int = positional_parameter()


class ScoredSortedSet:
    def __init__(self, members_and_scores: Iterable[tuple[bytes, float]] | None = None) -> None:
        self.members = SortedSet({(score, member) for member, score in members_and_scores or []})
        self.members_scores = SortedDict({member: score for score, member in self.members})

    def _score_operation(
        self, old_score: float, new_score: float | None, operation: Callable[[float, float], float]
    ) -> float:
        if new_score is None:
            if operation == min:
                new_score = math.inf
            elif operation == max:
                new_score = -math.inf
            else:
                new_score = 0.0
        result_score = operation(old_score, new_score)
        if math.isnan(result_score):
            result_score = 0.0
        return result_score

    def union(
        self, *others: ScoredSortedSet, score_operation: Callable[[float, float], float] = operator.add
    ) -> ScoredSortedSet:
        new_set = ScoredSortedSet()
        new_set.members_scores = self.members_scores.copy()
        new_set.members = self.members.copy()

        for other in others:
            for member, score in other.members_scores.items():
                new_set.add(
                    self._score_operation(score, new_set.members_scores.get(member), score_operation),
                    member,
                )

        return new_set

    def intersection(
        self, *others: ScoredSortedSet, score_operation: Callable[[float, float], float] = operator.add
    ) -> ScoredSortedSet:
        members = set(self.members_scores.keys())
        for other in others:
            members &= set(other.members_scores.keys())

        new_set = ScoredSortedSet()
        for member, score in self.members_scores.items():
            if member in members:
                new_set.add(score, member)

        for other in others:
            for member, score in other.members_scores.items():
                if member in members:
                    new_set.add(
                        self._score_operation(score, new_set.members_scores.get(member), score_operation),
                        member,
                    )

        return new_set

    def difference(
        self, *others: ScoredSortedSet, score_operation: Callable[[float, float], float] = operator.add
    ) -> ScoredSortedSet:
        members = set(self.members_scores.keys())
        for other in others:
            members.difference_update(set(other.members_scores.keys()))

        new_set = ScoredSortedSet()
        for member, score in self.members_scores.items():
            if member in members:
                new_set.add(score, member)

        for other in others:
            for member, score in other.members_scores.items():
                if member in members:
                    new_set.add(
                        self._score_operation(score, new_set.members_scores.get(member), score_operation),
                        member,
                    )

        return new_set

    def update_from(self, other: ScoredSortedSet) -> None:
        for member, score in other.members_scores.items():
            self.add(score, member)

    def update(self, *scored_members: tuple[float, bytes]) -> None:
        for score, member in scored_members:
            self.add(score, member)

    def update_with_iterator(self, iterator: Iterable[bytes | float], invert_tuples: bool = False) -> None:
        score: float
        member: bytes
        for score, member in zip(*([iter(iterator)] * 2), strict=True):
            self.add(*((member, score) if invert_tuples else (score, member)))

    def remove(self, member: bytes) -> None:
        old_score = self.members_scores.pop(member)
        self.members.remove((old_score, member))

    def add(self, score: float, member: bytes) -> None:
        if member in self.members_scores:
            self.remove(member)
        self.members_scores[member] = score
        self.members.add((score, member))

    def range_by_score(
        self,
        min_score: float,
        max_score: float,
        min_inclusive: bool,
        max_inclusive: bool,
        with_scores: bool = False,
        is_reversed: bool = False,
        limit: RangeLimit | None = None,
    ) -> Iterable[bytes | float]:
        if not is_reversed:
            minimum = (min_score, (b"" if min_inclusive else MAX_BYTES))
            maximum = (max_score, (MAX_BYTES if max_inclusive else b""))
        else:
            min_inclusive, max_inclusive = max_inclusive, min_inclusive
            minimum = (max_score, (b"" if max_inclusive else MAX_BYTES))
            maximum = (min_score, (MAX_BYTES if min_inclusive else b""))

        iterator = self.members.irange(minimum, maximum, (min_inclusive, max_inclusive), reverse=is_reversed)

        if limit:
            offset = limit.offset
            count = limit.count + limit.offset
        else:
            offset = None
            count = None
        for score, member in itertools.islice(iterator, offset, count):
            if with_scores:
                yield member
                yield score
            else:
                yield member

    def range_by_lexical(
        self,
        min_lex: bytes,
        max_lex: bytes,
        min_inclusive: bool,
        max_inclusive: bool,
        with_scores: bool = False,
        is_reversed: bool = False,
        limit: RangeLimit | None = None,
    ) -> Iterable[bytes | float]:
        if not is_reversed:
            minimum, maximum = min_lex, max_lex
        else:
            minimum, maximum = max_lex, min_lex
            min_inclusive, max_inclusive = max_inclusive, min_inclusive

        iterator = self.members_scores.irange(minimum, maximum, (min_inclusive, max_inclusive), reverse=is_reversed)

        if limit:
            offset = limit.offset
            count = limit.count + limit.offset
        else:
            offset = None
            count = None
        for member in itertools.islice(iterator, offset, count):
            if with_scores:
                yield member
                yield self.members_scores[member]
            else:
                yield member

    def range(self, range_slice: slice, with_scores: bool = False) -> list[bytes | float]:
        result = self.members[range_slice]

        offset = None
        count = None
        if with_scores:
            return list(itertools.islice(flatten(result, reverse_sub_lists=True), offset, count))
        return list(itertools.islice((member for score, member in result), offset, count))

    def pop_minimum(self) -> tuple[float, bytes]:
        if not self.members:
            raise KeyError("SortedSet is empty")
        score, member = self.members.pop(0)
        del self.members_scores[member]
        return score, member

    def pop_maximum(self) -> tuple[float, bytes]:
        if not self.members:
            raise KeyError("SortedSet is empty")
        score, member = self.members.pop(-1)
        del self.members_scores[member]
        return score, member

    def __len__(self) -> int:
        return len(self.members)
