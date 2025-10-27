from __future__ import annotations

import math
import operator
import random
from collections.abc import Callable
from dataclasses import field
from enum import Enum
from itertools import zip_longest
from typing import Protocol

from pyvalkey.commands.context import ClientContext
from pyvalkey.commands.core import Command
from pyvalkey.commands.dependencies import dependency
from pyvalkey.commands.parameters import (
    keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.parsers import CommandMetadata
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.consts import LONG_MAX
from pyvalkey.database_objects.databases import (
    Database,
    SortedSetBlockingManager,
)
from pyvalkey.database_objects.errors import ServerError, ServerWrongNumberOfArgumentsError
from pyvalkey.database_objects.scored_sorted_set import MAX_BYTES, RangeLimit, ScoredSortedSet
from pyvalkey.database_objects.utils import flatten
from pyvalkey.resp import ArrayNone, RespProtocolVersion, ValueType


class AggregateMode(Enum):
    SUM = b"SUM"
    MIN = b"MIN"
    MAX = b"MAX"


AGGREGATION_MODE_TO_OPERATOR: dict[AggregateMode, Callable[[float, float], float]] = {
    AggregateMode.SUM: operator.add,
    AggregateMode.MIN: min,
    AggregateMode.MAX: max,
}


def parse_score_parameter(score: bytes, is_lexical: bool = False) -> tuple[bytes, bool]:
    result_score: bytes
    inclusive = True
    if score == b"-":
        result_score = b""
    elif score == b"+":
        result_score = MAX_BYTES
    elif score == b"-inf":
        result_score = score
    elif score == b"+inf":
        result_score = score
    elif b"(" in score:
        result_score = score[1:]
        inclusive = False
    elif b"[" in score:
        result_score = score[1:]
    elif b"]" in score:
        result_score = score[:-1]
    elif b")" in score:
        result_score = score[:-1]
        inclusive = False
    else:
        if is_lexical:
            raise ServerError(b"ERR min or max not valid string range item")
        result_score = score

    return result_score, inclusive


def parse_ordered_range_parameters(
    min_score: bytes, max_score: bytes, is_lexical: bool = False
) -> tuple[bytes, bool, bytes, bool]:
    # noinspection PyTypeChecker
    return parse_score_parameter(min_score, is_lexical=is_lexical) + parse_score_parameter(
        max_score, is_lexical=is_lexical
    )


class RangeMode(Enum):
    BY_INDEX = None
    BY_SCORE = b"BYSCORE"
    BY_LEX = b"BYLEX"


def sorted_set_range(
    database: Database,
    key: bytes,
    start: bytes,
    stop: bytes,
    with_scores: bool = False,
    range_mode: RangeMode = RangeMode.BY_INDEX,
    is_reversed: bool = False,
    limit: RangeLimit | None = None,
    destination: bytes | None = None,
    nested_member_score: bool = False,
) -> int | list:
    if range_mode == RangeMode.BY_INDEX and limit is not None:
        raise ServerError(b"ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")

    if range_mode == RangeMode.BY_LEX and with_scores:
        raise ServerError(b"ERR syntax error, WITHSCORES not supported in combination with BYLEX")

    z = database.sorted_set_database.get_value_or_empty(key)

    if range_mode == RangeMode.BY_SCORE:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(start, stop)

        valid_float = True
        try:
            float_min_score = float(min_score)
            float_max_score = float(max_score)
            if math.isnan(float_min_score) or math.isnan(float_max_score):
                valid_float = False
        except ValueError:
            valid_float = False
        if not valid_float:
            raise ServerError(b"ERR value is not valid float")

        result_iterator = z.range_by_score(
            float_min_score,
            float_max_score,
            min_inclusive,
            max_inclusive,
            with_scores=with_scores or destination is not None,
            is_reversed=is_reversed,
            limit=limit,
        )
    elif range_mode == RangeMode.BY_LEX:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(
            start, stop, is_lexical=True
        )
        result_iterator = z.range_by_lexical(
            min_score,
            max_score,
            min_inclusive,
            max_inclusive,
            with_scores=with_scores or destination is not None,
            is_reversed=is_reversed,
            limit=limit,
        )
    else:
        result_iterator = z.range(
            parse_range_parameters(int(start), int(stop), is_reversed=is_reversed),
            with_scores=with_scores or destination is not None,
        )

    if destination is not None:
        database.sorted_set_database.pop(destination, default=None)
        value = database.sorted_set_database.get_value_or_create(destination)
        value.update_with_iterator(result_iterator, invert_tuples=True)

        return len(value)

    if with_scores and nested_member_score:
        return [[member, score] for member, score in zip(*([iter(result_iterator)] * 2), strict=True)]
    return list(result_iterator)


class AddMode(Enum):
    ALL = None
    UPDATE_ONLY = b"XX"
    INSERT_ONLY = b"NX"


class ScoreUpdateMode(Enum):
    ALL = None
    LESS_THAN = b"LT"
    GREATER_THAN = b"GT"


def sorted_set_value_pop(
    value: ScoredSortedSet,
    count: int,
    pop_operation: Callable[[ScoredSortedSet], tuple[float, bytes]],
    nested_member_score: bool = False,
    unnest_single_tuple: bool = True,
) -> list:
    result: list = []
    for _ in range(min(count, len(value))):
        score, member = pop_operation(value)
        if not nested_member_score:
            result.append(member)
            result.append(score)
        else:
            result.append([member, score])

    if nested_member_score and unnest_single_tuple and len(result) == 1:
        return result[0]

    return result


def sorted_set_key_pop(
    database: Database,
    key: bytes,
    count: int,
    pop_operation: Callable[[ScoredSortedSet], tuple[float, bytes]],
    nested_member_score: bool,
    unnest_single_tuple: bool = True,
) -> list:
    if count < 0:
        raise ServerError(b"ERR value is out of range, must be positive")

    value = database.sorted_set_database.get_value_or_empty(key)

    return sorted_set_value_pop(
        value, count, pop_operation, nested_member_score, unnest_single_tuple=unnest_single_tuple
    )


def sorted_set_multikey_pop(
    database: Database,
    keys: list[bytes],
    count: int,
    pop_operation: Callable[[ScoredSortedSet], tuple[float, bytes]],
) -> list | None:
    if count <= 0:
        raise ServerError(b"ERR count should be greater than 0")

    while keys:
        key = keys.pop(0)
        value = database.sorted_set_database.get_value_or_empty(key)
        if len(value) == 0:
            continue
        return [key, sorted_set_value_pop(value, count, pop_operation, True, unnest_single_tuple=False)]

    return None


@command(b"zpopmax", {b"sortedset"})
class SortedSetPopMaximum(DatabaseCommand):
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter()
    count: int | None = keyword_parameter(default=None)

    def execute(self) -> ValueType:
        return sorted_set_key_pop(
            self.database,
            self.key,
            self.count if self.count is not None else 1,
            ScoredSortedSet.pop_maximum,
            self.client_context.protocol == RespProtocolVersion.RESP3,
            unnest_single_tuple=self.count is None,
        )


@command(b"zpopmin", {b"sortedset"})
class SortedSetPopMinimum(DatabaseCommand):
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter()
    count: int | None = keyword_parameter(default=None)

    def execute(self) -> ValueType:
        return sorted_set_key_pop(
            self.database,
            self.key,
            self.count if self.count is not None else 1,
            ScoredSortedSet.pop_minimum,
            self.client_context.protocol == RespProtocolVersion.RESP3,
            unnest_single_tuple=self.count is None,
        )


@command(b"bzpopmax", {b"sortedset"})
class SortedSetBlockingPopMaximum(DatabaseCommand):
    client_context: ClientContext = dependency()
    blocking_manager: SortedSetBlockingManager = dependency()

    keys: list[bytes] = positional_parameter()
    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return ArrayNone

        result = sorted_set_key_pop(
            self.database,
            self._key,
            1,
            ScoredSortedSet.pop_maximum,
            self.client_context.protocol == RespProtocolVersion.RESP3,
        )
        result.insert(0, self._key)
        return result


@command(b"bzpopmin", {b"sortedset"})
class SortedSetBlockingPopMinimum(DatabaseCommand):
    client_context: ClientContext = dependency()
    blocking_manager: SortedSetBlockingManager = dependency()

    keys: list[bytes] = positional_parameter()
    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return ArrayNone

        result = sorted_set_key_pop(
            self.database,
            self._key,
            1,
            ScoredSortedSet.pop_minimum,
            self.client_context.protocol == RespProtocolVersion.RESP3,
        )
        result.insert(0, self._key)
        return result


@command(
    b"zadd", {b"write", b"sortedset", b"fast"}, metadata={CommandMetadata.PARAMETERS_LEFT_ERROR: b"ERR syntax error"}
)
class SortedSetAdd(DatabaseCommand):
    blocking_manager: SortedSetBlockingManager = dependency()

    key: bytes = positional_parameter()
    add_mode: AddMode = keyword_parameter(
        default=AddMode.ALL, flag={b"XX": AddMode.UPDATE_ONLY, b"NX": AddMode.INSERT_ONLY}
    )
    score_update: ScoreUpdateMode = keyword_parameter(
        default=ScoreUpdateMode.ALL, flag={b"LT": ScoreUpdateMode.LESS_THAN, b"GT": ScoreUpdateMode.GREATER_THAN}
    )
    return_changed_elements: bool = keyword_parameter(flag=b"CH")
    increment_mode: bool = keyword_parameter(flag=b"INCR")
    scores_members: list[tuple[float, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        if self.add_mode == AddMode.INSERT_ONLY and self.score_update != ScoreUpdateMode.ALL:
            raise ServerError(b"ERR GT, LT, and/or NX options at the same time are not compatible")

        if self.increment_mode and len(self.scores_members) != 1:
            raise ServerError(b"ERR INCR option supports a single increment-element pair")

        value = self.database.sorted_set_database.get_value_or_create(self.key)

        length_before = len(value)
        changed_elements = 0
        aborted = False
        for score, member in self.scores_members:
            if math.isnan(score):
                raise ServerError(b"ERR value is not a valid float")
            if self.add_mode == AddMode.UPDATE_ONLY and member not in value.members_scores:
                continue
            if self.add_mode == AddMode.INSERT_ONLY and member in value.members_scores:
                continue
            current_score = value.members_scores.get(member, None)
            new_score = score
            if self.increment_mode:
                new_score = (current_score or 0) + score

            if (
                self.score_update == ScoreUpdateMode.GREATER_THAN
                and current_score is not None
                and new_score <= current_score
            ) or (
                self.score_update == ScoreUpdateMode.LESS_THAN
                and current_score is not None
                and new_score >= current_score
            ):
                aborted = True
                continue

            if current_score != new_score:
                changed_elements += 1
            value.add(new_score, member)
        if aborted and changed_elements == 0:
            return None
        if self.return_changed_elements:
            return changed_elements
        return len(value) - length_before

    async def after(self, in_multi: bool = False) -> None:
        await self.blocking_manager.notify(self.key, in_multi=in_multi)


class PopModifier(Enum):
    MIN = b"MIN"
    MAX = b"MAX"


POP_MODIFIER_TO_OPERATION: dict[PopModifier, Callable[[ScoredSortedSet], tuple[float, bytes]]] = {
    PopModifier.MIN: ScoredSortedSet.pop_minimum,
    PopModifier.MAX: ScoredSortedSet.pop_maximum,
}


@command(b"zmpop", {b"sortedset"})
class SortedSetMultiplePop(Command):
    database: Database = dependency()

    numkeys: int = positional_parameter(parse_error=b"ERR numkeys should be greater than 0")
    keys: list[bytes] = positional_parameter(length_field_name="numkeys")
    pop_modifier: PopModifier = positional_parameter()
    count: int | None = keyword_parameter(
        token=b"COUNT", default=None, parse_error=b"ERR count should be greater than 0"
    )

    def execute(self) -> ValueType:
        result = sorted_set_multikey_pop(
            self.database,
            self.keys,
            self.count if self.count is not None else 1,
            POP_MODIFIER_TO_OPERATION[self.pop_modifier],
        )
        return result if result is not None else ArrayNone


@command(b"bzmpop", {b"sortedset"})
class SortedSetBlockingMultiplePop(Command):
    client_context: ClientContext = dependency()
    blocking_manager: SortedSetBlockingManager = dependency()
    database: Database = dependency()

    timeout: float = positional_parameter(parse_error=b"ERR timeout is out of range")
    numkeys: int = positional_parameter(parse_error=b"ERR numkeys should be greater than 0")
    keys: list[bytes] = positional_parameter(length_field_name="numkeys")
    pop_modifier: PopModifier = positional_parameter()
    count: int | None = keyword_parameter(
        token=b"COUNT", default=None, parse_error=b"ERR count should be greater than 0"
    )

    _key: bytes | None = field(default=None, init=False)

    async def before(self, in_multi: bool = False) -> None:
        if self.count is not None and self.count < 1:
            raise ServerError(b"ERR count should be greater than 0")

        self._key = await self.blocking_manager.wait_for_lists(
            self.client_context, self.keys, self.timeout, in_multi=in_multi
        )

    def execute(self) -> ValueType:
        if self._key is None:
            return ArrayNone

        result = sorted_set_key_pop(
            self.database,
            self._key,
            self.count if self.count is not None else 1,
            POP_MODIFIER_TO_OPERATION[self.pop_modifier],
            True,
            unnest_single_tuple=False,
        )
        return [self._key, result]


@command(b"zrange", {b"read", b"sortedset", b"slow"})
class SortedSetRange(DatabaseCommand):
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter()
    start: bytes = positional_parameter()
    stop: bytes = positional_parameter()
    range_mode: RangeMode = keyword_parameter(
        default=RangeMode.BY_INDEX, flag={b"BYSCORE": RangeMode.BY_SCORE, b"BYLEX": RangeMode.BY_LEX}
    )
    rev: bool = keyword_parameter(flag=b"REV")
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES")

    def execute(self) -> ValueType:
        return sorted_set_range(
            self.database,
            self.key,
            self.start,
            self.stop,
            self.with_scores,
            self.range_mode,
            self.rev,
            self.limit,
            nested_member_score=self.client_context.protocol == RespProtocolVersion.RESP3,
        )


@command(b"zrangestore", {b"write", b"sortedset", b"slow"})
class SortedSetRangeStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    key: bytes = positional_parameter()
    start: bytes = positional_parameter()
    stop: bytes = positional_parameter()
    range_mode: RangeMode = keyword_parameter(
        default=RangeMode.BY_INDEX, flag={b"BYSCORE": RangeMode.BY_SCORE, b"BYLEX": RangeMode.BY_LEX}
    )
    rev: bool = keyword_parameter(flag=b"REV")
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self) -> ValueType:
        return sorted_set_range(
            self.database,
            self.key,
            self.start,
            self.stop,
            False,
            self.range_mode,
            self.rev,
            self.limit,
            self.destination,
        )


@command(b"zrevrange", {b"read", b"sortedset", b"slow"})
class SortedSetReversedRange(DatabaseCommand):
    key: bytes = positional_parameter()
    start: bytes = positional_parameter()
    stop: bytes = positional_parameter()
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES")

    def execute(self) -> ValueType:
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.start,
            stop=self.stop,
            with_scores=self.with_scores,
            is_reversed=True,
        )


@command(b"zrangebyscore", {b"read", b"sortedset", b"slow"})
class SortedSetRangeByScore(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES")
    limit: RangeLimit | None = keyword_parameter(default=None, token=b"LIMIT")

    def execute(self) -> ValueType:
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            with_scores=self.with_scores,
            limit=self.limit,
            range_mode=RangeMode.BY_SCORE,
        )


@command(b"zrevrangebyscore", {b"read", b"sortedset", b"slow"})
class SortedSetReversedRangeByScore(DatabaseCommand):
    key: bytes = positional_parameter()
    max: bytes = positional_parameter()
    min: bytes = positional_parameter()
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES")
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self) -> ValueType:
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.max,
            stop=self.min,
            with_scores=self.with_scores,
            limit=self.limit,
            is_reversed=True,
            range_mode=RangeMode.BY_SCORE,
        )


@command(b"zrangebylex", {b"read", b"sortedset", b"slow"})
class SortedSetRangeByLexical(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self) -> ValueType:
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            limit=self.limit,
            range_mode=RangeMode.BY_LEX,
        )


@command(b"zrevrangebylex", {b"read", b"sortedset", b"slow"})
class SortedSetReversedRangeByLexical(DatabaseCommand):
    key: bytes = positional_parameter()
    max: bytes = positional_parameter()
    min: bytes = positional_parameter()
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self) -> ValueType:
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.max,
            stop=self.min,
            limit=self.limit,
            is_reversed=True,
            range_mode=RangeMode.BY_LEX,
        )


@command(b"zcount", {b"read", b"sortedset", b"fast"})
class SortedSetCount(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()

    def execute(self) -> ValueType:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(self.min, self.max)

        key_value = self.database.sorted_set_database.get_or_create(self.key)

        return sum(
            1
            for _ in key_value.value.range_by_score(
                float(min_score), float(max_score), min_inclusive, max_inclusive, with_scores=False
            )
        )


@command(b"zcard", {b"read", b"sortedset", b"fast"})
class SortedSetCardinality(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.sorted_set_database.get_or_create(self.key).value.members)


@command(b"zscore", {b"read", b"sortedset", b"fast"})
class SortedSetMemberScore(DatabaseCommand):
    key: bytes = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        return value.members_scores.get(self.member, None)


@command(b"zmscore", {b"read", b"sortedset", b"fast"})
class SortedSetMultipleMemberScore(DatabaseCommand):
    key: bytes = positional_parameter()
    members: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        if len(self.members) == 0:
            raise ServerWrongNumberOfArgumentsError()

        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        result = []
        for member in self.members:
            result.append(value.members_scores.get(member, None))

        return result


@command(b"zincrby", {b"read", b"sortedset", b"fast"})
class SortedSetIncrementBy(DatabaseCommand):
    key: bytes = positional_parameter()
    increment: float = positional_parameter()
    member: bytes = positional_parameter()

    def execute(self) -> ValueType:
        if math.isnan(self.increment):
            raise ServerError(b"ERR value is not a valid float")

        value = self.database.sorted_set_database.get_value_or_create(self.key)

        old_score = value.members_scores.get(self.member, 0.0)
        new_score = old_score + self.increment

        if math.isnan(new_score):
            raise ServerError(b"ERR resulting score is not a number (NaN)")

        value.update((new_score, self.member))
        return new_score


@command(b"zrem", {b"read", b"sortedset", b"fast"})
class SortedSetRemove(DatabaseCommand):
    key: bytes = positional_parameter()
    members: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        removed_members = 0
        for member in self.members:
            try:
                value.remove(member)
                removed_members += 1
            except KeyError:
                pass

        return removed_members


@command(b"zrank", {b"read", b"sortedset", b"fast"})
class SortedSetRank(DatabaseCommand):
    key: bytes = positional_parameter()
    member: bytes = positional_parameter()
    with_score: bool = keyword_parameter(flag=b"WITHSCORE")

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        member_score = value.members_scores.get(self.member, None)

        if member_score is None:
            return None if not self.with_score else ArrayNone

        rank = value.members.index((member_score, self.member))

        if self.with_score:
            return [rank, member_score]
        return rank


@command(b"zrevrank", {b"read", b"sortedset", b"fast"})
class SortedSetReversedRank(DatabaseCommand):
    key: bytes = positional_parameter()
    member: bytes = positional_parameter()
    with_score: bool = keyword_parameter(flag=b"WITHSCORE")

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        member_score = value.members_scores.get(self.member, None)

        if member_score is None:
            return None if not self.with_score else ArrayNone

        rank = len(value.members) - 1 - value.members.index((member_score, self.member))

        if self.with_score:
            return [rank, member_score]
        return rank


@command(b"zlexcount", {b"read", b"sortedset", b"fast"})
class SortedSetLexicalCount(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()

    def execute(self) -> ValueType:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(self.min, self.max)

        key_value = self.database.sorted_set_database.get_or_create(self.key)

        return sum(
            1
            for _ in key_value.value.range_by_lexical(
                min_score, max_score, min_inclusive, max_inclusive, with_scores=False
            )
        )


@command(b"zremrangebyscore", {b"write", b"sortedset", b"fast"})
class SortedSetRemoveRangeByScore(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        members = sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            range_mode=RangeMode.BY_SCORE,
        )

        if isinstance(members, int):
            raise ValueError()

        removed_members = 0
        for member in members:
            try:
                value.remove(member)
                removed_members += 1
            except KeyError:
                pass

        return removed_members


@command(b"zremrangebyrank", {b"write", b"sortedset", b"fast"})
class SortedSetRemoveRangeByRank(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        members = sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            range_mode=RangeMode.BY_INDEX,
        )

        if isinstance(members, int):
            raise ValueError()

        removed_members = 0
        for member in members:
            try:
                value.remove(member)
                removed_members += 1
            except KeyError:
                pass

        return removed_members


@command(b"zremrangebylex", {b"write", b"sortedset", b"fast"})
class SortedSetRemoveRangeByLexical(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_empty(self.key)

        members = sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            range_mode=RangeMode.BY_LEX,
        )

        if isinstance(members, int):
            raise ValueError()

        removed_members = 0
        for member in members:
            try:
                value.remove(member)
                removed_members += 1
            except KeyError:
                pass

        return removed_members


def sorted_set_store_operation(
    database: Database,
    operation: Callable[..., ScoredSortedSet],
    keys: list[bytes],
    weights: list[float] | None = None,
    cardinality_limit: int = 0,
    aggregate_mode: AggregateMode = AggregateMode.SUM,
) -> ScoredSortedSet:
    new_set: ScoredSortedSet | None = None
    for key, weight in zip_longest(keys, weights or [], fillvalue=None):
        if key is None:
            raise ValueError()
        any_set = database.any_set_database.get_value_or_empty(key)
        if isinstance(any_set, set):
            value = ScoredSortedSet((member, weight or 1.0) for member in any_set)
        elif weight is not None and weight != 1.0:
            members_and_scores = []
            for score, member in any_set.members:
                new_score = score * weight
                if math.isnan(new_score):
                    new_score = 0.0
                members_and_scores.append((member, new_score))
            value = ScoredSortedSet(members_and_scores)
        else:
            value = any_set

        if new_set is None:
            new_set = value
        else:
            new_set = operation(new_set, value, score_operation=AGGREGATION_MODE_TO_OPERATOR[aggregate_mode])

        if 0 < cardinality_limit < len(new_set):
            return new_set

    if new_set is None:
        new_set = ScoredSortedSet()

    return new_set


class SortedSetOperation(Protocol):
    def __call__(
        self,
        myself: ScoredSortedSet,
        /,
        *others: ScoredSortedSet,
        score_operation: Callable[[float, float], float] = operator.add,
    ) -> ScoredSortedSet: ...


def apply_sorted_set_store_operation(
    database: Database,
    operation: SortedSetOperation,
    keys: list[bytes],
    destination: bytes,
    weights: list[float] | None = None,
    aggregate_mode: AggregateMode = AggregateMode.SUM,
) -> int:
    new_set = sorted_set_store_operation(database, operation, keys, weights, aggregate_mode=aggregate_mode)

    database.pop(destination, None)
    database.sorted_set_database.get_value_or_create(destination).update_from(new_set)

    return len(new_set)


@command(b"zunionstore", {b"write", b"set", b"slow"})
class SortedSetUnionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={
            "when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zunionstore' command"
        },
    )
    weights: list[float] | None = keyword_parameter(
        token=b"WEIGHTS", default=None, length_field_name="numkeys", parse_error=b"ERR weight value is not a float"
    )
    aggregate: AggregateMode = keyword_parameter(token=b"AGGREGATE", default=AggregateMode.SUM)

    def execute(self) -> ValueType:
        return apply_sorted_set_store_operation(
            self.database,
            ScoredSortedSet.union,
            self.keys,
            self.destination,
            self.weights,
            aggregate_mode=self.aggregate,
        )


def apply_sorted_set_operation(
    database: Database,
    operation: Callable[[ScoredSortedSet, ScoredSortedSet], ScoredSortedSet],
    keys: list[bytes],
    protocol: RespProtocolVersion,
    weights: list[float] | None = None,
    with_scores: bool = False,
    cardinality_limit: int = 0,
    aggregate_mode: AggregateMode = AggregateMode.SUM,
) -> list:
    new_set = sorted_set_store_operation(
        database, operation, keys, weights, cardinality_limit=cardinality_limit, aggregate_mode=aggregate_mode
    )

    if with_scores:
        if protocol == RespProtocolVersion.RESP3:
            return [[member, score] for score, member in new_set.members]
        return list(flatten([member, score] for score, member in new_set.members))
    return [member for score, member in new_set.members]


@command(b"zunion", {b"write", b"set", b"slow"})
class SortedSetUnion(DatabaseCommand):
    client_context: ClientContext = dependency()

    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={"when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zunion' command"},
    )
    weights: list[float] | None = keyword_parameter(
        token=b"WEIGHTS", default=None, length_field_name="numkeys", parse_error=b"ERR weight value is not a float"
    )
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES", default=False)
    aggregate: AggregateMode = keyword_parameter(token=b"AGGREGATE", default=AggregateMode.SUM)

    def execute(self) -> ValueType:
        return apply_sorted_set_operation(
            self.database,
            ScoredSortedSet.union,
            self.keys,
            self.client_context.protocol,
            self.weights,
            self.with_scores,
            aggregate_mode=self.aggregate,
        )


@command(b"zinter", {b"write", b"set", b"slow"})
class SortedSetIntersection(DatabaseCommand):
    client_context: ClientContext = dependency()

    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={"when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zinter' command"},
    )
    weights: list[float] | None = keyword_parameter(
        token=b"WEIGHTS", default=None, length_field_name="numkeys", parse_error=b"ERR weight value is not a float"
    )
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES", default=False)
    aggregate: AggregateMode = keyword_parameter(token=b"AGGREGATE", default=AggregateMode.SUM)

    def execute(self) -> ValueType:
        return apply_sorted_set_operation(
            self.database,
            ScoredSortedSet.intersection,
            self.keys,
            self.client_context.protocol,
            self.weights,
            self.with_scores,
            aggregate_mode=self.aggregate,
        )


@command(b"zinterstore", {b"write", b"set", b"slow"})
class SortedSetIntersectionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={
            "when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zinterstore' command"
        },
    )
    weights: list[float] | None = keyword_parameter(
        token=b"WEIGHTS", default=None, length_field_name="numkeys", parse_error=b"ERR weight value is not a float"
    )
    aggregate: AggregateMode = keyword_parameter(token=b"AGGREGATE", default=AggregateMode.SUM)

    def execute(self) -> ValueType:
        return apply_sorted_set_store_operation(
            self.database,
            ScoredSortedSet.intersection,
            self.keys,
            self.destination,
            self.weights,
            aggregate_mode=self.aggregate,
        )


@command(b"zdiffstore", {b"write", b"set", b"slow"})
class SortedSetDifferenceStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={
            "when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zdiffstore' command"
        },
    )
    weights: list[float] | None = keyword_parameter(
        token=b"WEIGHTS", default=None, length_field_name="numkeys", parse_error=b"ERR weight value is not a float"
    )
    aggregate: AggregateMode = keyword_parameter(token=b"AGGREGATE", default=AggregateMode.SUM)

    def execute(self) -> ValueType:
        return apply_sorted_set_store_operation(
            self.database,
            ScoredSortedSet.difference,
            self.keys,
            self.destination,
            self.weights,
            aggregate_mode=self.aggregate,
        )


@command(b"zintercard", {b"write", b"set", b"slow"})
class SortedSetIntersectionCardinality(DatabaseCommand):
    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={
            "when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zintercard' command"
        },
    )
    limit: int = keyword_parameter(token=b"LIMIT", default=0, parse_error=b"ERR LIMIT can't be negative")

    def execute(self) -> ValueType:
        if self.limit < 0:
            raise ServerError(b"ERR LIMIT can't be negative")

        cardinality = len(
            apply_sorted_set_operation(
                self.database,
                ScoredSortedSet.intersection,
                self.keys,
                protocol=RespProtocolVersion.RESP2,
                cardinality_limit=self.limit,
            )
        )
        return cardinality if self.limit == 0 else min(cardinality, self.limit)


@command(b"zdiff", {b"write", b"set", b"slow"})
class SortedSetDifference(DatabaseCommand):
    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(
        length_field_name="numkeys",
        errors={"when_length_field_less_then_parameters": b"ERR at least 1 input key is needed for 'zdiff' command"},
    )
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES", default=False)

    def execute(self) -> ValueType:
        return apply_sorted_set_operation(
            self.database,
            ScoredSortedSet.difference,
            self.keys,
            protocol=RespProtocolVersion.RESP2,
            with_scores=self.with_scores,
        )


@command(b"zrandmember", {b"write", b"string", b"slow"})
class SortedSetRandomMember(DatabaseCommand):
    client_context: ClientContext = dependency()

    key: bytes = positional_parameter()
    count: int | None = positional_parameter(default=None)
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES", default=False)

    def execute(self) -> ValueType:
        value = self.database.sorted_set_database.get_value_or_none(self.key)

        if self.count is None:
            if value is None:
                return None
            score, member = random.choice(list(value.members))
            if self.with_scores:
                return [member, score]
            return member

        if not (-LONG_MAX < self.count < LONG_MAX):
            raise ServerError(f"ERR value is out of range, value must between {-LONG_MAX} and {LONG_MAX}".encode())

        if self.with_scores and not ((-LONG_MAX / 2) < self.count < (LONG_MAX / 2)):
            raise ServerError(b"ERR value is out of range")

        if value is None:
            return []

        score_members = list(value.members)

        result: list = []
        if self.count < 0:
            for _ in range(abs(self.count)):
                score, member = random.choice(score_members)
                if self.with_scores and self.client_context.protocol == RespProtocolVersion.RESP3:
                    result.append([member, score])
                elif self.with_scores:
                    result.append(member)
                    result.append(score)
                else:
                    result.append(member)
        else:
            for _ in range(self.count):
                if not score_members:
                    break

                score, member = score_members.pop(random.randrange(len(score_members)))
                if self.with_scores and self.client_context.protocol == RespProtocolVersion.RESP3:
                    result.append([member, score])
                elif self.with_scores:
                    result.append(member)
                    result.append(score)
                else:
                    result.append(member)
        return result
