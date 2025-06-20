import functools
import math
from collections.abc import Callable
from enum import Enum

from pyvalkey.commands.core import Command
from pyvalkey.commands.parameters import (
    keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.router import command
from pyvalkey.commands.string_commands import DatabaseCommand
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.database_objects.databases import (
    MAX_BYTES,
    Database,
    RangeLimit,
    ValkeySortedSet,
)
from pyvalkey.database_objects.errors import ServerError
from pyvalkey.resp import RESP_OK, ArrayNone, ValueType


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
) -> int | list:
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
            with_scores=with_scores,
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
            with_scores=with_scores,
            is_reversed=is_reversed,
            limit=limit,
        )
    else:
        result_iterator = z.range(
            parse_range_parameters(int(start), int(stop), is_reversed=is_reversed), with_scores=with_scores
        )

    if destination:
        key_value = database.sorted_set_database.get_or_none(destination)

        if key_value is not None:
            destination_sorted_set = key_value.value
        else:
            destination_sorted_set = ValkeySortedSet()
        destination_sorted_set.update_with_iterator(result_iterator)
        return len(destination_sorted_set)
    return list(result_iterator)


class AddMode(Enum):
    ALL = None
    UPDATE_ONLY = b"XX"
    INSERT_ONLY = b"NX"


class ScoreUpdateMode(Enum):
    ALL = None
    LESS_THAN = b"LT"
    GREATER_THAN = b"GT"


@command(b"bzpopmax", {b"sortedset"})
class SortedSetBlockingPopMaximum(DatabaseCommand):
    keys: list[bytes] = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        pass


@command(b"bzpopmin", {b"sortedset"})
class SortedSetBlockingPopMinimum(DatabaseCommand):
    keys: list[bytes] = positional_parameter()
    timeout: int = positional_parameter()

    def execute(self) -> ValueType:
        pass


@command(b"zadd", {b"write", b"sortedset", b"fast"})
class SortedSetAdd(DatabaseCommand):
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


class PopModifier(Enum):
    MIN = b"MIN"
    MAX = b"MAX"


@command(b"zmpop", {b"sortedset"})
class SortedSetPop(Command):
    numkeys: int = positional_parameter()
    keys: list[bytes] = positional_parameter(length_field_name="numkeys")
    pop_modifier: PopModifier = positional_parameter()
    count: int | None = keyword_parameter(token=b"COUNT", default=None)

    def execute(self) -> ValueType:
        return RESP_OK


@command(b"zrange", {b"read", b"sortedset", b"slow"})
class SortedSetRange(DatabaseCommand):
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

        removed_memebers = 0
        for member in self.members:
            try:
                value.remove(member)
                removed_memebers += 1
            except KeyError:
                pass

        return removed_memebers


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

        removed_memebers = 0
        for member in members:
            try:
                value.remove(member)
                removed_memebers += 1
            except KeyError:
                pass

        return removed_memebers


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

        removed_memebers = 0
        for member in members:
            try:
                value.remove(member)
                removed_memebers += 1
            except KeyError:
                pass

        return removed_memebers


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

        removed_memebers = 0
        for member in members:
            try:
                value.remove(member)
                removed_memebers += 1
            except KeyError:
                pass

        return removed_memebers


def apply_set_store_operation(
    database: Database,
    operation: Callable[[ValkeySortedSet, ValkeySortedSet], ValkeySortedSet],
    keys: list[bytes],
    destination: bytes,
) -> int:
    new_set: ValkeySortedSet = functools.reduce(operation, map(database.sorted_set_database.get_value_or_empty, keys))
    database.pop(destination, None)
    database.sorted_set_database.get_value_or_create(destination).update_from(new_set)
    return len(new_set)


@command(b"zunionstore", {b"write", b"set", b"slow"})
class SortedSetUnionStore(DatabaseCommand):
    destination: bytes = positional_parameter()
    keys: list[bytes] = positional_parameter()

    def execute(self) -> ValueType:
        return apply_set_store_operation(self.database, ValkeySortedSet.union, self.keys, self.destination)


def apply_set_operation(
    database: Database, operation: Callable[[ValkeySortedSet, ValkeySortedSet], ValkeySortedSet], keys: list[bytes]
) -> list:
    return functools.reduce(operation, map(database.sorted_set_database.get_value_or_empty, keys))  # type: ignore[arg-type]


@command(b"zunion", {b"write", b"set", b"slow"})
class SortedSetStore(DatabaseCommand):
    numkeys: int = positional_parameter(parse_error=b"ERR numkeys should be greater than 0")
    keys: list[bytes] = positional_parameter(length_field_name="numkeys")

    def execute(self) -> ValueType:
        return apply_set_operation(self.database, ValkeySortedSet.union, self.keys)
