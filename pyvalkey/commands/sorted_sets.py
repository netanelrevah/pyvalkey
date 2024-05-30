from enum import Enum

from pyvalkey.commands.parameters import (
    keyword_parameter,
    positional_parameter,
)
from pyvalkey.commands.router import ServerCommandsRouter
from pyvalkey.commands.strings_commands import DatabaseCommand
from pyvalkey.commands.utils import parse_range_parameters
from pyvalkey.database_objects.databases import (
    MAX_BYTES,
    Database,
    RangeLimit,
)
from pyvalkey.resp import ValueType


def parse_score_parameter(score: bytes) -> tuple[bytes, bool]:
    result_score: bytes
    min_inclusive = True
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
        min_inclusive = False
    elif b"[" in score:
        result_score = score[1:]
    else:
        result_score = score

    return result_score, min_inclusive


def parse_ordered_range_parameters(min_score: bytes, max_score: bytes) -> tuple[bytes, bool, bytes, bool]:
    return parse_score_parameter(min_score) + parse_score_parameter(max_score)


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
    z = database.get_sorted_set(key)

    if range_mode == RangeMode.BY_SCORE:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(start, stop)
        result_iterator = z.range_by_score(
            float(min_score),
            float(max_score),
            min_inclusive,
            max_inclusive,
            with_scores=with_scores,
            is_reversed=is_reversed,
            limit=limit,
        )
    elif range_mode == RangeMode.BY_LEX:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(start, stop)
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
        dz = database.get_or_create_sorted_set(destination)
        dz.update_with_iterator(result_iterator)
        return len(dz)
    return list(result_iterator)


class AddMode(Enum):
    ALL = None
    UPDATE_ONLY = b"XX"
    INSERT_ONLY = b"NX"


class ScoreUpdateMode(Enum):
    ALL = None
    LESS_THAN = b"LT"
    GREATER_THAN = b"GT"


@ServerCommandsRouter.command(b"zadd", [b"write", b"sortedset", b"fast"])
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
    scores_members: list[tuple[int, bytes]] = positional_parameter()

    def execute(self) -> ValueType:
        z = self.database.get_or_create_sorted_set(self.key)
        length_before = len(z)
        for score, member in self.scores_members:
            z.add(score, member)
        return len(z) - length_before


@ServerCommandsRouter.command(b"zrange", [b"read", b"sortedset", b"slow"])
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


@ServerCommandsRouter.command(b"zrangestore", [b"write", b"sortedset", b"slow"])
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


@ServerCommandsRouter.command(b"zrevrange", [b"read", b"sortedset", b"slow"])
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
        )


@ServerCommandsRouter.command(b"zrangebyscore", [b"read", b"sortedset", b"slow"])
class SortedSetRangeByScore(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()
    with_scores: bool = keyword_parameter(flag=b"WITHSCORES")
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")

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


@ServerCommandsRouter.command(b"zrevrangebyscore", [b"read", b"sortedset", b"slow"])
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
            start=self.min,
            stop=self.max,
            with_scores=self.with_scores,
            limit=self.limit,
            is_reversed=True,
            range_mode=RangeMode.BY_SCORE,
        )


@ServerCommandsRouter.command(b"zrangebylex", [b"read", b"sortedset", b"slow"])
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


@ServerCommandsRouter.command(b"zrevrangebylex", [b"read", b"sortedset", b"slow"])
class SortedSetReversedRangeByLexical(DatabaseCommand):
    key: bytes = positional_parameter()
    max: bytes = positional_parameter()
    min: bytes = positional_parameter()
    limit: RangeLimit | None = keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self) -> ValueType:
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            limit=self.limit,
            is_reversed=True,
            range_mode=RangeMode.BY_LEX,
        )


@ServerCommandsRouter.command(b"zcount", [b"read", b"sortedset", b"fast"])
class SortedSetCount(DatabaseCommand):
    key: bytes = positional_parameter()
    min: bytes = positional_parameter()
    max: bytes = positional_parameter()

    def execute(self) -> ValueType:
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(self.min, self.max)

        z = self.database.get_or_create_sorted_set(self.key)

        return sum(
            1
            for _ in z.range_by_score(
                float(min_score), float(max_score), min_inclusive, max_inclusive, with_scores=False
            )
        )


@ServerCommandsRouter.command(b"zcard", [b"read", b"sortedset", b"fast"])
class SortedSetCardinality(DatabaseCommand):
    key: bytes = positional_parameter()

    def execute(self) -> ValueType:
        return len(self.database.get_or_create_sorted_set(self.key).members)
