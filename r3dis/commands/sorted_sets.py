import math
from enum import Enum

from r3dis.commands.databases import DatabaseCommand
from r3dis.commands.parameters import (
    redis_keyword_parameter,
    redis_positional_parameter,
)
from r3dis.commands.router import RedisCommandsRouter
from r3dis.commands.utils import parse_range_parameters
from r3dis.consts import Commands
from r3dis.databases import (
    MAX_STRING,
    Database,
    RangeLimit,
    RedisMaxString,
    RedisSortedSet,
)

sorted_set_commands_router = RedisCommandsRouter()


def parse_score_parameter(score: bytes) -> tuple[bytes | RedisMaxString | float, bool]:
    result_score: bytes | RedisMaxString | float
    min_inclusive = True
    if score == b"-":
        result_score = b""
    elif score == b"+":
        result_score = MAX_STRING
    elif score == b"-inf":
        result_score = -math.inf
    elif score == b"+inf":
        result_score = -math.inf
    elif b"(" in score:
        result_score = score[1:]
        min_inclusive = False
    elif b"[" in score:
        result_score = score[1:]
    else:
        result_score = score

    return result_score, min_inclusive


def parse_ordered_range_parameters(min_score: bytes, max_score: bytes):
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
):
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
        database[destination] = RedisSortedSet()
        database[destination].update_with_iterator(result_iterator)
        return len(database[destination])
    return list(result_iterator)


class AddMode(Enum):
    ALL = None
    UPDATE_ONLY = b"XX"
    INSERT_ONLY = b"NX"


class ScoreUpdateMode(Enum):
    ALL = None
    LESS_THAN = b"LT"
    GREATER_THAN = b"GT"


@sorted_set_commands_router.command(Commands.SortedSetAdd)
class SortedSetAdd(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    add_mode: AddMode = redis_keyword_parameter(
        default=AddMode.ALL, flag={b"XX": AddMode.UPDATE_ONLY, b"NX": AddMode.INSERT_ONLY}
    )
    score_update: ScoreUpdateMode = redis_keyword_parameter(
        default=ScoreUpdateMode.ALL, flag={b"LT": ScoreUpdateMode.LESS_THAN, b"GT": ScoreUpdateMode.GREATER_THAN}
    )
    return_changed_elements: bool = redis_keyword_parameter(flag=b"CH")
    increment_mode: bool = redis_keyword_parameter(flag=b"INCR")
    scores_members: list[tuple[int, bytes]] = redis_positional_parameter()

    def execute(self):
        z = self.database.get_or_create_sorted_set(self.key)
        length_before = len(z)
        for score, member in self.scores_members:
            z.add(score, member)
        return len(z) - length_before


@sorted_set_commands_router.command(Commands.SortedSetRange)
class SortedSetRange(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    start: bytes = redis_positional_parameter()
    stop: bytes = redis_positional_parameter()
    range_mode: RangeMode = redis_keyword_parameter(
        default=RangeMode.BY_INDEX, flag={b"BYSCORE": RangeMode.BY_SCORE, b"BYLEX": RangeMode.BY_LEX}
    )
    rev: bool = redis_keyword_parameter(flag=b"REV")
    limit: RangeLimit | None = redis_keyword_parameter(default=None, flag=b"LIMIT")
    with_scores: bool = redis_keyword_parameter(flag=b"WITHSCORES")

    def execute(self):
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


@sorted_set_commands_router.command(Commands.SortedSetRangeStore)
class SortedSetRangeStore(DatabaseCommand):
    destination: bytes = redis_positional_parameter()
    key: bytes = redis_positional_parameter()
    start: bytes = redis_positional_parameter()
    stop: bytes = redis_positional_parameter()
    range_mode: RangeMode = redis_keyword_parameter(
        default=RangeMode.BY_INDEX, flag={b"BYSCORE": RangeMode.BY_SCORE, b"BYLEX": RangeMode.BY_LEX}
    )
    rev: bool = redis_keyword_parameter(flag=b"REV")
    limit: RangeLimit | None = redis_keyword_parameter(default=None, flag=b"LIMIT")
    with_scores: bool = redis_keyword_parameter(flag=b"WITHSCORES")

    def execute(self):
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


@sorted_set_commands_router.command(Commands.SortedSetReversedRange)
class SortedSetReversedRange(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    start: bytes = redis_positional_parameter()
    stop: bytes = redis_positional_parameter()
    with_scores: bool = redis_keyword_parameter(flag=b"WITHSCORES")

    def execute(self):
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.start,
            stop=self.stop,
            with_scores=self.with_scores,
        )


@sorted_set_commands_router.command(Commands.SortedSetRangeByScore)
class SortedSetRangeByScore(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    min: bytes = redis_positional_parameter()
    max: bytes = redis_positional_parameter()
    with_scores: bool = redis_keyword_parameter(flag=b"WITHSCORES")
    limit: RangeLimit | None = redis_keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self):
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            with_scores=self.with_scores,
            limit=self.limit,
            range_mode=RangeMode.BY_SCORE,
        )


@sorted_set_commands_router.command(Commands.SortedSetReversedRangeByScore)
class SortedSetReversedRangeByScore(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    max: bytes = redis_positional_parameter()
    min: bytes = redis_positional_parameter()
    with_scores: bool = redis_keyword_parameter(flag=b"WITHSCORES")
    limit: RangeLimit | None = redis_keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self):
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


@sorted_set_commands_router.command(Commands.SortedSetRangeByLexical)
class SortedSetRangeByLexical(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    min: bytes = redis_positional_parameter()
    max: bytes = redis_positional_parameter()
    limit: RangeLimit | None = redis_keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self):
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            limit=self.limit,
            range_mode=RangeMode.BY_LEX,
        )


@sorted_set_commands_router.command(Commands.SortedSetReversedRangeByLexical)
class SortedSetReversedRangeByLexical(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    max: bytes = redis_positional_parameter()
    min: bytes = redis_positional_parameter()
    limit: RangeLimit | None = redis_keyword_parameter(default=None, flag=b"LIMIT")

    def execute(self):
        return sorted_set_range(
            database=self.database,
            key=self.key,
            start=self.min,
            stop=self.max,
            limit=self.limit,
            is_reversed=True,
            range_mode=RangeMode.BY_LEX,
        )


@sorted_set_commands_router.command(Commands.SortedSetCount)
class SortedSetCount(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    min: bytes = redis_positional_parameter()
    max: bytes = redis_positional_parameter()

    def execute(self):
        min_score, min_inclusive, max_score, max_inclusive = parse_ordered_range_parameters(self.min, self.max)

        z = self.database.get_or_create_sorted_set(self.key)

        return sum(
            1
            for _ in z.range_by_score(
                float(min_score), float(max_score), min_inclusive, max_inclusive, with_scores=False
            )
        )


@sorted_set_commands_router.command(Commands.SortedSetCardinality)
class SortedSetCardinality(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def handle(self):
        return len(self.database.get_or_create_sorted_set(self.key).members)
