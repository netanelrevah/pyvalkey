import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterator

from r3dis.commands.database.core import DatabaseCommand
from r3dis.commands.handlers import CommandHandler
from r3dis.commands.parsers import redis_keyword_parameter, redis_positional_parameter
from r3dis.commands.utils import parse_range_parameters
from r3dis.databases import (
    MAX_STRING,
    Database,
    RangeLimit,
    RedisMaxString,
    RedisSortedSet,
)


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


@dataclass
class SortedSetAdd(DatabaseCommand):
    key: bytes = redis_positional_parameter()
    add_mode: RangeMode = redis_keyword_parameter(
        default=AddMode.ALL, flag={b"XX": AddMode.UPDATE_ONLY, b"NX": AddMode.INSERT_ONLY}
    )
    score_update: RangeMode = redis_keyword_parameter(
        default=ScoreUpdateMode.ALL, flag={b"LT": ScoreUpdateMode.LESS_THAN, b"GT": ScoreUpdateMode.GREATER_THAN}
    )
    return_changed_elements: bool = redis_keyword_parameter(flag=b"CH")
    increment_mode: bool = redis_keyword_parameter(flag=b"INCR")
    score_member: list[tuple[int, bytes]] = redis_keyword_parameter()


@dataclass
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


@dataclass
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


@dataclass
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


@dataclass
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


@dataclass
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


@dataclass
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


@dataclass
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


@dataclass
class SortedSetAdd2(CommandHandler):
    def handle(self, key: bytes, scores_members: Iterator[tuple[float, bytes]]):
        z = self.database.get_or_create_sorted_set(key)
        length_before = len(z)
        for score, member in scores_members:
            z.add(score, member)
        return len(z) - length_before

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)
        scores_members = []
        while parameters:
            score = float(parameters.pop(0))
            member = parameters.pop(0)

            scores_members.append((score, member))

        return key, scores_members


@dataclass
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


@dataclass
class SortedSetCardinality(DatabaseCommand):
    key: bytes = redis_positional_parameter()

    def handle(self):
        return len(self.database.get_or_create_sorted_set(self.key).members)
