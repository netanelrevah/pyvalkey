import math
from dataclasses import dataclass
from typing import Iterator

from r3dis.commands.core import CommandHandler
from r3dis.commands.utils import parse_range_parameters
from r3dis.databases import MAX_STRING, RangeLimit, RedisMaxString, RedisSortedSet
from r3dis.errors import RedisSyntaxError


def parse_score_parameter(score: bytes) -> tuple[bytes | RedisMaxString | float, bool]:
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


@dataclass
class SortedSetAdd(CommandHandler):
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
class SortedSetCardinality(CommandHandler):
    def handle(self, key: bytes):
        return len(self.database.get_or_create_sorted_set(key).members)

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)

        return key


@dataclass
class SortedSetCount(CommandHandler):
    def handle(self, key: bytes, min_score, min_inclusive, max_score, max_inclusive):
        z = self.database.get_or_create_sorted_set(key)

        return sum(
            1
            for _ in z.range_by_score(
                float(min_score), float(max_score), min_inclusive, max_inclusive, with_scores=False
            )
        )

    def parse(self, parameters: list[bytes]):
        key = parameters.pop(0)
        min_ = parameters.pop(0)
        max_ = parameters.pop(0)

        return key, *parse_ordered_range_parameters(min_, max_)


@dataclass
class SortedSetRange(CommandHandler):
    store: bool = False
    is_reversed: bool = False
    rev_allowed: bool = False
    limit_allowed: bool = False
    with_scores_allowed: bool = False
    bylex_allowed: bool = False
    byscore_allowed: bool = False

    def handle(
        self,
        key: bytes,
        range_slice,
        with_scores: bool,
        by_score: bool,
        by_lex: bool,
        is_reversed: bool,
        limit: RangeLimit,
        destination: bytes | None,
    ):
        z = self.database.get_sorted_set(key)

        if by_score:
            min_score, min_inclusive, max_score, max_inclusive = range_slice
            result_iterator = z.range_by_score(
                float(min_score),
                float(max_score),
                min_inclusive,
                max_inclusive,
                with_scores=with_scores,
                is_reversed=is_reversed,
                limit=limit,
            )
        elif by_lex:
            min_score, min_inclusive, max_score, max_inclusive = range_slice
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
            result_iterator = z.range(range_slice, with_scores=with_scores)

        if self.store:
            self.database[destination] = RedisSortedSet()
            self.database[destination].update_with_iterator(result_iterator)
            return len(self.database[destination])
        return list(result_iterator)

    def parse(self, parameters: list[bytes]):
        destination = None
        if self.store:
            destination = parameters.pop(0)
        key = parameters.pop(0)
        start = parameters.pop(0)
        stop = parameters.pop(0)

        by_score = False
        by_lex = False
        is_reversed = self.is_reversed
        limit = None
        with_scores = False
        while parameters:
            match parameters.pop(0):
                case b"WITHSCORES" if self.with_scores_allowed and not with_scores:
                    with_scores = True
                case b"REV" if self.rev_allowed and not is_reversed:
                    is_reversed = True
                case b"BYSCORE" if self.byscore_allowed and not (by_score or by_lex):
                    by_score = True
                case b"BYLEX" if self.bylex_allowed and not (by_score or by_lex):
                    by_lex = True
                case b"LIMIT" if self.limit_allowed and not limit:
                    limit = RangeLimit(
                        offset=int(parameters.pop(0)),
                        count=int(parameters.pop(0)),
                    )
                case _:
                    return RedisSyntaxError()

        if by_lex or by_score:
            range_slice = parse_ordered_range_parameters(start, stop)
        else:
            range_slice = parse_range_parameters(int(start), int(stop), is_reversed=is_reversed)
        return key, range_slice, with_scores, by_score, by_lex, is_reversed, limit, destination
