import time


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def now_s() -> int:
    return time.time_ns() // 1_000_000_000


def now_us() -> int:
    return time.time_ns() // 1_000


def now_f_s() -> float:
    return time.time_ns() / 1_000_000_000
