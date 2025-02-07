import re

NUMERIC_REGEX = re.compile(r"^-?\d+(\.\d*)?$")
INTEGER_REGEX = re.compile(r"^-?\d+$")
FLOATING_POINT_REGEX = NUMERIC_REGEX


def is_numeric(value: bytes | str) -> bool:
    return NUMERIC_REGEX.match(value if isinstance(value, str) else value.decode()) is not None


def is_integer(value: bytes | str) -> bool:
    return INTEGER_REGEX.match(value if isinstance(value, str) else value.decode()) is not None


def is_floating_point(value: bytes | str) -> bool:
    str_value = value if isinstance(value, str) else value.decode()
    if str_value in ["+inf", "-inf", "inf"]:
        return True
    return FLOATING_POINT_REGEX.match(str_value) is not None


def parse_range_parameters(start: int, stop: int, is_reversed: bool = False) -> slice:
    if not is_reversed:
        python_start = start
        if stop == -1:
            python_stop = None
        else:
            python_stop = stop + 1
        return slice(python_start, python_stop)

    python_reversed_start = -(start + 1)
    if stop == -1:
        python_reversed_stop = None
    else:
        python_reversed_stop = -(stop + 2)
    return slice(python_reversed_start, python_reversed_stop, -1)
