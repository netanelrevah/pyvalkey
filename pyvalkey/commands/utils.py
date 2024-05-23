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
