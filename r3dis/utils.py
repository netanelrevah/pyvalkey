def to_bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
