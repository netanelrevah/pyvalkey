def to_bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()
