class RedisException(Exception):
    def __init__(self, message: bytes = b""):
        super().__init__(message)
        self.message = message


class RouterKeyError(RedisException):
    pass


class RedisWrongType(RedisException):
    pass


class RedisSyntaxError(RedisException):
    pass


class RedisInvalidIntegerError(RedisException):
    pass


class RedisWrongNumberOfArguments(RedisException):
    pass
