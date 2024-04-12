class ServerException(Exception):
    def __init__(self, message: bytes = b""):
        super().__init__(message)
        self.message = message


class RouterKeyError(ServerException):
    pass


class ServerWrongType(ServerException):
    pass


class ServerSyntaxError(ServerException):
    pass


class ServerInvalidIntegerError(ServerException):
    pass


class ServerWrongNumberOfArguments(ServerException):
    pass
