class ServerException(Exception):
    def __init__(self, message: bytes = b""):
        super().__init__(message)
        self.message = message


class NoPermissionError(ServerException):
    pass


class KeyPermissionError(NoPermissionError):
    def __init__(self) -> None:
        super().__init__(b"NOPERM No permissions to access a key")


class CommandPermissionError(NoPermissionError):
    def __init__(self, command_name: bytes) -> None:
        self.command_name = command_name


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
