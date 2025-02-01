class ServerError(Exception):
    def __init__(self, message: bytes = b"") -> None:
        super().__init__(message)
        self.message = message


class NoPermissionError(ServerError):
    pass


class KeyPermissionError(NoPermissionError):
    def __init__(self) -> None:
        super().__init__(b"NOPERM No permissions to access a key")


class CommandPermissionError(NoPermissionError):
    def __init__(self, command_name: bytes) -> None:
        self.command_name = command_name


class RouterKeyError(ServerError):
    pass


class ServerWrongTypeError(ServerError):
    pass


class ValkeySyntaxError(ServerError):
    pass


class ServerWrongNumberOfArgumentsError(ServerError):
    pass
