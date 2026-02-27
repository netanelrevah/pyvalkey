from lupa.lua51 import LuaError


class LuaServerError(LuaError):
    def __init__(self, message: bytes = b"") -> None:
        super().__init__(message)
        self.message = message
