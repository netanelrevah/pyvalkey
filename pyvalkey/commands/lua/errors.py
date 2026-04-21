from lua_runtime import LuaError  # ty: ignore[unresolved-import]


class LuaServerError(LuaError):
    def __init__(self, message: bytes = b"") -> None:
        super().__init__(message.decode("utf-8", errors="replace"))
        self.message = message
