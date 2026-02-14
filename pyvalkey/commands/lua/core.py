from dataclasses import dataclass, field
from typing import Any


@dataclass
class RegisteredFunction:
    function_name: bytes
    library_name: bytes
    compiled_function: Any | None
    readonly_compiled_function: Any | None
    flags: list[bytes]
    description: Any | None = None


@dataclass
class RegisteredLibrary:
    name: bytes
    code: bytes
    engine: bytes
    functions: dict[bytes, RegisteredFunction] = field(default_factory=dict)
