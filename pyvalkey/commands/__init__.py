from __future__ import annotations

import importlib
import pkgutil

from pyvalkey.commands.dependencies import register_dependencies

register_dependencies()

_ = {name: importlib.import_module(f"{__name__}.{name}") for finder, name, _ in pkgutil.iter_modules(__path__)}
