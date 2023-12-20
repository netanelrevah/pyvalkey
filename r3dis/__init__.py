import importlib
import pkgutil

from r3dis import commands

_ = {
    name: importlib.import_module(f"{commands.__name__}.{name}")
    for finder, name, _ in pkgutil.iter_modules(commands.__path__)
}
