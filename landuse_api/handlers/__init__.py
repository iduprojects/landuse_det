"""Handlers module."""

import importlib
from pathlib import Path

from .routers import routers

for file in sorted(Path(__file__).resolve().parent.iterdir()):
    if file.name.endswith(".py"):
        importlib.import_module(f".{file.name[:-3]}", __package__)

list_of_routes = [
    *routers,
]

__all__ = [
    "list_of_routes",
]
