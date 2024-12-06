"""Main module."""

from .__main__ import config
from .fastapi_init import app

__all__ = [
    "app",
    "config",
]
