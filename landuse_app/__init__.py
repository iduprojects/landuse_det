"""Main module."""
from .config import config
from .fastapi_init import app


__all__ = [
    "app",
    "config",
]
