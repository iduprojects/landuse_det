"""Exceptions module."""

from .auth import AuthServiceUnavailable, ExpiredToken, InvalidTokenSignature, JWTDecodeError
from .base import ApiError
from .common import AccessDeniedError, EntityNotFoundById

__all__ = [
    "ApiError",
    "ExpiredToken",
    "JWTDecodeError",
    "InvalidTokenSignature",
    "AuthServiceUnavailable",
    "AccessDeniedError",
    "EntityNotFoundById",
]
