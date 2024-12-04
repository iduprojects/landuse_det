"""Api routers are defined here."""

from fastapi import APIRouter

system_router = APIRouter(tags=["system"])
landuse_router = APIRouter(tags=["landuse"])

routers = [system_router, landuse_router]

__all__ = [
    "routers",
]
