"""Api routers are defined here."""

from fastapi import APIRouter

system_router = APIRouter(tags=["system"])
landuse_router = APIRouter(tags=["landuse"])
renovation_router = APIRouter(tags=["renovation_potential"])
urbanization_router = APIRouter(tags=["urbanization_level"])
landuse_percentages_router = APIRouter(tags=["landuse_percentages"])
territories_urbanization_router = APIRouter(tags=["territories_urbanization"])

routers = [landuse_router, renovation_router, urbanization_router, landuse_percentages_router, territories_urbanization_router, system_router]

__all__ = [
    "routers",
]
