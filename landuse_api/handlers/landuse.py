"""Main landuse handlers are defined here."""

from fastapi import Path, Request
from starlette import status

from landuse_api.logic import LanduseService

from ..schemas import GeoJSON, Profile
from .routers import landuse_router


@landuse_router.get(
    "/projects/{project_id}/renovation_potential",
    response_model=GeoJSON,
    status_code=status.HTTP_200_OK,
)
async def get_projects_renovation_potential(
    request: Request,
    profile: Profile,
    project_id: int = Path(..., description="project identifier"),
) -> GeoJSON:
    """Calculate renovation potential for project."""

    landuse_service: LanduseService = request.state.landuse_service

    return await landuse_service.get_renovation_potential(project_id, profile)


@landuse_router.get(
    "/projects/{project_id}/urbanization_level",
    response_model=GeoJSON,
    status_code=status.HTTP_200_OK,
)
async def get_projects_urbanization_level(
    request: Request,
    profile: Profile,
    project_id: int = Path(..., description="project identifier"),
) -> GeoJSON:
    """Calculate urbanization level for project."""

    landuse_service: LanduseService = request.state.landuse_service

    return await landuse_service.get_urbanization_level(project_id, profile)
