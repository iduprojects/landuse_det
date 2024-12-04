"""Main landuse handlers are defined here."""

from fastapi import Depends, Path, Request, Security
from fastapi.security import HTTPBearer
from starlette import status

from landuse_api.dto import UserDTO
from landuse_api.logic import LanduseService
from landuse_api.utils import get_user

from ..schemas import GeoJSON, Profile
from .routers import landuse_router


@landuse_router.get(
    "/projects/{project_id}/renovation_potential",
    response_model=GeoJSON,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_projects_renovation_potential(
    request: Request,
    profile: Profile,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> GeoJSON:
    """Calculate renovation potential for project."""

    landuse_service: LanduseService = request.state.landuse_service

    return await landuse_service.get_renovation_potential(project_id, profile, user.id)


@landuse_router.get(
    "/projects/{project_id}/urbanization_level",
    response_model=GeoJSON,
    status_code=status.HTTP_200_OK,
    dependencies=[Security(HTTPBearer())],
)
async def get_projects_urbanization_level(
    request: Request,
    project_id: int = Path(..., description="project identifier"),
    user: UserDTO = Depends(get_user),
) -> GeoJSON:
    """Calculate urbanization level for project."""
    landuse_service: LanduseService = request.state.landuse_service

    return await landuse_service.get_urbanization_level(project_id, user.id)
