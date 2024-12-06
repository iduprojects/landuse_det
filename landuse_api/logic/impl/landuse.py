"""Landuse handlers implementation."""

from sqlalchemy.ext.asyncio import AsyncConnection

from ...schemas import GeoJSON, Profile
from ..helpers import get_projects_renovation_potential, get_projects_urbanization_level
from ..landuse import LanduseService


class LanduseServiceImpl(LanduseService):
    """Landuse service implementation."""

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_renovation_potential(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate renovation potential for project."""
        return await get_projects_renovation_potential(project_id, profile)

    async def get_urbanization_level(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate urbanization level for project."""
        return await get_projects_urbanization_level(project_id, profile)
