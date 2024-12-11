"""Landuse handlers logic."""

from landuse_app.schemas import GeoJSON, Profile

from .helpers import (
    get_projects_context_renovation_potential,
    get_projects_context_urbanization_level,
    get_projects_renovation_potential,
    get_projects_urbanization_level,
)


class LanduseService:
    """Landuse service."""

    async def get_renovation_potential(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate renovation potential for project."""
        return await get_projects_renovation_potential(project_id, profile)

    async def get_urbanization_level(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate urbanization level for project."""
        return await get_projects_urbanization_level(project_id, profile)

    async def get_context_renovation_potential(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate renovation potential for project's context."""
        return await get_projects_context_renovation_potential(project_id, profile)

    async def get_context_urbanization_level(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate urbanization level for project's context."""
        return await get_projects_context_urbanization_level(project_id, profile)


landuse_service = LanduseService()
