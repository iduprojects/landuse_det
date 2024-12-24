"""Landuse handlers logic."""

from landuse_app.schemas import GeoJSON, Profile

from .helpers import (
    get_projects_context_renovation_potential,
    get_projects_context_urbanization_level,
    get_projects_renovation_potential,
    get_projects_urbanization_level,
)
from .helpers.renovation_potential import get_projects_landuse_parts_scen_id_main_method


class LanduseService:
    """Landuse service."""

    async def get_renovation_potential(self, project_id: int, profile: Profile) -> dict:
        """Calculate renovation potential for project."""
        return await get_projects_renovation_potential(project_id, profile)

    async def get_urbanization_level(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate urbanization level for project."""
        return await get_projects_urbanization_level(project_id, profile)

    async def get_context_renovation_potential(self, project_id: int, profile: Profile) -> dict:
        """Calculate renovation potential for project's context."""
        return await get_projects_context_renovation_potential(project_id, profile)

    async def get_context_urbanization_level(self, project_id: int, profile: Profile) -> GeoJSON:
        """Calculate urbanization level for project's context."""
        return await get_projects_context_urbanization_level(project_id, profile)

    async def get_project_landuse_parts(self, scenario_id: int) -> dict:
        """Calculate zone parts inside project's territory"""
        return await get_projects_landuse_parts_scen_id_main_method(scenario_id)

landuse_service = LanduseService()
