"""Landuse handlers logic."""

from landuse_app.schemas import GeoJSON

from .helpers import (
    get_projects_context_renovation_potential,
    get_projects_context_urbanization_level,
    get_projects_renovation_potential,
    get_projects_urbanization_level,
)
from .helpers.renovation_potential import get_projects_landuse_parts_scen_id_main_method
from .helpers.territories_urbanization import get_territory


class LanduseService:
    """Landuse service."""

    async def get_renovation_potential(self, project_id: int, source: str = None) -> dict:
        """Calculate renovation potential for project."""
        return await get_projects_renovation_potential(project_id, source)

    async def get_urbanization_level(self, project_id: int, source: str = None) -> GeoJSON:
        """Calculate urbanization level for project."""
        return await get_projects_urbanization_level(project_id, source)

    async def get_context_renovation_potential(self, project_id: int, source: str = None) -> dict:
        """Calculate renovation potential for project's context."""
        return await get_projects_context_renovation_potential(project_id, source)

    async def get_context_urbanization_level(self, project_id: int, source: str = None) -> GeoJSON:
        """Calculate urbanization level for project's context."""
        return await get_projects_context_urbanization_level(project_id, source)

    async def get_project_landuse_parts(self, scenario_id: int, source: str = None) -> dict:
        """Calculate zone parts inside project's territory"""
        return await get_projects_landuse_parts_scen_id_main_method(scenario_id, source)

    async def get_territory_urbanization_level(self, territory_id: int, source: str = None, force_recalculate: bool = False) -> dict:
        """Calculates and saves the urbanization percentage for a given territory in Urban DB"""
        return await get_territory(territory_id, source, force_recalculate)

landuse_service = LanduseService()
