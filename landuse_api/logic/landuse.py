"""Landuse handlers logic."""

import abc
from typing import Protocol

from landuse_api.schemas import GeoJSON, Profile


class LanduseService(Protocol):
    """Landuse service."""

    @abc.abstractmethod
    async def get_renovation_potential(self, project_id: int, profile: Profile, user_id: str) -> GeoJSON:
        """Calculate renovation potential for project."""

    @abc.abstractmethod
    async def get_urbanization_level(self, project_id: int, user_id: str) -> GeoJSON:
        """Calculate urbanization level for project."""
