import geopandas as gpd
import asyncio
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads, dumps


class SpatialMethods:
    @staticmethod
    async def round_coords_geom(
            geometry: gpd.GeoSeries | BaseGeometry,
            ndigits: int = 5
    ) -> gpd.GeoSeries:
        """
        Rounds geometry coordinates to the specified precision.

        Args:
            geometry: GeoSeries or iterable of geometries to be rounded.
            ndigits: Number of decimal places for coordinates.

        Returns:
            A GeoSeries with rounded geometries.
        """
        return await asyncio.to_thread(
            geometry.map,
            lambda geom: loads(dumps(geom, rounding_precision=ndigits))
        )