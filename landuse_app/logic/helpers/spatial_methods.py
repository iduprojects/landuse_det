from typing import Union

import geopandas as gpd
import asyncio

import numpy as np
from pyproj import CRS
from pyproj.aoi import AreaOfInterest
from pyproj.database import query_utm_crs_info
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

    @staticmethod
    async def estimate_crs_for_bounds(minx, miny, maxx, maxy) -> CRS:
        x_center = np.mean([minx, maxx])
        y_center = np.mean([miny, maxy])
        utm_crs_list = query_utm_crs_info(
            datum_name="WGS 84",
            area_of_interest=AreaOfInterest(
                west_lon_degree=x_center,
                south_lat_degree=y_center,
                east_lon_degree=x_center,
                north_lat_degree=y_center,
            ),
        )
        crs = CRS.from_epsg(utm_crs_list[0].code)
        return crs

    @staticmethod
    async def compute_area(geom):
        utm_crs = await SpatialMethods.estimate_crs_for_bounds(*geom.bounds)
        gs = gpd.GeoSeries([geom], crs="EPSG:4326")
        gs_utm = gs.to_crs(utm_crs)
        area_m2 = gs_utm.area.iloc[0]
        return area_m2 / 1_000_000

    @staticmethod
    async def to_project_gdf(data: Union[dict, list]) -> gpd.GeoDataFrame:
        """
        Преобразует либо:
          - GeoJSON FeatureCollection (dict с ключами 'type':'FeatureCollection', 'features': [...])
          - Либо единичный dict с ключами 'geometry', 'project', 'project_territory_id' и т.п.
        в GeoDataFrame с колонками:
          - geometry
          - scenario_id
          - territory_id
        """
        if isinstance(data, dict) and data.get("type") == "FeatureCollection":
            features = data.get("features", [])
        elif isinstance(data, dict) and "geometry" in data:
            features = [{
                "type": "Feature",
                "geometry": data["geometry"],
                "properties": {
                    **data.get("properties", {}),
                    "project": data.get("project"),
                    "project_territory_id": data.get("project_territory_id"),
                }
            }]
        else:
            raise ValueError("to_project_gdf: Unsupported input format")

        gdf = gpd.GeoDataFrame.from_features(features)
        gdf = gdf.set_crs("EPSG:4326")

        if "project" in gdf.columns:
            gdf["scenario_id"] = gdf["project"].apply(
                lambda p: p.get("base_scenario", {}).get("id") if isinstance(p, dict) else None
            )
            gdf["territory_id"] = gdf["project"].apply(
                lambda p: (p.get("region") or {}).get("id") if isinstance(p, dict) else None
            )
        elif "base_scenario" in gdf.columns and "territory" in gdf.columns:
            gdf["scenario_id"] = gdf["base_scenario"].apply(
                lambda d: d.get("id") if isinstance(d, dict) else None
            )
            gdf["territory_id"] = gdf["territory"].apply(
                lambda d: d.get("id") if isinstance(d, dict) else None
            )
        else:
            raise ValueError("to_project_gdf: Cannot find scenario_id/territory_id in the data")

        return gdf
