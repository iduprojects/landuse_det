"""Service created for calculations related to indicators"""
from typing import Any

from landuse_app.logic.helpers.urban_api_access import get_territory_boundaries, put_indicator_value, get_service_count, \
    get_service_type_id_through_indicator
import pandas as pd
import requests
import geopandas as gpd
from shapely.geometry import shape
import math
import time


class IndicatorsService:
    @staticmethod
    async def calculate_territory_area(territory_id: int) -> dict:
        territory_data = await get_territory_boundaries(territory_id)
        # territory_data = response.json()
        geometry = shape(territory_data["geometry"])
        gdf = gpd.gpd.GeoDataFrame(
            [{"territory_id": territory_data["territory_id"], "name": territory_data["name"], "geometry": geometry}],
            geometry="geometry", crs="EPSG:4326"
        )
        gdf_utm = gdf.to_crs(gdf.estimate_utm_crs())
        area_sq_km = math.ceil(gdf_utm.area.sum() / 1e6)
        payload = {
            "indicator_id": 4,
            "territory_id": territory_data["territory_id"],
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": area_sq_km,
            "value_type": "real",
            "information_source": "modeled"
        }
        computed_indicator = await put_indicator_value(payload)
        return computed_indicator

    @staticmethod
    async def calculate_service_count(territory_id: int, indicator_id: int) -> dict:
        service_id = await get_service_type_id_through_indicator(indicator_id)
        services_count = await get_service_count(territory_id, service_id)
        # data = services_count.json()
        # number_of_services = data.get("count", 0)
        payload = {
            "indicator_id": indicator_id,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": services_count,
            "value_type": "real",
            "information_source": "modeled"
        }

        computed_indicator = await put_indicator_value(payload)
        return computed_indicator