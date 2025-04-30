"""Service created for calculations related to indicators"""

from loguru import logger

from landuse_app.logic.helpers.spatial_methods import SpatialMethods
from landuse_app.logic.helpers.urban_api_access import get_territory_boundaries, put_indicator_value, get_service_count, \
    get_service_type_id_through_indicator, check_indicator_exists, get_projects_territory, \
    check_project_indicator_exist, put_project_indicator
import geopandas as gpd
from shapely.geometry import shape
import math


class IndicatorsService:
    @staticmethod
    async def calculate_territory_area(territory_id: int, force_recalculate: bool = False) -> dict:
        indicator_id = 4
        if not force_recalculate:
            existing_indicator = await check_indicator_exists(territory_id, indicator_id)
            if existing_indicator is not None:
                logger.info(f"Indicator already exists in Urban DB, returning existing value")
                return existing_indicator
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
            "indicator_id": indicator_id,
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
    async def calculate_service_count(territory_id: int, indicator_id: int, force_recalculate: bool = False) -> dict:
        if not force_recalculate:
            existing_indicator = await check_indicator_exists(territory_id, indicator_id)
            if existing_indicator is not None:
                logger.info(f"Indicator already exists in Urban DB, returning existing value")
                return existing_indicator
        service_id = await get_service_type_id_through_indicator(indicator_id)
        services_count = await get_service_count(territory_id, service_id)
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

    @staticmethod
    async def calculate_project_territory_area(project_id: int, force_recalculate: bool = False) -> dict:
        logger.info(f"Started calculation for project id {project_id}")
        if not force_recalculate:
            existing_indicator = await check_project_indicator_exist(project_id, indicator_id=4)
            if existing_indicator is not None:
                logger.info(f"Indicator already exists in Urban DB, returning existing value")
                return existing_indicator
        territory_data = await get_projects_territory(project_id)
        territory_gdf = await SpatialMethods.to_project_gdf(territory_data)

        geom = territory_gdf.geometry.iloc[0]
        area_km2 = await SpatialMethods.compute_area(geom)
        area_km2 = round(area_km2, 2)

        scenario_id = territory_gdf["scenario_id"].loc[0]
        territory_id = territory_gdf["territory_id"].loc[0]

        payload = {
            "indicator_id": 4,
            "scenario_id": int(scenario_id),
            "territory_id": int(territory_id),
            "hexagon_id": None,
            "value": float(area_km2),
            "comment": "--",
            "information_source": "modeled",
            "properties": {}
        }
        logger.info(f"Calculation for project id {project_id} successful")

        computed_indicator = await put_project_indicator(scenario_id, payload)

        return computed_indicator
