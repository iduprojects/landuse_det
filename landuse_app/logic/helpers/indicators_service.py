"""Service created for calculations related to indicators"""
from datetime import datetime

from loguru import logger

from landuse_app.exceptions.http_exception_wrapper import http_exception
from landuse_app.logic.helpers.spatial_methods import SpatialMethods
from landuse_app.logic.helpers.urban_api_access import get_territory_boundaries, put_indicator_value, get_service_count, \
    get_service_type_id_through_indicator, check_indicator_exists, get_projects_territory, \
    check_project_indicator_exist, put_project_indicator, get_indicator_values, get_target_cities, \
    get_physical_objects_without_geometry, get_services_geojson, \
    get_functional_zones_geojson_territory_id

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

    @staticmethod
    async def population_density(territory_id: int, force_recalculate: bool = False) -> dict:
        logger.info(f"Started density calculation for territory {territory_id}")

        if not force_recalculate:
            existing = await check_indicator_exists(territory_id, indicator_id=37)
            if existing is not None:
                logger.info("Existing density indicator found, returning it")
                return existing

        territory_data = await get_territory_boundaries(territory_id)
        territory_gdf = await SpatialMethods.to_project_gdf(territory_data)
        gdf_utm = territory_gdf.to_crs(territory_gdf.estimate_utm_crs())
        area_km2 = gdf_utm.area.sum() / 1e6

        population_data = await get_indicator_values(territory_id, indicator_id=1)
        if not population_data:
            http_exception(404, f"No population data for territory {territory_id}")

        for rec in population_data:
            rec["_upd"] = datetime.fromisoformat(rec["updated_at"].rstrip("Z"))
        ros = [rec for rec in population_data if rec.get("information_source") == "РОССТАТ"]
        if ros:
            chosen = max(ros, key=lambda r: r["_upd"])
        else:
            chosen = max(population_data, key=lambda r: r["_upd"])
            logger.info(f"Fallback population source: '{chosen['information_source']}' on {chosen['date_value']}")

        population = float(chosen["value"])

        density = math.ceil(population / area_km2)

        payload = {
            "indicator_id": 37,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": density,
            "value_type": "real",
            "information_source": "modeled",
        }
        computed = await put_indicator_value(payload)
        logger.info(f"Stored density for territory {territory_id}: {density} population/km²")

        return computed

    @staticmethod
    async def target_cities(territory_id: int, force_recalculate: bool = False) -> dict:
        if not force_recalculate:
            existing = await check_indicator_exists(territory_id, indicator_id=349)
            if existing is not None:
                logger.info("Existing density indicator found, returning it")
                return existing

        logger.info(f"Started calculation for territory {territory_id}")
        cities_response = await get_target_cities(territory_id)
        count = sum(1 for item in cities_response if item.get("target_city_type") is not None)
        payload = {
            "indicator_id": 349,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": count,
            "value_type": "real",
            "information_source": "modeled",
        }
        computed = await put_indicator_value(payload)
        logger.info(f"Stored target cities count for territory {territory_id}: {count}")
        return computed

    @staticmethod
    async def big_cities(territory_id: int, indicator_id: int, force_recalculate: bool = False) -> dict:
        if not force_recalculate:
            # existing = await check_indicator_exists(territory_id, indicator_id=10)
            existing = await check_indicator_exists(territory_id, indicator_id)
            if existing is not None:
                logger.info("Existing density indicator found, returning it")
                return existing
        logger.info(f"Started calculation for territory {territory_id}")
        params = {
            "cities_only": "true",
            "include_child_territories": "true",
            "last_only": "true",
        }
        # big_cities_response = await get_indicator_values(territory_id, indicator_id=10, params=params)
        big_cities_response = await get_indicator_values(territory_id, indicator_id=1, params=params)
        if not big_cities_response:
            large_count = 0
        else:
            values = [item.get("value", 0) for item in big_cities_response]
            large_count = sum(1 for v in values if v >= 100_000)
        payload = {
            "indicator_id": indicator_id,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": large_count,
            "value_type": "real",
            "information_source": "modeled",
        }
        computed = await put_indicator_value(payload)
        logger.info(
            f"Stored number of big cities count for territory {territory_id} and indicator {indicator_id}: {large_count}")
        return computed

    @staticmethod
    async def engineering_infrastructure(territory_id: int, force_recalculate: bool = False) -> dict:
        if not force_recalculate:
            existing = await check_indicator_exists(territory_id, indicator_id=88)
            if existing is not None:
                logger.info("Existing density indicator found, returning it")
                return existing
        params = {
            "physical_object_function_id": "23",
            "page_size": "1",
        }
        engineering_infrastructure_response = await get_physical_objects_without_geometry(territory_id, params=params)
        number_of_objects = engineering_infrastructure_response.get("count", 0)
        payload = {
            "indicator_id": 88,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": number_of_objects,
            "value_type": "real",
            "information_source": "modeled"
        }
        computed = await put_indicator_value(payload)
        return computed

    @staticmethod
    async def recreation_area(territory_id: int, force_recalculate: bool = False, source: str = None) -> dict:
        if not force_recalculate:
            existing = await check_indicator_exists(territory_id, indicator_id=138)
            if existing is not None:
                logger.info("Existing density indicator found, returning it")
                return existing

        recreation_data = await get_functional_zones_geojson_territory_id(territory_id, source,
                                                                          functional_zone_type_id=2)
        recreation_gdf = gpd.GeoDataFrame.from_features(recreation_data)
        recreation_gdf = recreation_gdf.set_crs(4326)
        recreation_gdf = recreation_gdf.to_crs(recreation_gdf.estimate_utm_crs())
        recreation_area = round((recreation_gdf.area.sum() / 1e6), 2)

        payload = {
            "indicator_id": 138,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": recreation_area,
            "value_type": "real",
            "information_source": "modeled",
        }
        computed = await put_indicator_value(payload)
        return computed

    @staticmethod
    async def oopt_parts(territory_id: int, force_recalculate: bool = False):
        if not force_recalculate:
            existing = await check_indicator_exists(territory_id, indicator_id=187)
            if existing is not None:
                logger.info("Existing density indicator found, returning it")
                return existing
        territory_data = await get_territory_boundaries(territory_id)
        territory_gdf = await SpatialMethods.to_project_gdf(territory_data)
        gdf_utm = territory_gdf.to_crs(territory_gdf.estimate_utm_crs())
        area_km2 = gdf_utm.area.sum() / 1e6

        nature_objects = await get_services_geojson(territory_id, service_type_id=4)
        features = nature_objects.get("features", [])
        if not features:
            recreation_part = 0.0
            logger.warning(f"No features for ID {territory_id}:  recreation_part=0.0%")
        else:
            recreation_gdf = gpd.GeoDataFrame.from_features(nature_objects)
            recreation_gdf = recreation_gdf.set_crs(4326)
            utm_crs = recreation_gdf.estimate_utm_crs()
            recreation_gdf = recreation_gdf.to_crs(utm_crs)
            recreation_area = recreation_gdf.area.sum() / 1e6

            if recreation_area == 0:
                recreation_part = 0.0
                logger.warning(f"OOPT area=0 for ID:{territory_id},  recreation_part forced to 0.0%")
            else:
                recreation_part = round((recreation_area / area_km2) * 100, 6)

        payload = {
            "indicator_id": 187,
            "territory_id": territory_id,
            "date_type": "year",
            "date_value": "2025-01-01",
            "value": recreation_part,
            "value_type": "real",
            "information_source": "modeled",
        }

        computed = await put_indicator_value(payload)
        return computed
