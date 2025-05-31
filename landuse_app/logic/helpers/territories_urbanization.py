import json
from datetime import datetime
import geopandas as gpd
import pandas as pd
from loguru import logger
import asyncio
from pandarallel import pandarallel

from storage.caching import caching_service
from .preprocessing_service import data_extraction
from .renovation_potential import process_zones_with_bulk_update, \
    assign_development_type
from .urban_api_access import get_functional_zone_sources_territory_id, \
    check_urbanization_indicator_exists, put_indicator_value
from ..constants import actual_zone_mapping

pandarallel.initialize(progress_bar=False, nb_workers=4)


async def get_territory_renovation_potential(
        territory_id: int,
        source: str = None
) -> gpd.GeoDataFrame:
    """
    Calculate the renovation potential for a given project.

    This asynchronous function computes the renovation potential by analyzing
    functional zones and associated physical objects within a project area.
    It supports caching for optimized performance and processes data to
    generate a geospatial DataFrame containing renovation potential details.

    Parameters:
    -----------
    project_id : int
        The unique identifier of the project for which the renovation potential is calculated.
    is_context : bool
        A flag indicating whether the calculation should include surrounding context.
    profile : Optional[Profile]
        The user profile providing the context and criteria for analysis.
        If not provided, a stub "no_profile" will be used for caching and None will be passed for analysis.
    scenario_id : bool, optional
        Scenario identifier flag (default is False).

    Returns:
    --------
    gpd.GeoDataFrame
        A GeoDataFrame containing the renovation potential analysis results with calculated attributes.
    """

    if source is None:
        source_data = await get_functional_zone_sources_territory_id(territory_id)
        source_key = source_data["source"]
    else:
        source_key = source

    cache_name = f"renovation_potential_territory-{territory_id}"
    cache_file = caching_service.get_recent_cache_file(cache_name, {"profile": "no_profile", "source": source_key})

    if cache_file and caching_service.is_cache_valid(cache_file):
        logger.info(f"Using cached renovation potential for project {territory_id}")
        cached_data = caching_service.load_cache(cache_file)
        return gpd.GeoDataFrame.from_features(cached_data, crs="EPSG:4326")

    physical_objects_dict, landuse_polygons = await asyncio.gather(
        data_extraction.extract_physical_objects_from_territory(territory_id),
        data_extraction.extract_landuse_from_territory(territory_id, source)
    )
    logger.success("Physical objects are loaded")
    physical_objects = physical_objects_dict["physical_objects"]
    utm_crs = physical_objects.estimate_utm_crs()
    physical_objects = physical_objects.to_crs(utm_crs)
    landuse_polygons = landuse_polygons.to_crs(utm_crs)

    services_gdf = await data_extraction.extract_services(territory_id)
    if services_gdf.empty:
        combined_gdf = physical_objects.copy()
    else:
        services_gdf = services_gdf.to_crs(physical_objects.crs)

        combined_df = pd.concat(
            [physical_objects, services_gdf],
            ignore_index=True,
            sort=False
        )
        combined_gdf = gpd.GeoDataFrame(
            combined_df,
            geometry="geometry",
            crs=physical_objects.crs
        )

        physical_objects = combined_gdf

    logger.success("Functional objects and physical objects are loaded")
    landuse_polygons = landuse_polygons[landuse_polygons.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    landuse_polygons["Процент профильных объектов"] = 0.0
    landuse_polygons["Любые здания /на зону"] = 0.0
    logger.success("Functional zones and physical objects are filtered")

    landuse_polygons = await process_zones_with_bulk_update(landuse_polygons, physical_objects,
                                                            actual_zone_mapping)
    logger.success("Building percentages are calculated")

    landuse_polygons = await assign_development_type(landuse_polygons)
    logger.success("Urbanization level is calculated")

    zones = landuse_polygons.to_crs(utm_crs)

    high_obj_ids = {11, 61}
    high_srv_ids = {4, 81}

    high_objs = physical_objects[
        physical_objects["object_type_id"].isin(high_obj_ids) |
        physical_objects["service_type_id"].isin(high_srv_ids)
        ]

    if not high_objs.empty:
        high_objs = high_objs.to_crs(zones.crs)
        high_join = gpd.sjoin(
            zones,
            high_objs[["geometry"]],
            how="inner",
            predicate="intersects"
        )

        if not high_join.empty:
            high_zone_ids = high_join.index.unique()

            zones.loc[
                high_zone_ids,
                "Уровень урбанизации"
            ] = "Высоко урбанизированная территория"

    landuse_polygons = zones.to_crs("EPSG:4326")
    result_json = json.loads(landuse_polygons.to_json())
    caching_service.save_with_cleanup(
        result_json, cache_name,
        {"profile": "no_profile",
         "source": source_key})

    return landuse_polygons


async def compute_urbanization_indicator(polygons_gdf: gpd.GeoDataFrame, territory_id: int) -> dict:
    """
    Calculates the urbanization percentage for a given territory.

    A zone is considered well urbanized if the value in the "Urbanization Level" column
    (or "Urbanization Percentage" if not renamed) is one of the following:
      - "Moderately urbanized territory"
      - "Well urbanized territory"
      - "Highly urbanized territory"

    Returns a dictionary in the following format:
      {
        "indicator_id": 16,
        "territory_id": territory_id,
        "date_type": "year",
        "date_value": "YYYY-01-01",  # current year
        "value": percentage (float, rounded to 2 decimals),
        "value_type": "forecast",
        "information_source": "landuse_det"
      }
    """
    if "Процент урбанизации" in polygons_gdf.columns:
        polygons_gdf = polygons_gdf.rename(columns={"Процент урбанизации": "Уровень урбанизации"})

    if "Уровень урбанизации" not in polygons_gdf.columns:
        percentage = 0.0
    else:
        local_crs = polygons_gdf.estimate_utm_crs()
        polygons_gdf_m = polygons_gdf.to_crs(local_crs)
        good_levels = {
            "Средне урбанизированная территория",
            "Хорошо урбанизированная территория",
            "Высоко урбанизированная территория"
        }
        # total_zones = len(polygons_gdf)
        # good_zones = polygons_gdf[polygons_gdf["Уровень урбанизации"].isin(good_levels)]
        # percentage = round((len(good_zones) / total_zones * 100) if total_zones > 0 else 0.0, 2)

        mask_good = polygons_gdf_m["Уровень урбанизации"].isin(good_levels)
        total_area = polygons_gdf_m.geometry.area.sum()
        good_area = polygons_gdf_m.loc[mask_good, "geometry"].area.sum()

        if total_area > 0:
            percentage = round((good_area / total_area) * 100, 2)
        else:
            percentage = 0.0

    result = {
        "indicator_id": 16,
        "territory_id": territory_id,
        "date_type": "year",
        "date_value": f"{datetime.now().year}-01-01",
        "value": percentage,
        "value_type": "forecast",
        "information_source": "landuse_det"
    }
    return result


async def get_territory(territory_id: int, source: str = None, force_recalculate: bool = False) -> dict:
    """
    Main method for retrieving territory data.

    If force_recalculate is False, the method first checks for the existence of the indicator:
      - If the indicator exists, it returns the existing JSON response.
      - If the indicator is not found, it calculates, saves (via PUT), and returns the new result.

    If force_recalculate is True, the method always calculates the indicator, saves it via PUT (overwriting the existing value), and returns the new result.
    """
    logger.info(f"Started calculation for territory {territory_id}")
    if not force_recalculate:
        existing_indicator = await check_urbanization_indicator_exists(territory_id)
        if existing_indicator is not None:
            logger.info(f"Indicator already exists in Urban DB, returning existing value")
            return existing_indicator

    logger.info(f"Recalculating indicator for territory {territory_id} (either forced or not found)")
    landuse_polygons = await get_territory_renovation_potential(territory_id, source=source)
    computed_indicator = await compute_urbanization_indicator(landuse_polygons, territory_id)
    saved_indicator = await put_indicator_value(computed_indicator)
    return saved_indicator
