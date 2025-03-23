import json
from datetime import datetime
import geopandas as gpd
import pandas as pd
from loguru import logger
from shapely.validation import make_valid
from shapely.geometry import shape
import asyncio
from pandarallel import pandarallel
import random

from storage.caching import caching_service
from .renovation_potential import analyze_geojson_for_renovation_potential, process_zones_with_bulk_update, \
    assign_development_type
from .urban_api_access import get_functional_zones_territory_id, get_functional_zone_sources_territory_id, \
    check_indicator_exists, put_indicator_value, get_physical_objects_from_territory_parallel
from ..constants.constants import zone_mapping
from ...exceptions.http_exception_wrapper import http_exception

pandarallel.initialize(progress_bar=False, nb_workers=4)


def parse_physical_object(obj: dict[str, any]) -> list[dict[str, any]]:
    """
    Parses a single physical object from the API response into a structured dictionary
    with geometry and additional attributes.

    This function:
      - Parses the geometry and validates it.
      - Determines the object's category (residential, non_residential, recreational, or other).
      - Calculates the number of storeys if building information is present.
      - Processes services if the object is non-residential.

    Returns a list of dictionaries because one object may contain multiple services,
    resulting in multiple rows (one per service) in the final dataset.

    Parameters:
        obj (dict): A dictionary representing a physical object from the API response.

    Returns:
        list[dict]: A list of parsed physical objects with geometry and attributes ready for GeoDataFrame.
    """
    geometry_json = obj.get("geometry")
    if not geometry_json:
        return []

    try:
        shp = shape(geometry_json)
        if not shp.is_valid:
            shp = shp.buffer(0)
        if shp.is_empty:
            return []
    except Exception as e:
        logger.error(f"Ошибка при формировании geometry: {e}")
        return []

    object_data = {
        "physical_object_id": obj.get("physical_object_id"),
        "object_type": obj.get("physical_object_type", {}).get("name", "Unknown"),
        "object_type_id": obj.get("physical_object_type", {}).get("physical_object_type_id"),
        "name": obj.get("name", "(unnamed)"),
        "geometry_type": shp.geom_type,
        "geometry": shp,
        "category": None,
        "storeys_count": None,
        "living_area": None,
        # "service_id": None,
        # "service_name": None,
    }

    building = obj.get("building")
    if building:
        building_props = building.get("properties", {})
        osm_data = building_props.get("osm_data", {})

        floors = building.get("floors")
        storeys_count = building_props.get("storeys_count")
        building_levels = osm_data.get("building:levels")

        if floors is not None and floors > 0:
            final_floors = floors
        elif storeys_count is not None and storeys_count > 0:
            final_floors = storeys_count
        elif building_levels:
            try:
                num = int(building_levels)
                final_floors = max(num, 1)
            except ValueError:
                final_floors = random.randint(2, 5)
        else:
            final_floors = random.randint(2, 5)

        object_data.update({
            "category": "residential",
            "storeys_count": final_floors,
            "living_area": (
                    building_props.get("living_area_official")
                    or building_props.get("living_area_modeled")
            ),
        })

    elif object_data["object_type_id"] == 5:
        services = obj.get("services", [])
        if services:
            parsed = []
            for service in services:
                tmp = object_data.copy()
                tmp["category"] = "non_residential"
                tmp["is_capacity_real"] = service.get("is_capacity_real")
                parsed.append(tmp)
            return parsed
        else:
            object_data["category"] = "non_residential"

    elif object_data["object_type"] == "Рекреационная зона":
        object_data["category"] = "recreational"

    else:
        object_data["category"] = "other"

    return [object_data]


async def extract_physical_objects_from_territory(territory_id: int) -> dict[str, gpd.GeoDataFrame]:
    """
        Extracts and processes physical objects for a given territory using parallel API requests.

        This function:
          - Fetches physical objects with geometry for the specified territory via parallel paginated requests.
          - Parses each object, extracting relevant attributes and geometry.
          - Builds a GeoDataFrame from the parsed objects.
          - Separates water bodies, green areas, and forests for area calculations.

        Returns:
            dict[str, gpd.GeoDataFrame]: A dictionary containing:
                - "physical_objects": GeoDataFrame of all valid physical objects
                - "water_objects": total area of water objects (in square meters)
                - "green_objects": total area of green objects (in square meters)
                - "forests": total area of forest objects (in square meters)
        """
    logger.info("Физические объекты загружаются (с параллельной загрузкой)")
    raw_objects = await get_physical_objects_from_territory_parallel(territory_id)
    all_data = []

    for obj in raw_objects:
        parsed_objects = parse_physical_object(obj)
        all_data.extend(parsed_objects)

    logger.success("Физические объекты получены, создается  GeoDataFrame")
    all_data_df = pd.DataFrame(all_data)
    all_data_gdf = gpd.GeoDataFrame(all_data_df, geometry="geometry", crs="EPSG:4326")
    all_data_gdf = all_data_gdf.drop_duplicates(subset='physical_object_id')
    all_data_gdf = all_data_gdf[all_data_gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]

    local_crs = all_data_gdf.estimate_utm_crs()

    water_objects_gdf = all_data_gdf[
        all_data_gdf['object_type'].isin(["Озеро", "Водный объект", "Река"])
    ].to_crs(local_crs)

    green_objects_gdf = all_data_gdf[
        all_data_gdf['object_type'].isin(["Травяное покрытие", "Зелёная зона"])
    ].to_crs(local_crs)

    forests_gdf = all_data_gdf[
        all_data_gdf['object_type'].isin(["Лес"])
    ].to_crs(local_crs)

    logger.success("Физические объекты успешно обработаны")
    return {
        "physical_objects": all_data_gdf,
        "water_objects": water_objects_gdf.area.sum(),
        "green_objects": green_objects_gdf.area.sum(),
        "forests": forests_gdf.area.sum()
    }

async def extract_landuse_from_territory(territory_id, source: str = None,)\
        -> gpd.GeoDataFrame:
    """
    Extracts functional zones polygons for a given project and returns them as a GeoDataFrame.

    Parameters:
    project_id : int
        The ID of the project for which land use data is to be extracted.
    is_context : bool
        Flag to determine if context-specific functional zones should be fetched.

    Returns:
    gpd.GeoDataFrame
        A GeoDataFrame containing land use polygons with relevant attributes.

    Raises:
    KeyError
        If required keys are missing in the fetched data.
    ValueError
        If the input data is malformed or invalid.
    """
    geojson_data = await get_functional_zones_territory_id(territory_id, source)
    logger.info("Функциональные зоны загружаются")

    features = geojson_data
    geometries = []
    for feature in features:
        try:
            geom = shape(feature["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
            geometries.append(geom)
        except Exception as e:
            logger.error(f"Error processing geometry: {e}")
            geometries.append(None)

    properties = [feature["properties"] for feature in features]
    landuse_polygons = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")

    if 'properties' in landuse_polygons.columns:
        landuse_polygons['landuse_zone'] = landuse_polygons['properties'].apply(
            lambda x: x.get('landuse_zon') if isinstance(x, dict) else None)

    if 'functional_zone_type' in landuse_polygons.columns:
        landuse_polygons['zone_type_id'] = landuse_polygons['functional_zone_type'].apply(
            lambda x: x.get('id') if isinstance(x, dict) else None)
        landuse_polygons['zone_type_name'] = landuse_polygons['functional_zone_type'].apply(
            lambda x: x.get('name') if isinstance(x, dict) and x.get('name') != "unknown" else "residential"
        )
        landuse_polygons['zone_type_nickname'] = landuse_polygons['functional_zone_type'].apply(
            lambda x: x.get('nickname') if isinstance(x, dict) and x.get('nickname') != "unknown" else "Жилая зона"
        )

    if "territory" in landuse_polygons.columns:
        landuse_polygons['zone_type_parent_territory_id'] = landuse_polygons['territory'].apply(
            lambda x: x.get('id') if isinstance(x, dict) else None)
        landuse_polygons['zone_type_parent_territory_name'] = landuse_polygons['territory'].apply(
            lambda x: x.get('name') if isinstance(x, dict) else None)

    landuse_polygons.drop(
        columns=['properties', 'functional_zone_type', 'territory', 'created_at', 'updated_at', 'zone_type_name'],
        inplace=True, errors='ignore'
    )

    landuse_polygons.replace({
        "zone_type_name": {"unknown": "residential"},
        "zone_type_nickname": {"unknown": "Жилая зона"}
    }, inplace=True)

    if 'landuse_zon' in landuse_polygons.columns:
        landuse_polygons.rename(columns={'landuse_zon': 'landuse_zone'}, inplace=True)

    if 'landuse_zone' not in landuse_polygons.columns:
        landuse_polygons['landuse_zone'] = 'Residential'
    logger.success("Функциональные зоны загружены")
    return landuse_polygons


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
        extract_physical_objects_from_territory(territory_id),
        extract_landuse_from_territory(territory_id, source)
    )
    logger.success("Физические объекты загружены")
    physical_objects = physical_objects_dict["physical_objects"]
    utm_crs = physical_objects.estimate_utm_crs()
    physical_objects = physical_objects.to_crs(utm_crs)
    landuse_polygons = landuse_polygons.to_crs(utm_crs)

    logger.success("Функциональные зоны и физические объекты получены")
    landuse_polygons = landuse_polygons[landuse_polygons.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    landuse_polygons["Процент профильных объектов"] = 0.0
    landuse_polygons["Любые здания /на зону"] = 0.0
    logger.success("Функциональные зоны и физические объекты отфильтрованы")

    landuse_polygons = await process_zones_with_bulk_update(landuse_polygons, physical_objects,
                                                            zone_mapping)
    logger.success("Проценты зданий посчитаны")

    landuse_polygons = await assign_development_type(landuse_polygons)
    logger.success("Уровень урбанизации присвоен")

    landuse_polygons_ren_pot = await analyze_geojson_for_renovation_potential(landuse_polygons)
    logger.success("Потенциал для реновации рассчитан")

    zones = landuse_polygons_ren_pot.to_crs(utm_crs)
    non_renovated = zones[pd.isna(zones['Потенциал'])]
    non_renovated["geometry"] = non_renovated["geometry"].apply(make_valid)
    non_renovated = non_renovated[non_renovated.is_valid]
    buffered_geometries = non_renovated.buffer(300)
    buffered_gdf = gpd.GeoDataFrame(geometry=buffered_geometries, crs=zones.crs)
    renovated = zones[
        (zones['Потенциал'] == 'Подлежащие реновации')
        ]

    joined = gpd.sjoin(
        renovated,
        buffered_gdf,
        how='inner',
        predicate='intersects'
    )
    joined["geometry"] = joined["geometry"].apply(make_valid)
    if joined.empty:
        logger.info("No intersections between buffers and polygons were found,"
                    " returning polygons without intersections")
        landuse_polygons_ren_pot = zones.to_crs(epsg=4326)

        result_json = json.loads(landuse_polygons_ren_pot.to_json())
        caching_service.save_with_cleanup(
            result_json, cache_name,
            {"profile": "no_profile",
             "source": source_key})

        return landuse_polygons_ren_pot
    else:
        try:

            joined['intersection_area'] = joined.apply(
                lambda row: row.geometry.intersection(
                    buffered_gdf.loc[row.index_right].geometry
                ).area if not row.geometry.intersection(buffered_gdf.loc[row.index_right].geometry).is_empty else 0,
                axis=1
            )
        except Exception as e:
            raise http_exception(500, "Error while searching for intersections between buffers and polygons", str(e))

    grouped = joined.groupby(joined.index)['intersection_area'].sum()
    final_overlap_ratio = grouped / renovated.loc[grouped.index].geometry.area
    to_update = final_overlap_ratio[final_overlap_ratio > 0.50].index
    zones.loc[to_update, 'Потенциал'] = 'Не подлежащие реновации'
    landuse_polygons_ren_pot = zones.to_crs(epsg=4326)

    result_json = json.loads(landuse_polygons_ren_pot .to_json())
    caching_service.save_with_cleanup(
        result_json, cache_name,
        {"profile": "no_profile",
         "source": source_key})

    return landuse_polygons_ren_pot


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
        existing_indicator = await check_indicator_exists(territory_id)
        if existing_indicator is not None:
            logger.info(f"Indicator already exists in Urban DB, returning existing value")
            return existing_indicator

    logger.info(f"Recalculating indicator for territory {territory_id} (either forced or not found)")
    landuse_polygons = await get_territory_renovation_potential(territory_id, source=source)
    computed_indicator = await compute_urbanization_indicator(landuse_polygons, territory_id)
    saved_indicator = await put_indicator_value(computed_indicator)
    return saved_indicator
