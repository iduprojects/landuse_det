import json
import geopandas as gpd
import numpy as np
import pandas as pd
from loguru import logger
from shapely import Polygon
from shapely.geometry import shape
import asyncio
from pandarallel import pandarallel

from landuse_app.schemas import GeoJSON, Profile
from storage.caching import caching_service
from .urban_api_access import get_all_physical_objects_geometries, get_functional_zones_scenario_id
from ..constants.constants import zone_mapping

pandarallel.initialize(progress_bar=False, nb_workers=4)


async def extract_physical_objects(project_id: int, is_context: bool) -> gpd.GeoDataFrame:
    """
    Extracts and processes physical objects for a given project, handling geometries and object attributes.

    Parameters:
    project_id : int
        The ID of the project for which physical objects are to be extracted.
    is_context : bool
        A flag indicating whether to fetch context-based data.

    Returns:
    gpd.GeoDataFrame
        A GeoDataFrame containing processed physical objects with valid geometries and relevant attributes.
    """
    logger.info("Физические объекты загружаются")
    physical_objects_response = await get_all_physical_objects_geometries(project_id, is_context)
    physical_objects_data = physical_objects_response
    all_data = []

    for feature in physical_objects_data["features"]:
        geometry = feature.get("geometry")
        properties = feature["properties"]

        try:
            shapely_geometry = shape(geometry)
            if not shapely_geometry.is_valid:
                shapely_geometry = shapely_geometry.buffer(0)
            if not shapely_geometry.is_valid or shapely_geometry.is_empty:
                continue
        except Exception as e:
            logger.error(f"Ошибка при обработке геометрии: {e}")
            continue

        for physical_object in properties.get("physical_objects", []):
            object_data = {
                "physical_object_id": physical_object.get("physical_object_id"),
                "object_type": physical_object.get("physical_object_type", {}).get("name", "Unknown"),
                "object_type_id": physical_object.get("physical_object_type", {}).get("id"),
                "name": physical_object.get("name", "(unnamed)"),
                "geometry_type": shapely_geometry.geom_type if shapely_geometry else None,
                "geometry": shapely_geometry,
                "category": None,
                "storeys_count": None,
                "living_area": None,
                "service_id": None,
                "service_name": None,
            }

            if "living_building" in physical_object and physical_object["living_building"]:
                living_building = physical_object["living_building"]
                building_data = json.loads(living_building.get("properties", {}).get("building_data", "{}"))
                object_data.update({
                    "category": "residential",
                    "storeys_count": building_data.get("storeys_count"),
                    "living_area": building_data.get("living_area"),
                    "address": building_data.get("address", properties.get("address", None)),
                })

            elif physical_object.get("physical_object_type", {}).get("id") == 5:
                services = properties.get("services", [])
                if services:
                    for service in services:
                        service_id = service.get("service_type", {}).get("id", "Unknown")
                        service_name = service.get("service_type", {}).get("name", "Unknown")
                        object_data.update({
                            "category": "non_residential",
                            "service_id": service_id,
                            "service_name": service_name,
                        })
                        all_data.append(object_data.copy())
                else:
                    object_data.update({
                        "category": "non_residential",
                    })

            else:
                object_data.update({
                    "category": "other",
                })

            all_data.append(object_data)

    logger.info("Физические объекты загружены")

    all_data_df = pd.DataFrame(all_data)
    all_data_gdf = gpd.GeoDataFrame(all_data_df, geometry="geometry", crs="EPSG:4326")
    all_data_gdf = all_data_gdf.drop_duplicates(subset='physical_object_id')
    all_data_gdf = all_data_gdf[all_data_gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    all_data_gdf = all_data_gdf[~all_data_gdf['object_type'].isin(["Озеро", "Водный объект", "Река", "Площадка"])]

    return all_data_gdf


async def extract_landuse(project_id: int, is_context: bool) -> gpd.GeoDataFrame:
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

    geojson_data = await get_functional_zones_scenario_id(project_id, is_context)
    logger.info("Функциональные зоны загружаются")

    features = geojson_data["features"]
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

    logger.info("Функциональные зоны загружены")
    return landuse_polygons


async def calculate_building_percentages(buildings_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Filters residential buildings and calculates the percentage distribution by building categories.

    Parameters:
    buildings_gdf (gpd.GeoDataFrame): GeoDataFrame containing building information.

    Returns:
    pd.Series: Series with percentages of buildings by categories.
    """

    if buildings_gdf.empty:
        return pd.Series({"ИЖС": 0, "Малоэтажная": 0, "Среднеэтажная": 0, "Многоэтажная": 0})

    residential_buildings = buildings_gdf.loc[
        (buildings_gdf["object_type"] == "Жилой дом") & buildings_gdf["storeys_count"].notna()
        ]

    categories = pd.cut(
        residential_buildings["storeys_count"],
        bins=[0, 2, 4, 8, float('inf')],
        labels=["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"]
    )

    category_counts = categories.value_counts(normalize=True) * 100
    percentages = category_counts.reindex(["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"], fill_value=0)

    return percentages


def calculate_profiled_building_area(
    buildings_in_zone: gpd.GeoDataFrame,
    zone: gpd.GeoSeries,
    profile_types: list
) -> float:
    """
    Calculates the percentage of the total area occupied by profiled buildings within a specific zone.

    Parameters:
    buildings_in_zone : gpd.GeoDataFrame
        A GeoDataFrame containing building geometries and associated attributes that fall within the zone.
    zone : gpd.GeoSeries
        A GeoSeries representing the polygonal zone to analyze.
    profile_types : list
        A list of building types considered as "profiled" for the given zone.

    Returns:
    float
        The percentage of the zone's area covered by profiled buildings.
        If no buildings match the profile or the zone's area is zero, returns 0.
    """
    if not hasattr(zone, "geometry") or not isinstance(zone.geometry, Polygon) or zone.geometry.area == 0:
        return 0

    zone_area = zone.geometry.area
    profiled_buildings = buildings_in_zone[
        buildings_in_zone["object_type"].isin(profile_types) & buildings_in_zone.geometry.notnull()
    ]
    if profiled_buildings.empty:
        return 0

    profiled_building_area = profiled_buildings.geometry.area.sum()
    return (profiled_building_area / zone_area * 100) if zone_area > 0 else 0


def calculate_total_building_area(buildings_in_zone, zone):
    """
    Calculates the percentage of the total building area relative to the zone's area.

    Parameters:
    buildings_in_zone (gpd.GeoDataFrame): Buildings that fall within the zone.
    zone (gpd.GeoSeries): Polygonal zone.

    Returns:
    float: Percentage of the total building area.
    """

    if not hasattr(zone, "geometry") or not isinstance(zone.geometry, Polygon) or zone.geometry.area == 0:
        return 0
    zone_area = zone.geometry.area
    if buildings_in_zone.empty:
        return 0

    total_building_area = buildings_in_zone.geometry.area.sum()
    return (total_building_area / zone_area * 100) if zone_area > 0 else 0


async def assign_development_type(landuse_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Determines the type of development and the level of urbanization for each object in the GeoDataFrame.

    Parameters:
    gdf (GeoDataFrame): Input data with building attributes.

    Returns:
    GeoDataFrame: Updated data with 'Застройка' and 'Процент урбанизации' columns.
    """
    required_columns = ["Любые здания /на зону", "landuse_zone", "Многоэтажная"]
    development_types = ["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"]
    missing_columns = [col for col in required_columns + development_types if col not in landuse_polygons.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    landuse_polygons["Застройка"] = None
    landuse_polygons["Процент урбанизации"] = None

    development_values = landuse_polygons[development_types].apply(pd.to_numeric, errors="coerce")
    landuse_polygons["Застройка"] = development_values.idxmax(axis=1).where(development_values.max(axis=1) > 0)

    conditions = [
        (landuse_polygons["landuse_zone"] == "Residential") & (landuse_polygons["Многоэтажная"] > 30),
        (landuse_polygons["landuse_zone"] == "Residential") & (landuse_polygons["Среднеэтажная"] > 40),

        (landuse_polygons["Любые здания /на зону"].isna()) | (landuse_polygons["Любые здания /на зону"] == 0),
        (landuse_polygons["Любые здания /на зону"] < 10),
        (landuse_polygons["Любые здания /на зону"] < 25),
        (landuse_polygons["Любые здания /на зону"] < 75),
        (landuse_polygons["Любые здания /на зону"] < 90),
        (landuse_polygons["Любые здания /на зону"] >= 90),
    ]

    urbanization_levels = [
        "Высоко урбанизированная территория",  # Residential + Многоэтажная > 30%
        "Высоко урбанизированная территория",  # Residential + Среднеэтажная > 40%

        "Мало урбанизированная территория",  # No data or 0%
        "Мало урбанизированная территория",  # <10%
        "Слабо урбанизированная территория",  # <25%
        "Средне урбанизированная территория",  # <75%
        "Хорошо урбанизированная территория",  # <90%
        "Высоко урбанизированная территория",  # >=90%
    ]

    landuse_polygons["Процент урбанизации"] = np.select(
        conditions,
        urbanization_levels,
        default="Мало урбанизированная территория"
    )

    return landuse_polygons


async def analyze_geojson_for_renovation_potential(
        landuse_polygons: gpd.GeoDataFrame,
        selected_profile_to_exclude: str
        ) -> gpd.GeoDataFrame:
    """
    Analyze geodata to determine renovation potential and calculate a global "discomfort" coefficient.

    Parameters:
    landuse_polygons (GeoDataFrame): Input data with geometry and attributes.
    selected_profile_to_exclude (str): Profile to exclude from renovation.

    Returns:
    GeoDataFrame: Processed data with updated calculations and columns.
    """
    landuse_polygons["Площадь"] = landuse_polygons.geometry.area
    landuse_polygons["Потенциал"] = "Подлежащие реновации"

    conditions = [
        (landuse_polygons["landuse_zone"] == "Recreation") &
        (landuse_polygons["Процент урбанизации"] == "Высоко урбанизированная территория"),
        landuse_polygons["landuse_zone"] == "Special",
        (landuse_polygons["landuse_zone"] == "Residential") &
        (landuse_polygons["Многоэтажная"] > 50),
        landuse_polygons["Процент урбанизации"] == "Хорошо урбанизированная территория",
        landuse_polygons["Процент урбанизации"] == "Высоко урбанизированная территория",
        landuse_polygons["landuse_zone"] == selected_profile_to_exclude,
    ]

    if selected_profile_to_exclude in landuse_polygons["landuse_zone"].unique():
        landuse_polygons.loc[landuse_polygons["landuse_zone"] == selected_profile_to_exclude, "Потенциал"] = None

    combined_condition = np.logical_or.reduce(conditions)
    landuse_polygons.loc[combined_condition, "Потенциал"] = None
    total_area = landuse_polygons["Площадь"].sum()
    renovation_area = landuse_polygons.loc[landuse_polygons["Потенциал"].isnull(), "Площадь"].sum()

    landuse_polygons["Неудобия"] = (renovation_area / total_area * 100) if total_area > 0 else 0
    landuse_polygons = landuse_polygons[landuse_polygons["Площадь"] > 0]

    return landuse_polygons


def calculate_building_percentages_optimized(buildings_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Optimized function to calculate building percentages by storeys category.

    Parameters:
    buildings_gdf (gpd.GeoDataFrame): GeoDataFrame with buildings.

    Returns:
    pd.Series: Series with percentages of categorized buildings.
    """
    if buildings_gdf.empty or "storeys_count" not in buildings_gdf.columns or "object_type" not in buildings_gdf.columns:
        return pd.Series({"ИЖС": 0, "Малоэтажная": 0, "Среднеэтажная": 0, "Многоэтажная": 0})

    # Filter for valid "Жилой дом" and non-null storeys_count
    filtered_storeys = buildings_gdf.loc[
        (buildings_gdf["object_type"] == "Жилой дом") & buildings_gdf["storeys_count"].notna(), "storeys_count"
    ]

    if filtered_storeys.empty:
        return pd.Series({"ИЖС": 0, "Малоэтажная": 0, "Среднеэтажная": 0, "Многоэтажная": 0})

    bins = [0, 2, 4, 8, float('inf')]
    bin_indices = np.searchsorted(bins, filtered_storeys.values, side="right") - 1
    bin_counts = np.bincount(bin_indices, minlength=len(bins) - 1)

    total = len(filtered_storeys)
    percentages = (bin_counts / total * 100)
    labels = ["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"]
    return pd.Series(dict(zip(labels, percentages)))


async def process_zones_with_bulk_update(
    landuse_polygons: gpd.GeoDataFrame,
    physical_objects: gpd.GeoDataFrame,
    physical_objects_sindex: gpd.sindex.SpatialIndex,
    zone_mapping: dict[str, list]
) -> gpd.GeoDataFrame:
    """
    Asynchronously processes land-use zones and updates building metrics.

    This function processes zones in the GeoDataFrame `landuse_polygons`, calculating
    building percentages and area metrics for each zone using physical objects data.

    Parameters:
    -----------
    landuse_polygons : gpd.GeoDataFrame
        GeoDataFrame containing land-use zones to process.
    physical_objects : gpd.GeoDataFrame
        GeoDataFrame with physical object geometries and attributes.
    physical_objects_sindex : rtree.index.Index
        Spatial index for `physical_objects` to improve query performance.
    zone_mapping : dict
        Mapping of zone types to relevant profile types.

    Returns:
    --------
    gpd.GeoDataFrame
        Updated `landuse_polygons` with calculated building percentages and area metrics.

    Workflow:
    ---------
    1. Processes each zone to calculate:
        - Building percentages by type.
        - Profiled building area percentage.
        - Total building area percentage.
    2. Updates the GeoDataFrame with calculated metrics using bulk updates.
    3. Ensures all required columns exist and validates the result.
    """
    def process_zone(row):
        """
        Processes a single zone to calculate building percentages and area metrics.
        """
        idx, zone = row.name, row
        zone_gdf = gpd.GeoDataFrame([zone], geometry="geometry", crs=physical_objects.crs)
        precise_matches = gpd.sjoin(
            physical_objects, zone_gdf, how="inner", predicate="within"
        )
        percentages = calculate_building_percentages_optimized(precise_matches)
        profile_types = zone_mapping.get(zone["landuse_zone"], [])

        if profile_types:
            profiled_area_percentage = calculate_profiled_building_area(precise_matches, zone, profile_types)
        else:
            profiled_area_percentage = 0

        total_area_percentage = calculate_total_building_area(precise_matches, zone)

        return {
            "idx": idx,
            "percentages": percentages,
            "profiled_area_percentage": profiled_area_percentage,
            "total_area_percentage": total_area_percentage,
        }

    async def parallel_processing():
        return await asyncio.to_thread(lambda: landuse_polygons.parallel_apply(process_zone, axis=1).tolist())
    results = await parallel_processing()

    idx_list = [result["idx"] for result in results]
    percentages_df = pd.DataFrame([result["percentages"] for result in results])
    profiled_areas = [result["profiled_area_percentage"] for result in results]
    total_areas = [result["total_area_percentage"] for result in results]
    required_columns = ["Многоэтажная", "ИЖС", "Малоэтажная", "Среднеэтажная"]
    for col in required_columns:
        if col not in landuse_polygons.columns:
            landuse_polygons[col] = 0.0

    landuse_polygons.loc[idx_list, ["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"]] = percentages_df.values
    landuse_polygons.loc[idx_list, "Процент профильных зданий"] = profiled_areas
    landuse_polygons.loc[idx_list, "Любые здания /на зону"] = total_areas
    landuse_polygons["Процент профильных зданий"] = landuse_polygons["Процент профильных зданий"].clip(upper=100)
    landuse_polygons["Любые здания /на зону"] = landuse_polygons["Любые здания /на зону"].clip(upper=100)

    missing_columns = [col for col in required_columns if col not in landuse_polygons.columns]
    if missing_columns:
        raise ValueError(f"Columns still missing after update: {', '.join(missing_columns)}")
    return landuse_polygons


async def get_renovation_potential(project_id: int, profile: Profile, is_context: bool) -> gpd.GeoDataFrame:
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
    profile : Profile
        The user profile providing the context and criteria for analysis.
    is_context : bool
        A flag indicating whether the calculation should include surrounding context.

    Returns:
    --------
    gpd.GeoDataFrame
        A GeoDataFrame containing the renovation potential analysis results with calculated attributes.

    Workflow:
    ---------
    1. Checks for a valid cached result using the project ID and context flag.
    2. If cache exists, loads and returns the cached data.
    3. If no cache is found, asynchronously extracts physical objects and land-use polygons.
    4. Projects geometries to EPSG:3857 and simplifies large geometries for optimization.
    5. Filters and processes functional zones with associated physical objects.
    6. Assigns development types to functional zones and calculates urbanization levels.
    7. Analyzes zones for renovation potential and returns a GeoDataFrame in EPSG:4326.
    """
    cache_name = f"renovation_potential_project-{project_id}_is_context-{is_context}"
    cache_file = caching_service.get_recent_cache_file(cache_name, {"profile": profile})

    if cache_file and caching_service.is_cache_valid(cache_file):
        logger.info(f"Using cached renovation potential for project {project_id}")
        cached_data = caching_service.load_cache(cache_file)
        return gpd.GeoDataFrame.from_features(cached_data, crs="EPSG:4326")
    physical_objects, landuse_polygons = await asyncio.gather(
        extract_physical_objects(project_id, is_context),
        extract_landuse(project_id, is_context)
    )
    physical_objects = physical_objects.to_crs(epsg=3857)
    landuse_polygons = landuse_polygons.to_crs(epsg=3857)
    landuse_polygons["geometry"] = landuse_polygons.apply(
        lambda row: row.geometry.simplify(0.01, preserve_topology=True)
        if row.geometry.area > 1e8
        else row.geometry,
        axis=1
    )
    logger.info("Функциональные зоны и физические объекты получены")
    landuse_polygons = landuse_polygons[landuse_polygons.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    landuse_polygons["Процент профильных зданий"] = 0.0
    landuse_polygons["Любые здания /на зону"] = 0.0
    logger.info("Функциональные зоны и физические объекты отфильтрованы")

    physical_objects_sindex = physical_objects.sindex

    landuse_polygons = await process_zones_with_bulk_update(landuse_polygons, physical_objects, physical_objects_sindex,
                                                            zone_mapping)
    logger.info("Проценты зданий посчитаны")

    landuse_polygons = await assign_development_type(landuse_polygons)
    logger.info("Уровень урбанизации присвоен")
    landuse_polygons_ren_pot = await analyze_geojson_for_renovation_potential(landuse_polygons, profile)
    logger.info("Потенциал для реновации рассчитан")
    landuse_polygons_ren_pot = landuse_polygons_ren_pot.to_crs(epsg=4326)

    result_json = json.loads(landuse_polygons_ren_pot.to_json())
    caching_service.save_with_cleanup(result_json, f"renovation_potential_project-{project_id}_is_context-{is_context}",
                                      {"profile": profile})

    return landuse_polygons_ren_pot


async def get_projects_renovation_potential(project_id: int, profile: Profile) -> GeoJSON:
    """Calculate renovation potential for project."""
    landuse_polygons = await get_renovation_potential(project_id, profile, is_context=False)
    return GeoJSON.from_geodataframe(landuse_polygons)


async def get_projects_urbanization_level(project_id: int, profile: Profile) -> GeoJSON:
    """Calculate urbanization level for project."""
    landuse_polygons = await get_renovation_potential(project_id, profile, is_context=False)
    return GeoJSON.from_geodataframe(landuse_polygons)


async def get_projects_context_renovation_potential(project_id: int, profile: Profile) -> GeoJSON:
    """Calculate renovation potential for project's context."""
    landuse_polygons = await get_renovation_potential(project_id, profile, is_context=True)
    return GeoJSON.from_geodataframe(landuse_polygons)


async def get_projects_context_urbanization_level(project_id: int, profile: Profile) -> GeoJSON:
    """Calculate urbanization level for project's context."""
    landuse_polygons = await get_renovation_potential(project_id, profile, is_context=True)
    return GeoJSON.from_geodataframe(landuse_polygons)
