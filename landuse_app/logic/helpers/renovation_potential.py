import json
from typing import Optional
import geopandas as gpd
import numpy as np
import pandas as pd
from loguru import logger
from shapely import Polygon, MultiPolygon
import asyncio
from pandarallel import pandarallel
from landuse_app.schemas import GeoJSON, Profile
from storage.caching import caching_service
from .interpretation_service import interpretation_service
from .preprocessing_service import data_extraction
from .spatial_methods import SpatialMethods
from .urban_api_access import get_projects_base_scenario_id, get_functional_zone_sources
from ..constants.constants import actual_zone_mapping
from ...exceptions.http_exception_wrapper import http_exception

pandarallel.initialize(progress_bar=False, nb_workers=4)

def calculate_building_percentages(buildings_gdf: gpd.GeoDataFrame) -> pd.Series:
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
    if not hasattr(zone, "geometry") or not isinstance(zone.geometry,
                                                       (Polygon, MultiPolygon)) or zone.geometry.area == 0:
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
    landuse_polygons["Уровень урбанизации"] = None

    development_values = landuse_polygons[development_types].apply(pd.to_numeric, errors="coerce")
    landuse_polygons["Застройка"] = development_values.idxmax(axis=1).where(development_values.max(axis=1) > 0.0)

    conditions = [
        (landuse_polygons["landuse_zone"] == "Residential") & (landuse_polygons["Многоэтажная"] > 30.00),
        (landuse_polygons["landuse_zone"] == "Residential") & (landuse_polygons["Среднеэтажная"] > 40.00),
        (landuse_polygons["landuse_zone"] == "Special"),

        (landuse_polygons["Процент профильных объектов"].isna()),
        (landuse_polygons["Процент профильных объектов"] == 0.0),
        (landuse_polygons["Процент профильных объектов"] < 10.00),
        (landuse_polygons["Процент профильных объектов"] < 25.00),
        (landuse_polygons["Процент профильных объектов"] < 75.00),
        (landuse_polygons["Процент профильных объектов"] < 90.00),

        (landuse_polygons["Процент профильных объектов"] >= 90.00),
    ]

    urbanization_levels = [
        "Высоко урбанизированная территория",  # для Residential с Многоэтажной > 30%
        "Высоко урбанизированная территория",  # для Residential с Среднеэтажной > 40%
        "Высоко урбанизированная территория",  # для Special

        "Мало урбанизированная территория",  # данных нет или 0
        "Мало урбанизированная территория",
        "Мало урбанизированная территория",  # <10%
        "Слабо урбанизированная территория",  # <25%
        "Средне урбанизированная территория",  # <75%
        "Хорошо урбанизированная территория",  # <90%
        "Высоко урбанизированная территория",  # >=90%
    ]

    landuse_polygons["Уровень урбанизации"] = np.select(
        conditions,
        urbanization_levels,
        default="Мало урбанизированная территория"
    )

    return landuse_polygons


async def analyze_geojson_for_renovation_potential(
        landuse_polygons: gpd.GeoDataFrame,
        selected_profile_to_exclude: str = None,
        ) -> gpd.GeoDataFrame:
    """
    Analyze geodata to determine renovation potential and calculate a global "discomfort" coefficient.

    Parameters:
    landuse_polygons (GeoDataFrame): Input data with geometry and attributes.
    selected_profile_to_exclude (str): Profile to exclude from renovation.

    Returns:
    GeoDataFrame: Processed data with updated calculations and columns.
    """
    utm_crs = landuse_polygons.estimate_utm_crs()
    landuse_polygons = landuse_polygons.to_crs(utm_crs)
    landuse_polygons["Площадь"] = landuse_polygons.geometry.area
    landuse_polygons["Потенциал"] = "Подлежащие реновации"

    conditions = [
        (landuse_polygons["landuse_zone"] == "Recreation") &
        (landuse_polygons["Уровень урбанизации"] == "Высоко урбанизированная территория"),
        landuse_polygons["landuse_zone"] == "Special",
        (landuse_polygons["landuse_zone"] == "Residential") &
        (landuse_polygons["Многоэтажная"] > 50.00),
        landuse_polygons["Уровень урбанизации"] == "Средне урбанизированная территория",
        landuse_polygons["Уровень урбанизации"] == "Хорошо урбанизированная территория",
        landuse_polygons["Уровень урбанизации"] == "Высоко урбанизированная территория",
    ]

    if selected_profile_to_exclude:
        conditions.append(landuse_polygons["landuse_zone"] == selected_profile_to_exclude)
        if selected_profile_to_exclude in landuse_polygons["landuse_zone"].unique():
            landuse_polygons.loc[
                landuse_polygons["landuse_zone"] == selected_profile_to_exclude, "Потенциал"
            ] = None

    combined_condition = np.logical_or.reduce(conditions)
    landuse_polygons.loc[combined_condition, "Потенциал"] = None
    total_area = landuse_polygons["Площадь"].sum()
    renovation_area = landuse_polygons.loc[landuse_polygons["Потенциал"].isnull(), "Площадь"].sum()

    landuse_polygons["Неудобия"] = (renovation_area / total_area * 100) if total_area > 0 else 0
    landuse_polygons = landuse_polygons[landuse_polygons["Площадь"] > 0]
    landuse_polygons["Площадь"] = landuse_polygons["Площадь"].round(2)
    landuse_polygons = landuse_polygons.to_crs(epsg=4236)

    return landuse_polygons

def _calc_building_percentages_core(storeys: np.ndarray) -> dict[str, float]:
    """
    Вход — чистый numpy-массив чисел этажности (float, без NaN).
    Возвращает dict с четырьмя категориями.
    """
    if storeys.size == 0:
        return {"ИЖС": 0, "Малоэтажная": 0, "Среднеэтажная": 0, "Многоэтажная": 0}

    # бинарные границы
    bins = np.array([0, 2, 4, 8, np.inf], dtype=float)
    # получаем индексы корзин
    idx = np.searchsorted(bins, storeys, side="right") - 1
    counts = np.bincount(idx, minlength=4)
    total = storeys.size
    pct = counts / total * 100
    return dict(zip(
        ["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"],
        pct
    ))

def calculate_building_percentages_optimized(buildings_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Optimized function to calculate building percentages by storeys category.

    Parameters:
    buildings_gdf (gpd.GeoDataFrame): GeoDataFrame with buildings.

    Returns:
    pd.Series: Series with percentages of categorized buildings.
    """
    if buildings_gdf.empty:
        return pd.Series({"ИЖС": 0, "Малоэтажная": 0, "Среднеэтажная": 0, "Многоэтажная": 0})

    mask = (
            (buildings_gdf["object_type"] == "Жилой дом") &
            buildings_gdf["storeys_count"].notna()
    )
    arr = buildings_gdf.loc[mask, "storeys_count"].to_numpy(dtype=float)
    result = _calc_building_percentages_core(arr)
    return pd.Series(result)


def calculate_profiled_by_criteria(
        matches_df: pd.DataFrame,
        zone_area: float,
        criteria_list: list[dict],
        object_area_col: str = "object_area"
) -> float:
    """
    Calculate the percentage of a zone’s area covered by objects matching given criteria.

    Args:
        matches_df (pd.DataFrame):
            DataFrame of candidate objects. Must include a column (default “object_area”)
            with pre-computed area for each object in the same CRS as zone_area.
        zone_area (float):
            Total area of the zone (in same units as object_area).
        criteria_list (list[dict]):
            List of filter dictionaries. Each dict may contain keys:
            - "physical_object_type_id" (optional)
            - "service_type_id" (optional)
        object_area_col (str, optional):
            Name of the column in matches_df with each object’s area. Defaults to "object_area".

    Returns:
        float:
            Percentage of zone_area occupied by all objects matching **any** of the criteria
            (i.e. union of both type- and service-based masks), clipped to [0, 100].
    """
    if zone_area == 0 or matches_df.empty or not criteria_list:
        return 0.0

    # собираем все нужные id сразу
    obj_ids = [c["physical_object_type_id"] for c in criteria_list if c.get("physical_object_type_id") is not None]
    srv_ids = [c["service_type_id"] for c in criteria_list if c.get("service_type_id") is not None]

    mask = pd.Series(False, index=matches_df.index)
    if obj_ids:
        mask |= matches_df["object_type_id"].isin(obj_ids)
    if srv_ids:
        mask |= matches_df["service_id"].isin(srv_ids)

    if not mask.any():
        return 0.0

    profiled_area = matches_df.loc[mask, object_area_col].sum()
    return profiled_area / zone_area * 100.0


async def process_zones_with_bulk_update(
    landuse_polygons: gpd.GeoDataFrame,
    physical_objects: gpd.GeoDataFrame,
    zone_mapping: dict[str, list[dict]]
) -> gpd.GeoDataFrame:
    """
    Asynchronously compute building metrics for each land-use zone and update the GeoDataFrame in bulk.

    Args:
        landuse_polygons (gpd.GeoDataFrame):
            GeoDataFrame of land-use zones with a “geometry” column.
        physical_objects (gpd.GeoDataFrame):
            GeoDataFrame of physical objects (with geometry and attributes).
        zone_mapping (dict[str, list[dict]]):
            Mapping from landuse_zone names to lists of criteria dicts (as in calculate_profiled_by_criteria).

    Returns:
        gpd.GeoDataFrame:
            The input landuse_polygons extended with columns:
            - Building percentages by type (e.g. “ИЖС”, “Малоэтажная”, …)
            - “Процент профильных объектов” (profiled area %)
            - “Любые здания /на зону” (total building area %)
            All percentage values are clipped to the [0, 100] range.
    """
    def _sync_bulk(zones_gdf, phys_gdf, mapping):
        utm_crs = zones_gdf.estimate_utm_crs()
        phys = phys_gdf.to_crs(utm_crs).copy()
        zones = zones_gdf.to_crs(utm_crs).copy().reset_index(drop=True)

        zones["zone_area"] = zones.geometry.area
        zones["zone_id"] = zones.index
        metric_cols = [
            "ИЖС","Малоэтажная","Среднеэтажная","Многоэтажная",
            "Процент профильных объектов","Любые здания /на зону"
        ]
        drop_existing = [c for c in metric_cols if c in zones.columns]
        if drop_existing:
            zones = zones.drop(columns=drop_existing)

        phys["object_area"] = phys.geometry.area

        joined = gpd.sjoin(
            phys,
            zones[["zone_id", "geometry"]],
            how="inner",
            predicate="intersects"
        )

        if joined.empty:
            result = zones.copy()
            for c in metric_cols:
                result[c] = 0.0
            return (
                result
                .drop(columns=["zone_id", "zone_area"])
                .to_crs(zones_gdf.crs)
            )

        agg_list = []
        for zone_id, group in joined.groupby("zone_id"):
            pct = calculate_building_percentages(group)
            criteria = mapping.get(zones.at[zone_id, "landuse_zone"], [])
            prof_pct = (
                calculate_profiled_by_criteria(
                    group,
                    zone_area=zones.at[zone_id, "zone_area"],
                    criteria_list=criteria,
                    object_area_col="object_area"
                )
                if criteria else 0.0
            )
            total_pct = calculate_total_building_area(group, zones.loc[zone_id])

            agg_list.append({
                "zone_id": zone_id,
                **pct.to_dict(),
                "Процент профильных объектов": prof_pct,
                "Любые здания /на зону": total_pct,
            })

        metrics_df = pd.DataFrame(agg_list)
        if "zone_id" in metrics_df:
            metrics_df = metrics_df.set_index("zone_id")
        else:
            metrics_df = pd.DataFrame(index=zones["zone_id"])

        result = zones.join(metrics_df, on="zone_id")
        for c in metric_cols:
            if c not in result.columns:
                result[c] = 0.0
            result[c] = result[c].clip(0, 100)

        return (
            result
            .drop(columns=["zone_id", "zone_area"])
            .to_crs(zones_gdf.crs)
        )

    return await asyncio.to_thread(
        _sync_bulk,
        landuse_polygons,
        physical_objects,
        zone_mapping
    )


async def get_renovation_potential(
    project_id: int,
    is_context: bool,
    profile: Optional[Profile] = None,
    scenario_id: bool = False,
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

    profile_key = str(profile) if profile is not None else "no_profile"

    if source is None:
        base_scenario_id = await get_projects_base_scenario_id(project_id)
        source_data = await get_functional_zone_sources(base_scenario_id)
        source_key = source_data["source"]
    else:
        source_key = source

    cache_name = f"renovation_potential_project-{project_id}_is_context-{is_context}"
    cache_file = caching_service.get_recent_cache_file(cache_name, {"profile": profile_key, "source": source_key})

    if cache_file and caching_service.is_cache_valid(cache_file):
        logger.info(f"Using cached renovation potential for project {project_id}")
        cached_data = caching_service.load_cache(cache_file)
        return gpd.GeoDataFrame.from_features(cached_data, crs="EPSG:4326")

    physical_objects_dict, landuse_polygons = await asyncio.gather(
        data_extraction.extract_physical_objects(project_id, is_context),
        data_extraction.extract_landuse(project_id, is_context, scenario_id, source)
    )
    physical_objects = physical_objects_dict["physical_objects"]
    utm_crs = physical_objects.estimate_utm_crs()
    physical_objects = physical_objects.to_crs(utm_crs)
    landuse_polygons = landuse_polygons.to_crs(utm_crs)

    logger.info("Функциональные зоны и физические объекты получены")
    landuse_polygons = landuse_polygons[landuse_polygons.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    landuse_polygons["Процент профильных объектов"] = 0.0
    landuse_polygons["Любые здания /на зону"] = 0.0
    logger.info("Функциональные зоны и физические объекты отфильтрованы")

    landuse_polygons = await process_zones_with_bulk_update(landuse_polygons, physical_objects,
                                                            actual_zone_mapping)
    logger.info("Проценты зданий посчитаны")

    landuse_polygons = await assign_development_type(landuse_polygons)
    logger.info("Уровень урбанизации присвоен")

    profile_for_analysis = str(profile) if profile is not None else None

    landuse_polygons_ren_pot = await analyze_geojson_for_renovation_potential(landuse_polygons, profile_for_analysis)
    logger.info("Потенциал для реновации рассчитан")

    zones = landuse_polygons_ren_pot.to_crs(utm_crs)
    zones["Converted"] = None

    oop_objects = physical_objects[physical_objects["service_id"] == 4]
    if not oop_objects.empty:
        oop_objects = oop_objects.to_crs(zones.crs)
        oop_join = gpd.sjoin(zones, oop_objects, how="inner", predicate="intersects")
        if not oop_join.empty:
            oop_zone_ids = oop_join["functional_zone_id"].unique()
            zones.loc[zones["functional_zone_id"].isin(oop_zone_ids), "Потенциал"] = "Не подлежащие реновации"
            zones.loc[zones["functional_zone_id"].isin(oop_zone_ids), "Процент урбанизации"] = "Высоко урбанизированная территория"

    non_renovated = zones[pd.isna(zones['Потенциал'])]
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
    if joined.empty:
        logger.info("No intersections between buffers and polygons were found,"
                    " returning polygons without intersections")
        landuse_polygons_ren_pot = zones.to_crs(epsg=4326)

        result_json = json.loads(landuse_polygons_ren_pot.to_json())
        caching_service.save_with_cleanup(
            result_json, cache_name,
            {"profile": profile_key,
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
            raise http_exception(500, "Error while searching for intersections between buffers and polygons", e)

    grouped = joined.groupby(joined.index)['intersection_area'].sum()
    final_overlap_ratio = grouped / renovated.loc[grouped.index].geometry.area
    to_update = final_overlap_ratio[final_overlap_ratio > 0.50].index
    mask_renovation = zones.index.isin(to_update)
    zones.loc[mask_renovation & zones['Потенциал'].notnull(), 'Потенциал'] = \
        'Не подлежащие реновации'
    zones.loc[to_update, 'Converted'] = True
    landuse_polygons_ren_pot = zones.to_crs(epsg=4326)

    result_json = json.loads(landuse_polygons_ren_pot.to_json())
    caching_service.save_with_cleanup(
        result_json, cache_name,
        {"profile": profile_key,
         "source": source_key})

    return landuse_polygons_ren_pot


async def filter_response(polygons_gdf: gpd.GeoDataFrame, filter_type: bool = False) -> gpd.GeoDataFrame:
    """
    Filters a GeoDataFrame based on specific criteria.

    Args:
        polygons_gdf (gpd.GeoDataFrame): The input GeoDataFrame with land use polygons to filter.
        filter_type (bool): If True, applies a filter for renovation potential columns; otherwise sets to False - urbanization level
    Returns:
        gpd.GeoDataFrame: The filtered GeoDataFrame
    """
    columns_mapping = {
        "zone_type_nickname": "Тип землепользования",
        "Процент профильных объектов": "Доля профильных объектов на территории",
        "Любые здания /на зону": "Доля любых объектов на территории",
        "Застройка": "Доминирующий тип застройки",
        "Площадь": "Площадь",
        "Потенциал": "Потенциал реновации"
    }

    if filter_type: # реновация
        required_columns = [
            "Тип землепользования",
            "Уровень урбанизации",
            "Пояснение уровня урбанизации",
            "Потенциал реновации",
            "Пояснение потенциала реновации",
            "Площадь",
            "geometry"
        ]
        polygons_gdf = polygons_gdf.rename(columns=columns_mapping)
        polygons_gdf = polygons_gdf[required_columns]
        polygons_gdf["Потенциал реновации"] = polygons_gdf["Потенциал реновации"].fillna("Не подлежащие реновации")
        polygons_gdf.geometry = await SpatialMethods.round_coords_geom(polygons_gdf.geometry, 6)
    else: # урбанизация
        required_columns = [
            "Тип землепользования",
            "Уровень урбанизации",
            "Пояснение уровня урбанизации",
            "Площадь",
            "geometry"
        ]
        polygons_gdf = polygons_gdf.rename(columns=columns_mapping)
        polygons_gdf = polygons_gdf[required_columns]
        polygons_gdf.geometry = await SpatialMethods.round_coords_geom(polygons_gdf.geometry, 6)

    return polygons_gdf


async def calculate_zone_percentages(scenario_id: int, is_context: bool = False, source: str = None) -> dict:
    """
    Calculates the percentage of the total area occupied by each unique landuse zone,
    including water objects, forests, and green objects, and ensures all predefined categories are present in the output.

    Args:
        scenario_id (int): ID of the project to process.
        is_context (bool): Whether to include contextual data.

    Returns:
        dict: A dictionary with the percentages for each unique landuse zone.
    """
    if scenario_id:
        physical_objects_dict, landuse_polygons = await asyncio.gather(
            data_extraction.extract_physical_objects(scenario_id, is_context, True),
            data_extraction.extract_landuse(scenario_id, is_context, True, source)
        )
    else:
        physical_objects_dict, landuse_polygons = await asyncio.gather(
            data_extraction.extract_physical_objects(scenario_id, is_context),
            data_extraction.extract_landuse(scenario_id, is_context, False, source)
        )
    water_objects = physical_objects_dict["water_objects"]
    green_objects = physical_objects_dict["green_objects"]  # новое
    forests = physical_objects_dict["forests"]  # новое
    utm_crs = landuse_polygons.estimate_utm_crs()
    landuse_polygons = landuse_polygons.to_crs(utm_crs)

    landuse_polygons["landuse_zone"] = landuse_polygons["landuse_zone"].replace({None: "Residential", "null": "Residential"}).fillna("Residential")
    landuse_polygons["area"] = landuse_polygons.geometry.area
    total_area_landuse = landuse_polygons["area"].sum()

    zone_area = landuse_polygons.groupby("landuse_zone")["area"].sum()
    zone_percentages = (zone_area / (total_area_landuse + water_objects + green_objects + forests) * 100).to_dict()

    water_percentage = (water_objects / (total_area_landuse + water_objects + green_objects + forests)) * 100
    green_objects_percentage = (green_objects / (total_area_landuse + water_objects + green_objects + forests)) * 100
    forests_percentage = (forests / (total_area_landuse + water_objects + green_objects + forests)) * 100

    zone_percentages["Water Objects"] = water_percentage
    zone_percentages["Green Objects"] = green_objects_percentage
    zone_percentages["Forests"] = forests_percentage

    predefined_zones = ["Industrial", "Residential", "Special", "Recreation", "Agriculture", "Business", "Transport"]
    for zone in predefined_zones:
        if zone not in zone_percentages:
            zone_percentages[zone] = 0.0
    zone_percentages = {key: round(value, 2) for key, value in zone_percentages.items()}

    zone_mapping = {
        "Industrial": "Земли промышленного назначения",
        "Residential": "Земли жилой застройки",
        "Special": "Земли специального назначения",
        "Recreation": "Земли рекреационного назначения",
        "Agriculture": "Земли сельскохозяйственного назначения",
        "Business": "Земли общественно-делового назначения",
        "Transport": "Земли транспортного назначения",
        "Water Objects": "Земли водного фонда",
        "Green Objects": "Земли зелёных насаждений",
        "Forests": "Земли лесных массивов"
    }

    other_categories_total = 0.0
    filtered_zone_percentages = {}

    for key, value in zone_percentages.items():
        mapped_key = zone_mapping.get(key)
        if mapped_key:
            filtered_zone_percentages[mapped_key] = round(value, 2)
        else:
            other_categories_total += value

    filtered_zone_percentages["Иные категории земель"] = round(other_categories_total, 2)

    return filtered_zone_percentages


async def get_projects_renovation_potential(project_id: int, source: str = None) -> dict:
    """Calculate renovation potential for project and include discomfort as a separate key."""
    landuse_polygons = await get_renovation_potential(project_id, is_context=False, source=source)
    discomfort_value = (
        round(landuse_polygons["Неудобия"].iloc[0], 2)
        if "Неудобия" in landuse_polygons.columns and not landuse_polygons["Неудобия"].isna().iloc[0]
        else None
    )
    landuse_polygons = await interpretation_service.interpret_urbanization_value(landuse_polygons)
    landuse_polygons = await interpretation_service.interpret_renovation_value(landuse_polygons)
    landuse_polygons = await filter_response(landuse_polygons, True)
    geojson = GeoJSON.from_geodataframe(landuse_polygons)

    response = {
        "geojson": geojson,
        "discomfort": discomfort_value
    }

    return response


async def get_projects_urbanization_level(project_id: int, source: str = None) -> GeoJSON:
    """Calculate urbanization level for project."""
    logger.info(f"Calculating urbanization level for project {project_id}")
    landuse_polygons = await get_renovation_potential(project_id, is_context=False, source=source)
    landuse_polygons = await interpretation_service.interpret_urbanization_value(landuse_polygons)
    landuse_polygons = await interpretation_service.interpret_renovation_value(landuse_polygons)
    landuse_polygons = await filter_response(landuse_polygons)
    return GeoJSON.from_geodataframe(landuse_polygons)


async def get_projects_context_renovation_potential(project_id: int, source: str = None) -> dict:
    """Calculate renovation potential for project's context."""
    logger.info(f"Calculating renovation potential for project {project_id}")
    landuse_polygons = await get_renovation_potential(project_id, is_context=True, source=source)

    discomfort_value = (
        round(landuse_polygons["Неудобия"].iloc[0], 2)
        if "Неудобия" in landuse_polygons.columns and not landuse_polygons["Неудобия"].isna().iloc[0]
        else None
    )
    landuse_polygons = await interpretation_service.interpret_urbanization_value(landuse_polygons)
    landuse_polygons = await interpretation_service.interpret_renovation_value(landuse_polygons)
    landuse_polygons = await filter_response(landuse_polygons, True)
    geojson = GeoJSON.from_geodataframe(landuse_polygons)

    response = {
        "geojson": geojson,
        "discomfort": discomfort_value
    }

    return response


async def get_projects_context_urbanization_level(project_id: int, source: str = None) -> GeoJSON:
    """Calculate urbanization level for project's context."""
    logger.info(f"Calculating urbanization level for project {project_id}")
    landuse_polygons = await get_renovation_potential(project_id, is_context=True, source=source)
    landuse_polygons = await interpretation_service.interpret_urbanization_value(landuse_polygons)
    landuse_polygons = await interpretation_service.interpret_renovation_value(landuse_polygons)
    landuse_polygons = await filter_response(landuse_polygons)
    return GeoJSON.from_geodataframe(landuse_polygons)


async def get_projects_landuse_parts_scen_id_main_method(scenario_id: int, source: str = None) -> dict:
    logger.info(f"Calculating landuse parts for scenario {scenario_id}")
    landuse_parts = await calculate_zone_percentages(scenario_id, source=source)
    return landuse_parts