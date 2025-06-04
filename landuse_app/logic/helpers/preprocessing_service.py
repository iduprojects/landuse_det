import random
import geopandas as gpd
import pandas as pd
from loguru import logger
from shapely.geometry import shape
from .urban_api_access import get_functional_zones_territory_id, get_physical_objects_from_territory_parallel, \
    get_all_physical_objects_geometries_scen_id_percentages, get_all_physical_objects_geometries, \
    get_functional_zones_scen_id_percentages, get_functional_zones_scenario_id, get_services_geojson
from ...exceptions.http_exception_wrapper import http_exception


class PreProcessingService:
    @staticmethod
    async def extract_physical_objects(project_id: int, is_context: bool, scenario_id_flag: bool = False) -> dict[
        str, gpd.GeoDataFrame]:
        """
        Extracts and processes physical objects for a given project from GeoJson,
        handling geometries and object attributes.

        Parameters:
        project_id : int
            The ID of the project for which physical objects are to be extracted.
        is_context : bool
            Flag indicating whether to fetch context-based data.

        Returns:
        dict[str, gpd.GeoDataFrame]
            Словарь с обработанным GeoDataFrame и площадями для водных, зелёных и лесных объектов.
        """
        logger.info("Физические объекты загружаются")
        if scenario_id_flag:
            resp = await get_all_physical_objects_geometries_scen_id_percentages(project_id)
        else:
            resp = await get_all_physical_objects_geometries(project_id, is_context)

        all_data: list[dict] = []
        for feature in resp.get("features", []):
            geom_json = feature.get("geometry")
            props = feature.get("properties", {})

            try:
                geom = shape(geom_json)
                if not geom.is_valid:
                    geom = geom.buffer(0)
                if not geom.is_valid or geom.is_empty:
                    continue
            except Exception as e:
                logger.error(f"Ошибка при обработке геометрии: {e}")
                continue

            for phys in props.get("physical_objects", []):
                base = {
                    "physical_object_id": phys.get("physical_object_id"),
                    "object_type": phys.get("physical_object_type", {}).get("name", "Unknown"),
                    "object_type_id": phys.get("physical_object_type", {}).get("id"),
                    "name": phys.get("name", "(unnamed)"),
                    "geometry_type": geom.geom_type,
                    "geometry": geom,
                    "category": None,
                    "storeys_count": None,
                    "living_area": None,
                    "service_id": None,
                    "service_name": None,
                    "is_capacity_real": None,
                }

                building = phys.get("building")
                if building:
                    b_props = building.get("properties", {})
                    osm = b_props.get("osm_data", {})
                    floors = building.get("floors")
                    levels = osm.get("building:levels")
                    count = b_props.get("storeys_count")

                    if floors:
                        final_floors = floors
                    elif count:
                        final_floors = count
                    elif levels:
                        final_floors = int(levels)
                    else:
                        final_floors = random.randint(2, 5)

                    base.update({
                        "category": "residential",
                        "storeys_count": final_floors,
                        "living_area": b_props.get("living_area_official") or b_props.get("living_area_modeled"),
                        "address": b_props.get("address", props.get("address")),
                    })
                    all_data.append(base)
                    continue

                services = props.get("services", [])
                if services:
                    for svc in services:
                        svc_type = svc.get("service_type", {}) or {}
                        sid = svc_type.get("id", "Unknown")
                        sname = svc_type.get("name", "Unknown")
                        cap_real = svc.get("is_capacity_real")

                        row = base.copy()
                        row.update({
                            "category": "non_residential",
                            "service_id": sid,
                            "service_name": sname,
                            "is_capacity_real": cap_real,
                            "object_type": sname,
                        })
                        all_data.append(row)
                    continue

                base.update({"category": "other"})
                all_data.append(base)

        logger.info("Физические объекты загружены")
        df = pd.DataFrame(all_data)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").drop_duplicates("physical_object_id")
        gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])]

        local_crs = gdf.estimate_utm_crs()
        water = gdf[gdf['object_type_id'].isin([45, 2, 44])].to_crs(local_crs).area.sum()
        green = gdf[gdf['object_type_id'].isin([47, 3])].to_crs(local_crs).area.sum()
        forests = gdf[gdf['object_type_id'].isin([48])].to_crs(local_crs).area.sum()

        return {
            "physical_objects": gdf,
            "water_objects": water,
            "green_objects": green,
            "forests": forests
        }

    @staticmethod
    async def extract_landuse(project_id: int, is_context: bool, scenario_id_flag: bool = False, source: str = None, ) \
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
        if scenario_id_flag:
            geojson_data = await get_functional_zones_scen_id_percentages(project_id)
        else:
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

    @staticmethod
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
            logger.error(f"Error creating geometry: {e}")
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
            "service_id": None,
            "service_name": None,
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

        services = obj.get("services", [])
        if services:
            parsed = []
            for svc in services:
                svc_type = svc.get("service_type", {}) or {}
                sid = svc_type.get("id", "Unknown")
                sname = svc_type.get("name", "Unknown")
                cap_real = svc.get("is_capacity_real")

                row = object_data.copy()
                row.update({
                    "category": "non_residential",
                    "service_id": sid,
                    "service_name": sname,
                    "is_capacity_real": cap_real,
                    "object_type": sname,
                })
                parsed.append(row)

        # elif object_data["object_type_id"] == 5:
        #     services = obj.get("services", [])
        #     if services:
        #         parsed = []
        #         for service in services:
        #             tmp = object_data.copy()
        #             tmp["category"] = "non_residential"
        #             tmp["service_id"] = service.get("service_id")
        #             tmp["service_name"] = service.get("name")
        #             tmp["is_capacity_real"] = service.get("is_capacity_real")
        #             parsed.append(tmp)
        #         return parsed
        #     else:
        #         object_data["category"] = "non_residential"

        # elif object_data["object_type"] == "Рекреационная зона":
        #     object_data["category"] = "recreational"
        #
        # else:
        #     object_data["category"] = "other"

        return [object_data]

    @staticmethod
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
        logger.info("Physical objects are loading with parallel processing")
        raw_objects = await get_physical_objects_from_territory_parallel(territory_id)
        all_data = []

        for obj in raw_objects:
            parsed_objects = PreProcessingService.parse_physical_object(obj)
            all_data.extend(parsed_objects)
        if not all_data:
            raise http_exception(404, "No physical objects found for territory ID", territory_id)

        logger.success("Physical objects are loaded, creating the  GeoDataFrame")
        all_data_df = pd.DataFrame(all_data)
        all_data_gdf = gpd.GeoDataFrame(all_data_df, geometry="geometry", crs="EPSG:4326")
        all_data_gdf = all_data_gdf.drop_duplicates(subset='physical_object_id')
        all_data_gdf = all_data_gdf.dropna(subset=['geometry'])
        all_data_gdf = all_data_gdf[all_data_gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        all_data_gdf = all_data_gdf[all_data_gdf.geometry.is_valid]
        if len(all_data_gdf) < 1:
            raise http_exception(404, "No polygonal physical objects found for territory ID", territory_id)
        local_crs = all_data_gdf.estimate_utm_crs()

        water_objects_gdf = all_data_gdf[
            all_data_gdf['object_type_id'].isin([45, 2, 44])
        ].to_crs(local_crs)

        green_objects_gdf = all_data_gdf[
            all_data_gdf['object_type_id'].isin([47, 3])
        ].to_crs(local_crs)

        forests_gdf = all_data_gdf[
            all_data_gdf['object_type_id'].isin([48])
        ].to_crs(local_crs)

        logger.success("Physical objects are successfully loaded into GeoDataFrame")
        return {
            "physical_objects": all_data_gdf,
            "water_objects": water_objects_gdf.area.sum(),
            "green_objects": green_objects_gdf.area.sum(),
            "forests": forests_gdf.area.sum()
        }

    @staticmethod
    async def extract_landuse_from_territory(territory_id, source: str = None, ) \
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
        logger.info("Functional zones are loading")

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
        logger.success("Functional zones are loaded")
        return landuse_polygons

    @staticmethod
    async def extract_services(
            territory_id: int,
            service_type_ids=None
    ) -> gpd.GeoDataFrame:
        """
        Retrieve and flatten service features for the specified territory and service types.

        This asynchronous function fetches GeoJSON features for each service type ID provided,
        flattens nested properties (including service_type and urban_function metadata),
        and combines them into a single GeoDataFrame. If no services are found, an empty
        GeoDataFrame is returned.

        Args:
            territory_id (int):
                The identifier of the territory to query services for.
            service_type_ids (list[int], optional):
                A list of service_type_id values to include. Defaults to [2, 4, 1, 81]
                if None is provided.

        Returns:
            gpd.GeoDataFrame:
                A GeoDataFrame containing one row per service feature, with flattened
                attribute columns and valid Polygon/MultiPolygon geometries in an
                appropriate UTM projection. Columns osm_id, full_id, leisure, osm_type,
                capacity, and fid are dropped.
        """
        if service_type_ids is None:
            service_type_ids = [2, 4, 1, 81]
        all_features: list[dict[str, any]] = []

        for service_type in service_type_ids:
            resp = await get_services_geojson(territory_id, service_type)
            features = resp.get("features") or resp.get("results") or []
            all_features.extend(features)

        if not all_features:
            gdf = gpd.GeoDataFrame()
            logger.warning(f"No services found for territory {territory_id} and service types {service_type_ids}")
            return gdf

        records = []
        for feat in all_features:
            geom_json = feat.get("geometry")
            try:
                geom = shape(geom_json) if geom_json else None
            except Exception:
                geom = None

            props = feat.get("properties", {})
            svc_type = props.pop("service_type", {})
            uf = svc_type.pop("urban_function", {})

            territories = props.pop("territories", [])
            terr = territories[0] if territories else {}

            flat = {"service_name": props.get("name"),
                    "capacity": props.get("capacity"),
                    "service_type_id": svc_type.get("service_type_id"), "service_type_name": svc_type.get("name"),
                    **{k: v for k, v in props.get("properties", {}).items()
                       if k not in ("name", "is_capacity_real")}, "geometry": geom}

            records.append(flat)

        df = pd.DataFrame(records)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        gdf = gdf.to_crs(gdf.estimate_utm_crs())
        gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        gdf = gdf.drop(
            columns=[
                'osm_id',
                'full_id',
                'leisure',
                'osm_type',
                'capacity',
                'fid'
            ],
            errors='ignore'
        )

        return gdf

data_extraction = PreProcessingService()
