import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Polygon
from shapely.ops import polygonize


def _polygons_to_linestring(geom):
    def convert_polygon(polygon: Polygon):
        lines = []
        exterior = LineString(polygon.exterior.coords)
        lines.append(exterior)
        interior = [LineString(p.coords) for p in polygon.interiors]
        lines = lines + interior
        return lines

    def convert_multipolygon(polygon: MultiPolygon):
        return MultiLineString(sum([convert_polygon(p) for p in polygon.geoms], []))

    if geom.geom_type == "Polygon":
        return MultiLineString(convert_polygon(geom))
    if geom.geom_type == "MultiPolygon":
        return convert_multipolygon(geom)
    return geom


def _combine_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    crs = gdf.crs
    polygons = polygonize(gdf["geometry"].apply(_polygons_to_linestring).unary_union)
    enclosures = gpd.GeoSeries(list(polygons), crs=crs)
    enclosures_points = gpd.GeoDataFrame(enclosures.representative_point(), columns=["geometry"], crs=crs)
    joined = gpd.sjoin(enclosures_points, gdf, how="inner", predicate="within").reset_index()
    cols = joined.columns.tolist()
    cols.remove("geometry")
    joined = joined.groupby("index").agg({column: list for column in cols}).reset_index(drop=True)
    joined["geometry"] = enclosures
    joined = gpd.GeoDataFrame(joined, geometry="geometry", crs=crs)
    return joined


def calculate_intersection_percentage(gdf_landuse, gdf_zones):
    gdf_landuse = gdf_landuse.to_crs(epsg=32636)
    gdf_zones = gdf_zones.to_crs(epsg=32636)

    gdf_zones = gdf_zones.explode().drop_duplicates(subset="geometry")

    gdf_zones["Процент пересечения"] = 0.0
    intersecting_geometries = []

    spatial_index = gdf_landuse.sindex

    # Проверяем пересечение каждого полигона gdf_zones с полигонами gdf_landuse
    for idx_zone, zone_row in gdf_zones.iterrows():
        total_intersection_area = 0.0
        total_area_zone = zone_row.geometry.area

        possible_matches_index = list(spatial_index.intersection(zone_row.geometry.bounds))
        possible_matches = gdf_landuse.iloc[possible_matches_index]

        for idx_landuse, landuse_row in possible_matches.iterrows():
            # Проверяем пересечение и соответствие landuse_zon
            if zone_row["landuse_zon"] == landuse_row["landuse_zon"]:
                intersection = landuse_row.geometry.intersection(zone_row.geometry)

                if not intersection.is_empty:
                    intersection_area = intersection.area
                    total_intersection_area += intersection_area
                    intersecting_geometries.append(intersection)

        if total_area_zone > 0:
            percentage = (total_intersection_area / total_area_zone) * 100
            gdf_zones.at[idx_zone, "Процент пересечения"] = min(percentage, 100)
        total_percentage = 100 - gdf_zones["Процент пересечения"].mean()
        gdf_zones["Общий процент"] = total_percentage

    intersection_geometries_gdf = gpd.GeoDataFrame(geometry=intersecting_geometries, crs=gdf_landuse.crs)
    intersection_geometries_gdf = intersection_geometries_gdf.explode().drop_duplicates(subset="geometry")
    intersection_geometries_gdf = _combine_geometry(intersection_geometries_gdf)
    intersection_geometries_gdf = gpd.GeoDataFrame(
        intersection_geometries_gdf, geometry="geometry", crs=gdf_landuse.crs
    )
    intersection_geometries_gdf["Значение неудобий"] = 1
    gdf_zones_copy = gdf_zones.copy()
    gdf_zones_copy["Значение неудобий"] = 0

    geometry_difference = gpd.overlay(gdf_zones_copy, intersection_geometries_gdf, how="difference")

    combined_geometries = pd.concat([geometry_difference, intersection_geometries_gdf], ignore_index=True)

    # Создаем буферы и проверяем пересечения
    for idx, row in combined_geometries.iterrows():
        if row["Значение неудобий"] == 1:
            current_geometry = row.geometry
            buffer_geometry = current_geometry.buffer(350)

            intersecting_polygons = combined_geometries[
                (combined_geometries["Значение неудобий"] == 0)
                & (combined_geometries.geometry.intersects(buffer_geometry))
            ]

            for idx2, small_row in intersecting_polygons.iterrows():
                intersection_area = small_row.geometry.intersection(buffer_geometry).area
                small_polygon_area = small_row.geometry.area

                if intersection_area / small_polygon_area > 0.1 and small_polygon_area <= 100000:
                    combined_geometries.loc[idx2, "Значение неудобий"] = 1

    # Объединяем геометрии по значению неудобий
    dissolved_geometries = combined_geometries.dissolve(by="Значение неудобий")
    dissolved_geometries["geometry"] = dissolved_geometries["geometry"].buffer(0.01)
    dissolved_geometries.to_crs(4326)
    gdf_zones.to_crs(4326)
    return dissolved_geometries, gdf_zones
