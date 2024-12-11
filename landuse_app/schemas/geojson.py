"""Geojson response models are defined here."""

from typing import Any, Literal

import geopandas as gpd
from geojson_pydantic import Feature, FeatureCollection
from shapely.geometry import mapping


class GeoJSON(FeatureCollection):
    type: Literal["FeatureCollection"] = "FeatureCollection"

    @classmethod
    def from_geometry(cls, geometry: dict[str, Any]) -> "GeoJSON":
        return cls(features=[Feature(type="Feature", geometry=geometry, properties={})])

    @classmethod
    def from_features_list(cls, features: list[dict[str, Any]]) -> "GeoJSON":
        feature_collection = []
        for feature in features:
            properties = dict(feature)
            geometry = properties.pop("geometry", None)
            feature_collection.append(Feature(type="Feature", geometry=geometry, properties=properties))
        return cls(features=feature_collection)

    @classmethod
    def from_geodataframe(cls, gdf: gpd.GeoDataFrame) -> "GeoJSON":
        feature_collection = []
        for _, row in gdf.iterrows():
            geometry = mapping(row.geometry)
            properties = row.drop("geometry").to_dict()
            feature_collection.append(Feature(type="Feature", geometry=geometry, properties=properties))
        return cls(features=feature_collection)
