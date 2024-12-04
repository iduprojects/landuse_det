"""Geojson response models are defined here."""

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import shapely.geometry as geom
from geojson_pydantic import Feature, FeatureCollection


@dataclass
class GeoJSON:
    features: list[Feature]
    type: str = "FeatureCollection"

    def as_dict(self) -> dict[str, Any]:
        features = []
        for feature in self.features:
            features.append(feature.as_dict())
        self.features = features
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def empty(cls) -> "GeoJSON":
        return cls(features=[])


@dataclass
class Feature:
    geometry: geom.MultiPolygon | geom.Polygon
    properties: dict[str, Any] = field(default_factory=dict)
    type: str = "Feature"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, indent=2)
