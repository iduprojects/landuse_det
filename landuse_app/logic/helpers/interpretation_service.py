import geopandas as gpd
import numpy as np

class InterpretationService:
    async def interpret_urbanization_value(self, landuse_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        required_columns = ["Любые здания /на зону", "landuse_zone", "Многоэтажная"]
        development_types = ["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"]
        missing_columns = [col for col in required_columns + development_types if col not in landuse_polygons.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        conditions = [
            (landuse_polygons["landuse_zone"] == "Residential") & (landuse_polygons["Многоэтажная"] > 30.00) & (landuse_polygons["Уровень урбанизации"] == "Высоко урбанизированная территория"),
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
            "На территории доминирует многоэтажный тип застройки, что делает уровень урбанизации высоким",
            "На территории доминирует среднеэтажный тип застройки, что делает уровень урбанизации высоким",
            "На территории расположены объекты специального назначения, что делает уровень урбанизации высоким",

            "На территории нет профильных объектов, поэтому она мало урбанизирована",
            "На территории нет профильных объектов, поэтому она мало урбанизирована",
            "Профильные объекты занимают <10% площади территории, поэтому она мало урбанизирована",
            "Профильные объекты занимают <25% площади территории, поэтому она слабо урбанизирована",
            "Профильные объекты занимают ≈75% площади территории, поэтому она урбанизирована на среднем уровне",
            "Профильные объекты занимают ≈90% площади территории, поэтому она хорошо урбанизирована",
            "Профильные объекты занимают >90% площади территории, поэтому она высоко урбанизирована",
        ]

        landuse_polygons["Пояснение уровня урбанизации"] = np.select(
            conditions,
            urbanization_levels,
            default="На территории нет профильных объектов, поэтому территория мало урбанизирована"
        )

        return landuse_polygons

    async def interpret_renovation_value(self, landuse_polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        required_columns = ["Любые здания /на зону", "landuse_zone", "Многоэтажная"]
        development_types = ["ИЖС", "Малоэтажная", "Среднеэтажная", "Многоэтажная"]
        missing_columns = [col for col in required_columns + development_types if col not in landuse_polygons.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        conditions = [
            #Converted
            landuse_polygons["Converted"] == True,

            #Residential / Special
            (landuse_polygons["landuse_zone"] == "Residential")
            & (landuse_polygons["Многоэтажная"] > 30.00)
            & (landuse_polygons["Уровень урбанизации"] == "Высоко урбанизированная территория"),
            (landuse_polygons["landuse_zone"] == "Residential")
            & (landuse_polygons["Среднеэтажная"] > 40.00),
            (landuse_polygons["landuse_zone"] == "Special"),

            #Нет объектов или 0%
            landuse_polygons["Процент профильных объектов"].isna(),
            landuse_polygons["Процент профильных объектов"] == 0.0,

            #Диапазоны по доле
            landuse_polygons["Процент профильных объектов"] < 10.00,
            landuse_polygons["Процент профильных объектов"] < 25.00,
            landuse_polygons["Процент профильных объектов"] < 75.00,
            landuse_polygons["Процент профильных объектов"] < 90.00,
            landuse_polygons["Процент профильных объектов"] >= 90.00,
        ]

        urbanization_levels = [
            #Converted
            "Территория не подлежит реновации, так как находится в зоне влияния высоко урбанизированной территории",

            #Residential / Special
            "Территория используется эффективно и не подлежит реновации",
            "Территория используется эффективно и не подлежит реновации",
            "На территории находятся объекты специального назначения не подлежащие реновации",

            #Нет объектов или 0%
            "Территория используется неэффективно и подлежит реновации",
            "Территория используется неэффективно и подлежит реновации",

            #Диапазоны по доле
            "Территория используется неэффективно и подлежит реновации",
            "Территория используется неэффективно и подлежит реновации",
            "Территория используется эффективно и не подлежит реновации",
            "Территория используется эффективно и не подлежит реновации",
            "Территория используется эффективно и не подлежит реновации",
        ]

        landuse_polygons["Пояснение потенциала реновации"] = np.select(
            conditions,
            urbanization_levels,
            default="На территории отсутствуют профильные объекты, соответственно, она мало урбанизированная"
        )

        return landuse_polygons

interpretation_service = InterpretationService()