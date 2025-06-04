DEFAULT_RESIDENTIAL = {
    "functional_zone_type": {
        "id": 1,
        "name": "residential",
        "nickname": "Жилая зона"
    },
    "zone_type_id": 1,
    "zone_type_name": "residential",
    "zone_type_nickname": "Жилая зона",
    "landuse_zone": "Residential"
}

# zone_mapping = {
#     "Residential": ["Жилой дом"],
#     "Industrial": ["Промышленная территория"],
#     "Transport": ["Железнодорожный вокзал", "Аэропорт"],
#     "Business": ["Нежилое здание"],
#     "Agriculture": ["Вспаханное поле", "Поле"],
#     "Recreation": ["Травяное покрытие", "ООПТ", "Зелёная зона", "Парк"],
# }

VALID_SOURCES = {"PZZ", "OSM", "User"}

actual_zone_mapping = {
    "Residential": [
        {"physical_object_type_id": 4.0,  "service_type_id": None, "physical_object_function_id": 1.0,  "urban_function_id": None}
    ],
    "Industrial": [
        {"physical_object_type_id": 43.0, "service_type_id": None, "physical_object_function_id": 16.0, "urban_function_id": None}
    ],
    "Transport": [
        {"physical_object_type_id": None, "service_type_id": 81.0, "physical_object_function_id": None,  "urban_function_id": 30.0},
        {"physical_object_type_id": 61.0, "service_type_id": None, "physical_object_function_id": 21.0, "urban_function_id": None}
    ],
    "Business": [
        {"physical_object_type_id": 5.0,  "service_type_id": None, "physical_object_function_id": 1.0,  "urban_function_id": None}
    ],
    "Agriculture": [
        {"physical_object_type_id": 60.0, "service_type_id": None, "physical_object_function_id": 17.0, "urban_function_id": None},
        {"physical_object_type_id": 46.0, "service_type_id": None, "physical_object_function_id": 17.0, "urban_function_id": None}
    ],
    "Recreation": [
        {"physical_object_type_id": 47.0,  "service_type_id": None, "physical_object_function_id": 2.0,  "urban_function_id": None},
        {"physical_object_type_id": None,   "service_type_id": 4.0,  "physical_object_function_id": None,  "urban_function_id": 2.0},
        {"physical_object_type_id": 3.0,   "service_type_id": None, "physical_object_function_id": 2.0,  "urban_function_id": None},
        {"physical_object_type_id": None,   "service_type_id": 1.0,  "physical_object_function_id": None,  "urban_function_id": 2.0}
    ]
}
