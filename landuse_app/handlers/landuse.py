"""Main landuse handlers are defined here."""
from fastapi import Path, Query

from ..exceptions.http_exception_wrapper import http_exception
from ..logic import landuse_service
from ..logic.constants.constants import VALID_SOURCES
from ..logic.helpers import IndicatorsService
from ..schemas import GeoJSON
from .routers import renovation_router, urbanization_router, landuse_percentages_router, \
    territories_urbanization_router, indicators_router


@renovation_router.get(
    "/projects/{project_id}/renovation_potential",
    response_model=dict,
    description=(
        "Function for getting renovation potential for a project. "
        "Additionally, returns layer in GeoJSON implemented in JSON response. "
        "Args: project_id (int): unique identifier of the project. "
        "Returns: GeoJSON: renovation potential data."
    )
)
async def get_projects_renovation_potential(
    project_id: int = Path(..., description="The unique identifier of the project."),
    source: str = Query(None, description="The source of the landuse zones data. Available sources are: PZZ, OSM"),
) -> GeoJSON:
    if source is not None and source not in VALID_SOURCES:
        raise http_exception(
            422,
            f"Invalid source. Valid sources are: {', '.join(VALID_SOURCES)}",
            source
        )
    return await landuse_service.get_renovation_potential(project_id, source=source)


@urbanization_router.get(
    "/projects/{project_id}/urbanization_level",
    response_model=GeoJSON,
    description=(
        "Function for getting urbanization level for a project. "
        "Additionally, returns layer in GeoJSON implemented in JSON response. "
        "Args: project_id (int): unique identifier of the project. "
        "Returns: GeoJSON: urbanization level data."
    )
)
async def get_projects_urbanization_level(
    project_id: int = Path(..., description="The unique identifier of the project."),
    source: str = Query(None, description="The source of the landuse zones data. Available sources are: PZZ, OSM"),
) -> GeoJSON:
    if source is not None and source not in VALID_SOURCES:
        raise http_exception(
            422,
            f"Invalid source. Valid sources are: {', '.join(VALID_SOURCES)}",
            source
        )
    return await landuse_service.get_urbanization_level(project_id, source=source)


@renovation_router.get(
    "/projects/{project_id}/context/renovation_potential",
    response_model=dict,
    description=(
        "Function for getting renovation potential for a project's context. "
        "Additionally, returns layer in GeoJSON implemented in JSON response. "
        "Args: project_id (int): unique identifier of the project. "
        "Returns: GeoJSON: context renovation potential data."
    )
)
async def get_projects_context_renovation_potential(
    project_id: int = Path(..., description="The unique identifier of the project."),
    source: str = Query(None, description="The source of the landuse zones data. Available sources are: PZZ, OSM"),
) -> GeoJSON:
    if source is not None and source not in VALID_SOURCES:
        raise http_exception(
            422,
            f"Invalid source. Valid sources are: {', '.join(VALID_SOURCES)}",
            source
        )
    return await landuse_service.get_context_renovation_potential(project_id, source=source)


@urbanization_router.get(
    "/projects/{project_id}/context/urbanization_level",
    response_model=GeoJSON,
    description=(
        "Function for getting urbanization level for a project's context. "
        "Additionally, returns layer in GeoJSON implemented in JSON response. "
        "Args: project_id (int): unique identifier of the project. "
        "Returns: GeoJSON: context urbanization level data."
    )
)
async def get_projects_context_urbanization_level(
    project_id: int = Path(..., description="The unique identifier of the project."),
    source: str = Query(None, description="The source of the landuse zones data. Available sources are: PZZ, OSM"),
) -> GeoJSON:
    if source is not None and source not in VALID_SOURCES:
        raise http_exception(
            422,
            f"Invalid source. Valid sources are: {', '.join(VALID_SOURCES)}",
            source
        )
    return await landuse_service.get_context_urbanization_level(project_id, source=source)


@landuse_percentages_router.get(
    "/scenarios/{scenario_id}/landuse_percentages",
    response_model=dict,
    description=(
        "Function for getting land use percentages for a scenario. "
        "Args: scenario_id (int): unique identifier of the scenario. "
        "Returns: dict: land use percentages data."
    )
)
async def get_project_landuse_parts(
    scenario_id: int = Path(..., description="The unique identifier of the scenario."),
    source: str = Query(None, description="The source of the landuse zones data. Available sources are: PZZ, OSM"),
) -> dict:
    if source is not None and source not in VALID_SOURCES:
        raise http_exception(
            422,
            f"Invalid source. Valid sources are: {', '.join(VALID_SOURCES)}",
            source
        )
    return await landuse_service.get_project_landuse_parts(scenario_id, source=source)

@indicators_router.post(
    "/indicators/{territory_id}/calculate_territory_urbanization",
    response_model=dict | list[dict],
    responses={
        200: {
            "description": "Successful Response",
            "content": {
                "application/json": {
                    "example": {
                        "indicator": {
                            "indicator_id": 16,
                            "parent_id": 3,
                            "name_full": "Степень урбанизации территории",
                            "measurement_unit": {
                                "id": 3,
                                "name": "%"
                            },
                            "level": 2,
                            "list_label": "1.3"
                        },
                        "territory": {
                            "id": 13,
                            "name": "Сабское сельское поселение"
                        },
                        "date_type": "year",
                        "date_value": "2025-01-01",
                        "value": 10.65,
                        "value_type": "forecast",
                        "information_source": "landuse_det",
                        "created_at": "2025-03-13T11:44:46.727723Z",
                        "updated_at": "2025-03-13T11:44:46.727723Z"
                    }
                }
            }
        },
        422: {
            "description": "Validation Error"
        }
    },
    description=(
        "Calculates and saves the urbanization percentage for a given territory in Urban DB. "
        "Returns a dictionary containing the computed indicator data that was saved in Urban DB."
    )
)
async def get_territory_urbanization_level(
    territory_id: int = Path(..., description="The unique identifier of the territory."),
    source: str = Query(None, description="The source of the landuse zones data. Valid options: PZZ, OSM."),
    force_recalculate: bool = Query(
        False,
        description="If True, forces recalculation even if the indicator already exists."
    )
) -> dict | list[dict]:
    """
    Calculate and store the urbanization percentage for a given territory in Urban DB.

    **Parameters**:
    - **territory_id** (int): The unique identifier of the territory.
    - **source** (str, optional): The source of the landuse zones data. Valid options: PZZ or OSM. Defaults to None.
    - **force_recalculate** (bool, optional): If set to True, forces recalculation even if the indicator exists. Defaults to False.

    **Returns**:
    - **dict**: A dictionary containing the computed urbanization indicator data.

    **Raises**:
    - **HTTPException (422)**: If the provided `source` is invalid.
    """
    if source is not None and source not in VALID_SOURCES:
        raise http_exception(
            422,
            f"Invalid source. Valid sources are: {', '.join(VALID_SOURCES)}",
            source
        )
    return await landuse_service.get_territory_urbanization_level(
        territory_id,
        source=source,
        force_recalculate=force_recalculate
    )
@indicators_router.post(
    "/indicators/{territory_id}/calculate_area_indicator",
    response_model=dict | list[dict])
async def calculate_area_indicator(
        territory_id: int = Path(..., description="The unique identifier of the territory."),
        force_recalculate: bool = Query(
        False,
        description="If True, forces recalculation even if the indicator already exists."
        )
        ) -> dict | list[dict]:
    territory_area = await IndicatorsService.calculate_territory_area(territory_id, force_recalculate=force_recalculate)
    return territory_area

@indicators_router.post(
    "/indicator/{territory_id}/services_count_indicator")
async def services_count_indicator(
    territory_id: int = Path(description="The unique identifier of the territory."),
    indicator_id: int = Query(description="The unique identifier of the indicator."),
    force_recalculate: bool = Query(
        False,
        description="If True, forces recalculation even if the indicator already exists."
        )
        ) -> dict | list[dict]:
    services_count = await IndicatorsService.calculate_service_count(territory_id, indicator_id, force_recalculate=force_recalculate)
    return services_count