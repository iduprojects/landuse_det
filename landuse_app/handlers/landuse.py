"""Main landuse handlers are defined here."""
from fastapi import Path, Query

from ..exceptions.http_exception_wrapper import http_exception
from ..logic import landuse_service
from ..logic.constants.constants import VALID_SOURCES
from ..schemas import GeoJSON
from .routers import renovation_router, urbanization_router, landuse_percentages_router


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

