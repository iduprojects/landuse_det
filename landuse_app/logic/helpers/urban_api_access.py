from landuse_app.exceptions.http_exception_wrapper import http_exception
from landuse_app.logic.api import urban_db_api


async def get_projects_territory(project_id: int) -> dict:
    """
    Fetches the territory information for a project.

    Parameters:
    project_id (int): ID of the project.

    Returns:
    dict: Territory information.
    """
    endpoint = f"/projects/{project_id}/territory"
    response = await urban_db_api.get(endpoint)

    if not response:
        raise http_exception(404, f"No territory information found for project ID:", project_id)

    return response


async def get_projects_base_scenario_id(project_id: int) -> int:
    """
    Fetches the base scenario ID for a project.

    Parameters:
    project_id (int): ID of the project.

    Returns:
    int: Base scenario ID.
    """
    endpoint = f"/projects/{project_id}/scenarios"
    scenarios = await urban_db_api.get(endpoint)

    for scenario in scenarios:
        if scenario.get("is_based"):
            return scenario.get("scenario_id")


async def get_functional_zones_scenario_id(project_id: int, is_context: bool = False) -> dict:
    """
    Fetches functional zones for a project with an optional context flag.

    Parameters:
    project_id (int): ID of the project.
    is_context (bool): Flag to determine if context data should be fetched. Default is False.

    Returns:
    dict: Response data from the API.

    Raises:
    HTTPException: If the response is empty.
    """
    base_scenario_id = await get_projects_base_scenario_id(project_id)

    endpoint = (
        f"/scenarios/{base_scenario_id}/context/functional_zones?year=2024&source=OSM"
        if is_context
        else f"/scenarios/{base_scenario_id}/functional_zones?year=2024&source=OSM"
    )

    response = await urban_db_api.get(endpoint)
    if not response or "features" not in response or not response["features"]:
        raise http_exception(404, "No functional zones found for the given project ID:", project_id)

    return response


async def get_all_physical_objects_geometries(project_id: int, is_context: bool = False) -> dict:
    """
    Fetches all physical object geometries for a project, optionally for context.

    Parameters:
        project_id (int): ID of the project.
        is_context (bool): Whether to fetch context geometries.

    Returns:
        dict: The API response containing geometries.

    Raises:
        HTTPException: If the response is empty.
    """
    base_scenario_id = await get_projects_base_scenario_id(project_id)

    endpoint = (
        f"/scenarios/{base_scenario_id}/context/geometries_with_all_objects"
        if is_context
        else f"/scenarios/{base_scenario_id}/geometries_with_all_objects"
    )

    response = await urban_db_api.get(endpoint)
    try:
        if not response or not response.get("features"):
            raise http_exception(404, "No geometries found for the given project ID:", project_id)
    except Exception:
        raise http_exception(500, "Failed to fetch data from urban API", response.status)

    return response

async def get_all_physical_objects_geometries_type_id(project_id: int, object_type_id: int) -> dict:
    base_scenario_id = await get_projects_base_scenario_id(project_id)
    return await urban_db_api.get(
        f"/scenarios/{base_scenario_id}/geometries_with_all_objects?physical_object_type_id={object_type_id}"
    )
