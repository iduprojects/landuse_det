import pandas as pd

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


async def get_functional_zone_sources(scenario_id: int, source: str = None) -> dict:
    """
    Fetch available functional zone sources for a given scenario ID and determine the best source.

    Parameters:
    scenario_id (int): The ID of the scenario.
    source (str, optional): The preferred source (PZZ or OSM). If not provided, the best source is selected automatically.

    Returns:
    dict: The most relevant functional zone source data.

    Raises:
    http_exception: If no sources are found or the specified source is not available.
    """
    endpoint = f"/scenarios/{scenario_id}/functional_zone_sources"
    response = await urban_db_api.get(endpoint)

    if not response:
        raise http_exception(404, f"No functional zone sources found for the given scenario ID", scenario_id)

    if source:
        # Если источник указан, ищем его в доступных источниках
        source_data = next((s for s in response if s["source"] == source), None)
        if not source_data:
            raise http_exception(404, f"No data found for the specified source", source)
        return source_data

    # Если источник не указан, используем текущую логику
    return await _form_source_params(response)


async def _form_source_params(sources: list[dict]) -> dict:
    """
    Determine the most relevant functional zone source from the available sources.

    Parameters:
    sources (list[dict]): List of available sources.

    Returns:
    dict: The most relevant source dictionary containing 'source' and 'year'.
    """
    if len(sources) == 1:
        return sources[0]

    source_names = [i["source"] for i in sources]
    source_data_df = pd.DataFrame(sources)

    if "OSM" in source_names:
        return source_data_df.loc[
            source_data_df[source_data_df["source"] == "OSM"]["year"].idxmax()
        ].to_dict()
    elif "PZZ" in source_names:
        return source_data_df.loc[
            source_data_df[source_data_df["source"] == "PZZ"]["year"].idxmax()
        ].to_dict()
    else:
        return source_data_df.loc[
            source_data_df[source_data_df["source"] == "User"]["year"].idxmax()
        ].to_dict()


async def get_functional_zones_scenario_id(project_id: int, is_context: bool = False, source: str = None) -> dict:
    """
    Fetches functional zones for a project with an optional context flag and source selection.

    Parameters:
    project_id (int): ID of the project.
    is_context (bool): Flag to determine if context data should be fetched. Default is False.
    source (str, optional): The preferred source (PZZ or OSM). If not provided, the best source is selected automatically.

    Returns:
    dict: Response data from the API.

    Raises:
    http_exception: If the response is empty or the specified source is not available.
    """
    base_scenario_id = await get_projects_base_scenario_id(project_id)
    source_data = await get_functional_zone_sources(base_scenario_id, source)

    if not source_data or "source" not in source_data or "year" not in source_data:
        raise http_exception(404, "No valid source found for the given project ID", project_id)

    source = source_data["source"]
    year = source_data["year"]

    endpoint = (
        f"/projects/{project_id}/context/functional_zones?year={year}&source={source}"
        if is_context
        else f"/scenarios/{base_scenario_id}/functional_zones?year={year}&source={source}"
    )

    response = await urban_db_api.get(endpoint)
    if not response or "features" not in response or not response["features"]:
        raise http_exception(404, "No functional zones found for the given project ID", project_id)

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
        http_exception: If the response is empty.
    """
    base_scenario_id = await get_projects_base_scenario_id(project_id)

    endpoint = (
        f"/projects/{project_id}/context/geometries_with_all_objects"
        if is_context
        else f"/scenarios/{base_scenario_id}/geometries_with_all_objects"
    )

    try:
        response = await urban_db_api.get(endpoint)
    except Exception:
        raise http_exception(404, "No geometries found for the given project ID:", project_id)

    return response


async def get_all_physical_objects_geometries_type_id(project_id: int, object_type_id: int) -> dict:
    base_scenario_id = await get_projects_base_scenario_id(project_id)
    return await urban_db_api.get(
        f"/scenarios/{base_scenario_id}/geometries_with_all_objects?physical_object_type_id={object_type_id}"
    )


async def get_functional_zones_scen_id_percentages(scenario_id: int, source: str = None) -> dict:
    """
    Fetches functional zone percentages for a given scenario ID with an optional source selection.

    Parameters:
    scenario_id (int): The ID of the scenario.
    source (str, optional): The preferred source (PZZ or OSM). If not provided, the best source is selected automatically.

    Returns:
    dict: Response data from the API.

    Raises:
    http_exception: If the response is empty or the specified source is not available.
    """
    source_data = await get_functional_zone_sources(scenario_id, source)

    if not source_data or "source" not in source_data or "year" not in source_data:
        raise http_exception(404, "No valid source found for the given scenario ID", scenario_id)

    source = source_data["source"]
    year = source_data["year"]

    endpoint = f"/scenarios/{scenario_id}/functional_zones?year={year}&source={source}"
    response = await urban_db_api.get(endpoint)

    if not response or "features" not in response or not response["features"]:
        raise http_exception(404, "No functional zones found for the given scenario ID", scenario_id)

    return response


async def get_all_physical_objects_geometries_scen_id_percentages(scenario_id: int) -> dict:
    """
    Fetches all physical object geometries for a project, optionally for context.

    Parameters:
        scenario_id (int): ID of the scenario.
    Returns:
        dict: The API response containing geometries.

    Raises:
        http_exception: If the response is empty.
    """
    endpoint = f"/scenarios/{scenario_id}/geometries_with_all_objects"
    response = await urban_db_api.get(endpoint)

    if not response or "features" not in response or not response["features"]:
        raise http_exception(404, "No functional zones found for the given scenario ID:", scenario_id)

    return response
