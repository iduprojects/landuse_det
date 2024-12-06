from landuse_api.logic.api import urban_db_api


async def get_projects_territory(project_id: int) -> dict:
    return await urban_db_api.get(f"/projects/{project_id}/territory")


async def get_projects_base_scenario_id(project_id: int) -> int:
    scenarios = await urban_db_api.get(f"/projects/{project_id}/scenarios")
    for scenario in scenarios:
        if scenario.get("is_based"):
            return scenario.get("scenario_id")


async def get_projects_base_scenario_context_geometries(project_id: int) -> dict:
    base_scenario_id = await get_projects_base_scenario_id(project_id)
    return await get_scenario_context_geometries(base_scenario_id)


async def get_scenario_context_geometries(scenario_id: int) -> dict:
    return await urban_db_api.get(f"/scenarios/{scenario_id}/context/geometries")
