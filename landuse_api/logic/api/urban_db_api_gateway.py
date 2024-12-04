import aiohttp
from loguru import logger
import yaml
from landuse_api.info import API_DESCRIPTION, API_TITLE, CONFIG_PATH
from landuse_api.config import APIConfig
from fastapi.exceptions import FastAPIError


config = APIConfig.load(CONFIG_PATH)


class UrbanDbAPI:
    def __init__(self):
        self.url = config.DIG_TP_API

    async def fetch_territories(self, scenario_id: int):
        """
        Fetches the territories from the UrbanDB API.
        """
        api_url = f"{self.url}/api/v1/scenarios/{scenario_id}/context/functional_zones?year=2024&source=OSM"
        logger.info(f"Fetching territories from API: {api_url}")

        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status == 200:
                    geojson_data = await response.json()
                    logger.info(f"TERRITORIES for scenario id {scenario_id} successfully fetched from API.")
                    return geojson_data
                else:
                    logger.error(f"Failed to fetch city model, status code: {response.status}")
                    raise FastAPIError(404, f"Failed to fetch territories from API", response.status)


urban_db_api = UrbanDbAPI()
