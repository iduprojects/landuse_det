import aiohttp
from loguru import logger

from landuse_app import config
from landuse_app.exceptions.http_exception_wrapper import http_exception
from storage.caching import CachingService, caching_service


class UrbanDbAPI:
    def __init__(self, url: str, bearer_token: dict[str, str] = None, cache_service: CachingService = None):
        self.url = config.get("DIG_TP_API")
        self.bearer_token = {"Authorization": bearer_token}
        self.cache_service = cache_service

    async def get(self, extra_url: str, params: dict = None) -> dict:
        endpoint_name = extra_url.strip("/").replace("/", "_")
        recent_cache_file = self.cache_service.get_recent_cache_file(endpoint_name, params or {})
        if recent_cache_file and self.cache_service.is_cache_valid(recent_cache_file):
            logger.info(f"Using cached data for {extra_url}")
            return self.cache_service.load_cache(recent_cache_file)

        endpoint_url = self.url + extra_url
        async with aiohttp.ClientSession() as session:
            async with session.get(url=endpoint_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    self.cache_service.save_with_cleanup(data, endpoint_name, params or {})
                    return data
                error_details = await response.text()
                logger.error(f"Error from API: {error_details}")
                raise http_exception(response.status, f"Failed to fetch data from Urban API:", error_details)


urban_db_api = UrbanDbAPI(config.get("DIG_TP_API"), cache_service=caching_service)
