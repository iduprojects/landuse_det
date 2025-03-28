import aiohttp
from loguru import logger

from landuse_app import config
from landuse_app.exceptions.http_exception_wrapper import http_exception
from storage.caching import CachingService, caching_service


class UrbanDbAPI:
    def __init__(self, url: str, bearer_token: dict[str, str] = None, cache_service: CachingService = None):
        self.url = config.get("URBAN_API")
        self.bearer_token = {"Authorization": bearer_token} if bearer_token else {}
        self.cache_service = cache_service

    async def get(self, extra_url: str, params: dict = None, ignore_404: bool = False) -> dict:
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
                elif ignore_404 and response.status == 404:
                    return None
                error_details = await response.text()
                logger.error(f"Error from API: {error_details}")
                raise http_exception(response.status, f"Failed to fetch data from Urban API:", error_details)

    async def put(self, extra_url: str, data: dict = None) -> dict:
        """
        Sends PUT-request on requested endpoint (without caching).
        Returns JSON-response, or raises http_exception on exceptions.
        """
        endpoint_url = self.url + extra_url
        headers = {k: v for k, v in self.bearer_token.items() if v is not None}

        async with aiohttp.ClientSession() as session:
            async with session.put(url=endpoint_url, json=data, headers=headers) as response:
                if response.status in (200, 201):
                    return await response.json()
                error_details = await response.text()
                logger.error(f"Error from API (PUT): {error_details}")
                raise http_exception(response.status, f"Failed to PUT data to Urban API:", error_details)

urban_db_api = UrbanDbAPI(config.get("URBAN_API"), cache_service=caching_service)



