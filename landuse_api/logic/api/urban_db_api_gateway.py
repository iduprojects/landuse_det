import aiohttp
from fastapi.exceptions import FastAPIError

from landuse_api import config
from landuse_api.exceptions.http_exception_wrapper import http_exception


class UrbanDbAPI:
    def __init__(self, url: str, bearer_token: dict[str, str] = None):
        self.url = url
        self.bearer_token = {"Authorization": bearer_token}

    async def get(self, extra_url: str, params: dict = None) -> dict:
        endpoint_url = self.url + extra_url
        async with aiohttp.ClientSession() as session:
            async with session.get(url=endpoint_url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                raise http_exception(404, f"Failed to fetch data from Urban API", response.status)


urban_db_api = UrbanDbAPI(config.DIG_TP_API)
