import time
import aiohttp
import logging
import jwt
from fastapi import HTTPException

from landuse_app import config
from storage.caching import caching_service

logger = logging.getLogger(__name__)


class AuthService:
    def __init__(self, auth_base_url: str):
        self.introspect_url = f"{auth_base_url}/introspect/"
        self.refresh_url = f"{auth_base_url}/refresh_token/"
        self.token_url = f"{auth_base_url}/token/"

    async def _introspect(self, token: str) -> bool:
        async with aiohttp.ClientSession() as sess:
            payload = {
                "token": token,
                "token_type_hint": "access_token",
                "client_id": "unknown_client"
            }
            headers = {
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            async with sess.post(self.introspect_url, data=payload, headers=headers) as resp:
                if resp.status == 200:
                    return (await resp.json()).get("active", False)
                logger.warning("Introspect failed %s: %s", resp.status, await resp.text())
                return False

    async def _refresh(self, refresh_token: str) -> dict:
        async with aiohttp.ClientSession() as sess:
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": "unknown_client"
            }
            headers = {
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            async with sess.post(self.refresh_url, data=payload, headers=headers) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.error("Refresh token failed %s: %s", resp.status, text)
                    raise HTTPException(401, f"Cannot refresh token: {text}")
                return await resp.json()

    async def _password_grant(self) -> dict:
        """
        Request to /token/ using password-flow, using login/password from config
        """
        username = config.get("AUTH_USERNAME")
        password = config.get("AUTH_PASSWORD")
        if not username or not password:
            raise HTTPException(500, "Missing AUTH_USERNAME/AUTH_PASSWORD in config")

        async with aiohttp.ClientSession() as sess:
            payload = {
                "grant_type": "password",
                "username": username,
                "password": password,
                "client_id": "unknown_client"
            }
            headers = {
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            async with sess.post(self.token_url, data=payload, headers=headers) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.error("Password grant failed %s: %s", resp.status, text)
                    raise HTTPException(401, f"Password grant error: {text}")
                return await resp.json()

    def _is_jwt_expired(self, token: str) -> bool:
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
            return payload.get("exp", 0) < time.time()
        except Exception:
            return True

    async def validate_and_refresh(self) -> str:
        """
        1) If refresh_token expired — trying password grant.
        2) Otherwise, if access_token is not expired yet — introspect.
        3) If introspect return False or access token is expired — refresh.
        4) Setting new tokens and returning access_token.
        """
        access = config.get("ACCESS_TOKEN") or ""
        refresh = config.get("REFRESH_TOKEN") or ""

        if self._is_jwt_expired(refresh):
            logger.info("Local: refresh token expired, falling back to password grant")
            tokens = await self._password_grant()

        else:
            if not self._is_jwt_expired(access):
                logger.info("Local: access token still valid, will introspect")
                if await self._introspect(access):
                    return access
                logger.info("Introspect: access token inactive, will refresh")

            try:
                tokens = await self._refresh(refresh)
            except HTTPException as e:
                if "Signature has expired" in e.detail:
                    logger.info("Server: refresh token expired, falling back to password grant")
                    tokens = await self._password_grant()
                else:
                    raise

        config.set("ACCESS_TOKEN", tokens["access_token"])
        config.set("REFRESH_TOKEN", tokens["refresh_token"])
        logger.info("Tokens updated (expires_in=%s)", tokens.get("expires_in"))
        return tokens["access_token"]


class UrbanDbAPI:
    def __init__(self, api_base: str, auth_service: AuthService, cache_service=None):
        self.url = api_base
        self.auth = auth_service
        self.cache = cache_service

    async def _prepare_headers(self, use_token: bool = True, override_token: str | None = None) -> dict:
        headers: dict[str,str] = {}
        if override_token:
            headers["Authorization"] = f"Bearer {override_token}"
        elif use_token:
            token = await self.auth.validate_and_refresh()
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def get(self, path: str, params: dict = None, ignore_404: bool = False) -> dict | None:
        headers = await self._prepare_headers()
        key = path.strip("/").replace("/", "_")
        if self.cache:
            recent = self.cache.get_recent_cache_file(key, params or {})
            if recent and self.cache.is_cache_valid(recent):
                logger.info("Using cache for %s", path)
                return self.cache.load_cache(recent)

        url = f"{self.url}{path}"
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if self.cache:
                        self.cache.save_with_cleanup(data, key, params or {})
                    return data
                if ignore_404 and resp.status == 404:
                    return None
                text = await resp.text()
                logger.error("GET %s failed: %s", path, text)
                raise HTTPException(resp.status, f"Urban API GET error: {text}")

    async def put(
        self,
        path: str,
        data: dict | None = None,
        *,
        extra_headers: dict[str, str] | None = None,
        use_token: bool = True,
        override_token: str | None = None,
    ) -> dict:
        """
        Async PUT-request.
        - extra_headers – any additional headers
        - use_token, override_token – default logic from AuthService
        """
        headers = dict(extra_headers) if extra_headers else {}

        if override_token:
            headers["Authorization"] = f"Bearer {override_token}"
        elif use_token:
            token = await self.auth.validate_and_refresh()
            headers["Authorization"] = f"Bearer {token}"

        url = f"{self.url}{path}"
        async with aiohttp.ClientSession() as sess:
            async with sess.put(url, json=data, headers=headers) as resp:
                if resp.status in (200, 201):
                    return await resp.json()
                text = await resp.text()
                logger.error("PUT %s failed: %s", path, text)
                raise HTTPException(resp.status, f"Urban API PUT error: {text}")


auth_svc = AuthService(auth_base_url=config.get("AUTH_SERVICE_URL"))
urban_db_api = UrbanDbAPI(
    api_base=config.get("URBAN_API"),
    auth_service=auth_svc,
    cache_service=caching_service
)
