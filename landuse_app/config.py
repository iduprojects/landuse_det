from pathlib import Path
from dotenv import load_dotenv
import os
from loguru import logger


class ApplicationConfig:
    def __init__(self):
        load_dotenv(Path().absolute() / f".env.{os.getenv('APP_ENV')}")
        logger.info("Env variables loaded")

    @staticmethod
    def get(key: str) -> str | None:
        return os.getenv(key)

    @staticmethod
    def get_bool(key: str) -> bool:
        val = os.getenv(key)
        return val is not None and val.lower() in ("true", "1", "yes")

config = ApplicationConfig()
