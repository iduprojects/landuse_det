"""Application configuration classes."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TextIO

import yaml


@dataclass
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    debug: int = 0
    logger_verbosity: Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    name: str = "landuse_api"


@dataclass
class DBConfig:
    addr: str = "localhost"
    port: int = 5432
    name: str = "urban_db"
    user: str = "postgres"
    password: str = "postgres"
    pool_size: int = 15


@dataclass
class AuthConfig:
    url: str = ""
    validate: int = 0
    cache_size: int = 100
    cache_ttl: int = 1800


@dataclass
class APIConfig:
    app: AppConfig
    db: DBConfig
    auth: AuthConfig

    @classmethod
    def load(cls, file: str | Path | TextIO) -> "APIConfig":
        """Import config from the given filename or raise `ValueError` on error."""

        try:
            if isinstance(file, (str, Path)):
                with open(file, "r", encoding="utf-8") as file_r:
                    data = yaml.safe_load(file_r)
            else:
                data = yaml.safe_load(file)

            return cls(
                app=AppConfig(**data.get("app", {})),
                db=DBConfig(**data.get("db", {})),
                auth=AuthConfig(**data.get("auth", {})),
            )
        except Exception as exc:
            raise ValueError("Could not read app config file") from exc
