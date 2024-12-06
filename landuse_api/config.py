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
class APIConfig:
    app: AppConfig
    URBAN_DB_API: str = ""
    DIG_TP_API: str = ""

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
                URBAN_DB_API=data.get("URBAN_DB_API", ""),
                DIG_TP_API=data.get("DIG_TP_API", ""),
            )
        except Exception as exc:
            raise ValueError("Could not read app config file") from exc
