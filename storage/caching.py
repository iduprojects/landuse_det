import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger

from landuse_app import config


class CachingService:
    def __init__(self, cache_path: Path, cache_enabled: bool = True):
        self.cache_enabled = cache_enabled
        self.refresh_days: int = 3
        if self.cache_enabled:
            self.cache_path = cache_path
            self.cache_path.mkdir(parents=True, exist_ok=True)
        else:
            self.cache_path = None

    def _sanitize_filename(self, name: str) -> str:
        return re.sub(r'[<>:"/\\|?*&]', "", name)

    def get_cache_file_path(self, name: str, params: dict) -> Path:
        if not self.cache_enabled:
            return None
        sanitized_name = self._sanitize_filename(name)
        param_string = "_".join([f"{k}-{v}" for k, v in sorted(params.items())])
        date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        return self.cache_path / f"{date}_{sanitized_name}_{param_string}.json"

    def is_cache_valid(self, file_path: Path) -> bool:
        if not self.cache_enabled or not file_path or not file_path.exists():
            return False
        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        return datetime.now() - file_time < timedelta(days=self.refresh_days)

    def save_cache(self, data: dict, file_path: Path) -> None:
        if not self.cache_enabled or not file_path:
            return
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.warning(f"Ошибка при сохранении кэша в {file_path}: {e}")

    def load_cache(self, file_path: Path) -> dict:
        if not self.cache_enabled or not file_path or not file_path.exists():
            return {}
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Ошибка при загрузке кэша из {file_path}: {e}")
            return {}

    def get_recent_cache_file(self, name: str, params: dict) -> Path:
        if not self.cache_enabled:
            return None
        sanitized_name = self._sanitize_filename(name)
        param_string = "_".join([f"{k}-{v}" for k, v in sorted(params.items())])
        pattern = f"*_{sanitized_name}_{param_string}.json"
        matching_files = sorted(self.cache_path.glob(pattern), reverse=True)
        return matching_files[0] if matching_files else None

    def clean_cache(self, name: str, params: dict) -> None:
        if not self.cache_enabled:
            return
        sanitized_name = self._sanitize_filename(name)
        param_string = "_".join([f"{k}-{v}" for k, v in sorted(params.items())])
        pattern = f"*_{sanitized_name}_{param_string}.json"
        matching_files = self.cache_path.glob(pattern)
        for file in matching_files:
            if not self.is_cache_valid(file):
                logger.info(f"Удаление устаревшего кэш-файла: {file}")
                try:
                    file.unlink()
                except Exception as e:
                    logger.warning(f"Ошибка при удалении файла {file}: {e}")

    def save_with_cleanup(self, data: dict, name: str, params: dict) -> None:
        if not self.cache_enabled:
            return
        self.clean_cache(name, params)
        file_path = self.get_cache_file_path(name, params)
        self.save_cache(data, file_path)

cache_enabled = config.get_bool("CACHE_ENABLED")  # должен вернуть True или False
caching_service = CachingService(Path().absolute() / "__landuse_cache__", cache_enabled)