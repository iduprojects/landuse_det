from pathlib import Path
from dotenv import load_dotenv
import os

class ApplicationConfig:
    def __init__(self):
        self.env_path = Path().absolute() / f".env.{os.getenv('APP_ENV')}"
        load_dotenv(self.env_path)

    def get(self, key: str) -> str | None:
        return os.getenv(key)

    def get_bool(self, key: str) -> bool:
        val = os.getenv(key)
        return val is not None and val.lower() in ("true", "1", "yes")

    def set(self, key: str, value: str) -> None:
        os.environ[key] = value
        text = self.env_path.read_text() if self.env_path.exists() else ""
        lines = text.splitlines()
        prefix = f"{key}="
        for i, line in enumerate(lines):
            if line.startswith(prefix):
                lines[i] = f"{key}={value}"
                break
        else:
            lines.append(f"{key}={value}")
        self.env_path.write_text("\n".join(lines) + "\n")


config = ApplicationConfig()

