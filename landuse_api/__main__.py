import uvicorn

from landuse_api.config import APIConfig
from landuse_api.info import CONFIG_PATH

config = APIConfig.load(CONFIG_PATH)


def main():
    uvicorn.run(
        "landuse_api:app",
        host=config.app.host,
        port=config.app.port,
        reload=bool(config.app.debug),
        log_level=config.app.logger_verbosity.lower(),
    )


if __name__ == "__main__":
    main()
