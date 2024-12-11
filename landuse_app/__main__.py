from landuse_app import config
import uvicorn


def main():
    print(f"Starting {config.get("API_TITLE")} version {config.get("VERSION")}")
    print(f"Description: {config.get("API_DESCRIPTION")}")
    print(f"Last update: {config.get("LAST_UPDATE")}")

    uvicorn.run(
        app="landuse_app:app",
        host=config.get("host"),
        port=config.get("port"),
        reload=bool(config.get("debug")),
        log_level=config.get("logger_verbosity").lower(),
    )


if __name__ == "__main__":
    main()
