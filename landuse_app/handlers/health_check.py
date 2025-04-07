"""health_check handler is defined here."""

from .routers import system_router
from fastapi.responses import FileResponse

from .. import config
from ..exceptions.http_exception_wrapper import http_exception


@system_router.get(
    "/health_check/ping",
    response_model=dict,
)
async def health_check():
    """
    Return health check response.
    """
    return {"status": "ok"}

@system_router.get("/logs")
async def get_logs():
    """
    Получить файл логов приложения
    """

    try:
        return FileResponse(
            f"{config.get('LOG_FILE')}.log",
            media_type='application/octet-stream',
            filename=f"{config.get('LOG_FILE')}.log",
        )
    except FileNotFoundError as e:
        raise http_exception(
            status_code=404,
            msg="Log file not found",
            _input={"lof_file_name": f"{config.get('LOG_FILE')}.log"},
        )
    except Exception as e:
        raise http_exception(
            status_code=500,
            msg="Internal server error during reading logs",
            _input={"lof_file_name": f"{config.get('LOG_FILE')}.log"},
        )