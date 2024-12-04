"""health_check handler is defined here."""

from starlette import status

from .routers import system_router


@system_router.get(
    "/health_check/ping",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def health_check():
    """
    Return health check response.
    """
    return {"status": "ok"}
