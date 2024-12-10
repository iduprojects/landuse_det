"""health_check handler is defined here."""

from .routers import system_router


@system_router.get(
    "/health_check/ping",
    response_model=dict,
)
async def health_check():
    """
    Return health check response.
    """
    return {"status": "ok"}
