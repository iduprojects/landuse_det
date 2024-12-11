"""Redirection from '/' and '/api' to swagger-ui is defined here."""

import fastapi


from .routers import system_router


@system_router.get("/", include_in_schema=False)
@system_router.get("/api/", include_in_schema=False)
async def redirect_to_swagger_docs():
    """Redirects to **/api/docs** from **/**"""
    return fastapi.responses.RedirectResponse("/api/docs")
