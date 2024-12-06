from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html

from landuse_api.handlers import list_of_routes
from landuse_api.info import API_DESCRIPTION, API_TITLE, LAST_UPDATE, VERSION
from landuse_api.middlewares import ExceptionHandlerMiddleware


def bind_routes(application: FastAPI, prefix: str) -> None:
    """Bind all routes to application."""
    for route in list_of_routes:
        application.include_router(route, prefix=(prefix if "/" not in {r.path for r in route.routes} else ""))


def get_app(prefix: str = "/api") -> FastAPI:
    """Create application and all dependable objects."""

    application = FastAPI(
        title=API_TITLE,
        description=API_DESCRIPTION,
        docs_url=None,
        openapi_url=f"{prefix}/openapi",
        version=f"{VERSION} ({LAST_UPDATE})",
        terms_of_service="http://swagger.io/terms/",
        contact={"email": "idu@itmo.ru"},
        license_info={"name": "Apache 2.0", "url": "http://www.apache.org/licenses/LICENSE-2.0.html"},
    )
    bind_routes(application, prefix)

    @application.get(f"{prefix}/docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url="https://unpkg.com/swagger-ui-dist@5.11.7/swagger-ui-bundle.js",
            swagger_css_url="https://unpkg.com/swagger-ui-dist@5.11.7/swagger-ui.css",
        )

    origins = ["*"]

    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.add_middleware(
        ExceptionHandlerMiddleware,
        debug=[False],  # reinitialized on startup
    )

    return application


app = get_app()
