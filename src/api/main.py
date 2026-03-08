from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import internal_router, node_router
from ..core.config import settings
from ..core.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Application lifespan manager for connection pool cleanup."""
    logger.info("Application starting up...")
    # await DatabaseManager.get_client()
    yield
    logger.info("Application shutting down...")
    # await DatabaseManager.close_all_connections()


def create_api() -> FastAPI:
    app = FastAPI(
        title="FastAPI MongoDB Movies",
        description="A modular FastAPI application with MongoDB for movie data management",
        version=settings.__VERSION__,
        docs_url="/docs" if settings.ENABLE_DOCS else None,
        redoc_url="/redoc" if settings.ENABLE_DOCS else None,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_methods=settings.CORS_ALLOW_METHODS,
        allow_headers=settings.CORS_ALLOW_HEADERS,
        allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    )

    logger.info("Including API routers")
    app.include_router(internal_router)
    app.include_router(node_router)
    logger.info("FastAPI application created successfully")
    return app
