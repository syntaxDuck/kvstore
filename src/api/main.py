from contextlib import asynccontextmanager
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from .routes import internal_router, client_router, node_router
from ..core.config import settings
from ..core.logging import get_logger
from ..core.metrics import get_metrics

logger = get_logger(__name__)


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        metrics = get_metrics()
        start_time = time.perf_counter()

        response = await call_next(request)

        duration_ms = (time.perf_counter() - start_time) * 1000
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        endpoint = f"{request.method} {route_path}"
        metrics.record_timing_sync(f"api_request_duration_ms.{endpoint}", duration_ms)
        metrics.increment_counter_sync(
            f"api_request_count.{endpoint}.{response.status_code}"
        )

        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for connection pool cleanup."""
    logger.info("Application starting up...")
    yield
    node = getattr(app.state, "node", None)
    if node is not None:
        if hasattr(node, "shutdown"):
            await node.shutdown()
        elif hasattr(node, "close_peer_sessions"):
            await node.close_peer_sessions()
    logger.info("Application shutting down...")


def create_api() -> FastAPI:
    app = FastAPI(
        title="KVStore Node",
        description="A distributed key-value store with Raft consensus",
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

    app.add_middleware(MetricsMiddleware)

    app.include_router(internal_router, prefix="/internal/v1")
    app.include_router(client_router, prefix="/client/v1")
    app.include_router(node_router)

    logger.info("FastAPI application created successfully")
    return app
