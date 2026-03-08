from typing import Any

from fastapi import APIRouter

from ...core.config import settings
from ...core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/node", tags=["node"])


@router.get("/health")
async def health_check() -> dict[str, Any]:
    logger.debug("Health check requested")
    return {
        "status": "healthy",
        "version": settings.__VERSION__,
    }


@router.get("/ready")
async def ready_check() -> dict[str, Any]:
    logger.debug("Ready check requested")
    return {
        "status": "Ready",
    }
