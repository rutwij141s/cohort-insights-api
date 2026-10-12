"""Health-check endpoint: verifies MongoDB and Redis connectivity."""

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.database import get_client
from app.models import HealthResponse
from app.redis_client import get_redis

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> JSONResponse:
    """Return the connectivity status of MongoDB and Redis.

    HTTP 200 when both backends are reachable, HTTP 503 otherwise.
    """
    mongo_ok = True
    redis_ok = True

    # ── MongoDB ping ──────────────────────────────────────────
    try:
        await get_client().admin.command("ping")
    except Exception as exc:
        logger.error("Health check: MongoDB unreachable — %s", exc)
        mongo_ok = False

    # ── Redis ping ────────────────────────────────────────────
    try:
        await get_redis().ping()
    except Exception as exc:
        logger.error("Health check: Redis unreachable — %s", exc)
        redis_ok = False

    body = {
        "status": "healthy" if (mongo_ok and redis_ok) else "unhealthy",
        "mongodb": "healthy" if mongo_ok else "unhealthy",
        "redis": "healthy" if redis_ok else "unhealthy",
    }
    status_code = 200 if (mongo_ok and redis_ok) else 503
    return JSONResponse(content=body, status_code=status_code)
