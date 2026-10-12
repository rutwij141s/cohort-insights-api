"""Cohort Insights API — FastAPI application entry point.

Wires together routers, configures structured logging, creates MongoDB
indexes, and recovers in-flight pipeline jobs on (re)start.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.database import close_client, create_indexes, get_documents_collection
from app.redis_client import close_redis, get_redis
from app.services.pipeline import new_run_id, start_pipeline
from app.utils.hashing import compute_content_hash

logger = logging.getLogger(__name__)


# ── Logging ─────────────────────────────────────────────────

def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ── Startup recovery ────────────────────────────────────────

async def _recover_in_flight_documents() -> None:
    """Re-enqueue documents left in a non-terminal state by a crash/restart.

    On startup, any document whose status is ``queued``, ``processing``,
    or ``enriching`` had its in-memory asyncio task killed.  We:
        1. Reset the document to ``queued`` with fresh stages.
        2. Assign a new ``pipeline_run_id``.
        3. Add it to the Redis rate-limiter set (bypassing the limit check
           — these documents were already active before the restart).
        4. Start a new pipeline task.
    """
    coll = get_documents_collection()
    redis = get_redis()

    # Clear stale rate-limiter keys first (they may contain phantom entries
    # from the previous process that were never released).
    cursor = redis.scan_iter(match="rate_limit:*", count=100)
    async for key in cursor:
        await redis.delete(key)

    # Find all documents in non-terminal states
    active_filter = {"status": {"$in": ["queued", "processing", "enriching"]}}
    recovered = 0
    async for doc in coll.find(active_filter):
        run_id = new_run_id()

        # Reset to queued
        await coll.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "status": "queued",
                    "pipeline_run_id": run_id,
                    "stage1": {
                        "status": "queued",
                        "error": None,
                        "attempt": 0,
                        "started_at": None,
                        "completed_at": None,
                    },
                    "stage2": {
                        "status": "queued",
                        "error": None,
                        "attempt": 0,
                        "started_at": None,
                        "completed_at": None,
                    },
                    "summary": {"text": None, "content_hash": None},
                    "tags": {"keywords": None, "content_hash": None},
                }
            },
        )

        # Re-add to rate limiter (bypass check — already active)
        await redis.sadd(f"rate_limit:{doc['user_id']}", str(doc["_id"]))

        # Restart pipeline
        content_hash = compute_content_hash(doc["content"])
        start_pipeline(
            str(doc["_id"]),
            doc["user_id"],
            doc["content"],
            content_hash,
            run_id,
        )
        recovered += 1

    if recovered:
        logger.info("Recovered %d in-flight document(s) on startup", recovered)


# ── Lifespan ────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    logger.info("Starting Cohort Insights API…")

    # Startup
    await create_indexes()
    await _recover_in_flight_documents()
    logger.info("Startup complete")

    yield

    # Shutdown
    logger.info("Shutting down…")
    from app.services.pipeline import registry
    registry.cancel_all()
    await close_client()
    await close_redis()
    logger.info("Shutdown complete")


# ── App ─────────────────────────────────────────────────────

app = FastAPI(
    title="Cohort Insights API",
    description=(
        "A backend service that ingests content, processes it through a "
        "two-stage simulated pipeline, and serves the results."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Register routers
from app.routers import documents, health, users  # noqa: E402

app.include_router(health.router)
app.include_router(documents.router)
app.include_router(users.router)
