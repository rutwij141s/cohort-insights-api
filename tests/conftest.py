"""Shared test fixtures and configuration.

Tests require running MongoDB and Redis instances.  Start them with::

    docker compose up -d mongodb redis

Then run::

    pytest

Pipeline delays are overridden to ~0.1–0.2 s and failure rate set to 0
so the suite is fast and deterministic.
"""

import asyncio
import os

# ── Override settings BEFORE any app code is imported ────────
os.environ["MONGODB_DATABASE"] = "cohort_insights_test"
os.environ["PIPELINE_STAGE1_MIN_DELAY"] = "0.1"
os.environ["PIPELINE_STAGE1_MAX_DELAY"] = "0.2"
os.environ["PIPELINE_STAGE2_MIN_DELAY"] = "0.1"
os.environ["PIPELINE_STAGE2_MAX_DELAY"] = "0.2"
os.environ["PIPELINE_FAILURE_RATE"] = "0.0"
os.environ["PIPELINE_MAX_RETRIES"] = "1"
os.environ["PIPELINE_RETRY_BASE_DELAY"] = "0.1"
os.environ["CACHE_TTL"] = "60"

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


# ── App + client fixtures ─────────────────────────────────────
#
# Everything is function-scoped.  Between tests pytest-asyncio creates a
# fresh event loop; Motor and redis.asyncio bind their connection pools to
# the loop they were created on.  We reset the module-level singletons
# before every test so a new client is created on the new loop.

@pytest_asyncio.fixture
async def app_instance():
    """Reset clients, wipe data, then start the app lifespan."""
    # Reset singletons so new clients are created on THIS loop
    from app.database import reset_client
    from app.redis_client import reset_redis
    reset_client()
    reset_redis()

    # Wipe any leftover data before startup recovery runs
    from app.database import get_documents_collection
    from app.redis_client import get_redis
    await get_documents_collection().delete_many({})
    await get_redis().flushdb()

    from app.main import app, lifespan
    async with lifespan(app):
        yield app

    # Teardown: wipe data while clients are still open
    try:
        await get_documents_collection().delete_many({})
        await get_redis().flushdb()
    except Exception:
        pass


@pytest_asyncio.fixture
async def client(app_instance):
    """Provide an httpx AsyncClient backed by the ASGI transport."""
    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ── Poll helper ────────────────────────────────────────────────

async def wait_for_status(
    client: AsyncClient,
    document_id: str,
    user_id: str,
    target_statuses: set[str] | None = None,
    timeout: float = 10.0,
) -> dict:
    """Poll GET /documents/{id} until the status is terminal or timeout."""
    if target_statuses is None:
        target_statuses = {"completed", "failed"}

    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        resp = await client.get(
            f"/documents/{document_id}",
            headers={"X-User-Id": user_id},
        )
        assert resp.status_code == 200, f"GET failed: {resp.status_code} {resp.text}"
        data = resp.json()
        if data["status"] in target_statuses:
            return data
        if loop.time() > deadline:
            raise TimeoutError(
                f"Document {document_id} did not reach {target_statuses} "
                f"within {timeout}s (current: {data['status']})"
            )
        await asyncio.sleep(0.05)
