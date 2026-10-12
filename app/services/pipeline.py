"""Two-stage async pipeline: processing → enriching.

Stage 1 (processing, 10–20 s)  — produces a mock summary.
Stage 2 (enriching, 5–15 s)    — reads the summary, produces mock tags.

Each stage has an independent ~10 % random failure chance and retries
with exponential backoff (bonus).  A failure at stage 2 does **not** redo
stage 1 — partial progress is preserved.

Staleness / race-condition guarantee
------------------------------------
Every MongoDB write is conditional on ``pipeline_run_id`` matching the
value the run captured at start time.  When a PATCH fires, it writes a
*new* ``pipeline_run_id``; the in-flight run's writes then match zero
documents and are silently discarded — the old summary/tags can never
overwrite the new content's results, and the rate-limiter is not
decremented by the stale run.
"""

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from app.config import settings
from app.database import get_documents_collection
from app.redis_client import get_redis
from app.services.cache import set_cached_result
from app.services.rate_limiter import release
from app.utils.mock_processing import generate_summary, generate_tags

logger = logging.getLogger(__name__)


# ── Pipeline-run task manager ───────────────────────────────
# Keeps strong references to asyncio tasks so the GC doesn't
# collect them mid-flight.

class _TaskRegistry:
    def __init__(self) -> None:
        self._tasks: set[asyncio.Task] = set()

    def create(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def cancel_all(self) -> None:
        for task in self._tasks:
            task.cancel()


registry = _TaskRegistry()


# ── Helpers ─────────────────────────────────────────────────

def new_run_id() -> str:
    """Generate a unique pipeline run ID."""
    return uuid.uuid4().hex


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _compute_overall_status(stage1: str, stage2: str) -> str:
    """Derive the top-level document status from per-stage statuses."""
    if stage1 == "failed" or stage2 == "failed":
        return "failed"
    if stage1 == "queued":
        return "queued"
    if stage1 == "processing":
        return "processing"
    if stage1 == "completed":
        if stage2 in ("queued", "processing"):
            return "enriching"
        if stage2 == "completed":
            return "completed"
    return "queued"


async def _conditional_update(
    document_id: str,
    run_id: str,
    set_fields: dict,
) -> bool:
    """Update the document only if ``pipeline_run_id`` still matches.

    Returns ``True`` if the update was applied, ``False`` if the run
    was superseded by a PATCH (or the document was deleted).
    """
    from bson import ObjectId

    coll = get_documents_collection()
    result = await coll.update_one(
        {"_id": ObjectId(document_id), "pipeline_run_id": run_id},
        {"$set": {**set_fields, "updated_at": _now()}},
    )
    return result.matched_count > 0


async def _sleep_random(min_delay: float, max_delay: float) -> None:
    delay = random.uniform(min_delay, max_delay)
    await asyncio.sleep(delay)


# ── Main pipeline coroutine ─────────────────────────────────

async def run_pipeline(
    document_id: str,
    user_id: str,
    content: str,
    content_hash: str,
    run_id: str,
) -> None:
    """Execute the full two-stage pipeline for a single document.

    This coroutine runs as a fire-and-forget background task.  All
    exceptions are caught and logged — a background task must never
    raise.
    """
    try:
        await _run_stage1(document_id, user_id, content, content_hash, run_id)
        await _run_stage2(document_id, user_id, content, content_hash, run_id)
    except Exception:
        logger.exception(
            "Unhandled error in pipeline for document=%s run=%s",
            document_id, run_id,
        )


async def _run_stage1(
    document_id: str,
    user_id: str,
    content: str,
    content_hash: str,
    run_id: str,
) -> None:
    """Stage 1 — processing: produce a mock summary."""
    # Mark stage 1 as processing
    applied = await _conditional_update(
        document_id, run_id,
        {
            "status": "processing",
            "stage1.status": "processing",
            "stage1.attempt": 1,
            "stage1.started_at": _now(),
            "stage1.error": None,
        },
    )
    if not applied:
        logger.info("Stage 1 skipped (run superseded): doc=%s run=%s", document_id, run_id)
        return

    logger.info("Stage 1 started: doc=%s run=%s", document_id, run_id)

    # Attempt processing with retries
    error_msg = None
    for attempt in range(1, settings.pipeline_max_retries + 2):
        await _sleep_random(
            settings.pipeline_stage1_min_delay,
            settings.pipeline_stage1_max_delay,
        )

        if random.random() < settings.pipeline_failure_rate:
            error_msg = f"Stage 1 (processing) failed on attempt {attempt}"
            logger.warning("Stage 1 attempt %d failed: doc=%s", attempt, document_id)
            if attempt <= settings.pipeline_max_retries:
                backoff = settings.pipeline_retry_base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
                # Update attempt counter
                await _conditional_update(
                    document_id, run_id,
                    {"stage1.attempt": attempt + 1},
                )
                continue
            # All retries exhausted
            break
        # Success
        error_msg = None
        break

    if error_msg is not None:
        applied = await _conditional_update(
            document_id, run_id,
            {
                "status": "failed",
                "stage1.status": "failed",
                "stage1.error": error_msg,
                "stage1.completed_at": _now(),
            },
        )
        if applied:
            logger.error("Stage 1 permanently failed: doc=%s", document_id)
            await release(user_id, document_id)
        return

    # Generate summary
    summary = generate_summary(content)

    applied = await _conditional_update(
        document_id, run_id,
        {
            "stage1.status": "completed",
            "stage1.error": None,
            "stage1.completed_at": _now(),
            "summary.text": summary,
            "summary.content_hash": content_hash,
            # Overall status moves to "enriching" once stage 2 starts,
            # but we set it here to bridge the gap.
            "status": "enriching",
        },
    )
    if not applied:
        logger.info("Stage 1 result discarded (run superseded): doc=%s", document_id)
        return

    logger.info("Stage 1 completed: doc=%s", document_id)


async def _run_stage2(
    document_id: str,
    user_id: str,
    content: str,
    content_hash: str,
    run_id: str,
) -> None:
    """Stage 2 — enriching: produce mock keyword tags from the summary."""
    # Mark stage 2 as processing
    applied = await _conditional_update(
        document_id, run_id,
        {
            "status": "enriching",
            "stage2.status": "processing",
            "stage2.attempt": 1,
            "stage2.started_at": _now(),
            "stage2.error": None,
        },
    )
    if not applied:
        logger.info("Stage 2 skipped (run superseded): doc=%s run=%s", document_id, run_id)
        return

    logger.info("Stage 2 started: doc=%s run=%s", document_id, run_id)

    # Attempt enriching with retries
    error_msg = None
    for attempt in range(1, settings.pipeline_max_retries + 2):
        await _sleep_random(
            settings.pipeline_stage2_min_delay,
            settings.pipeline_stage2_max_delay,
        )

        if random.random() < settings.pipeline_failure_rate:
            error_msg = f"Stage 2 (enriching) failed on attempt {attempt}"
            logger.warning("Stage 2 attempt %d failed: doc=%s", attempt, document_id)
            if attempt <= settings.pipeline_max_retries:
                backoff = settings.pipeline_retry_base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
                await _conditional_update(
                    document_id, run_id,
                    {"stage2.attempt": attempt + 1},
                )
                continue
            break
        error_msg = None
        break

    if error_msg is not None:
        applied = await _conditional_update(
            document_id, run_id,
            {
                "status": "failed",
                "stage2.status": "failed",
                "stage2.error": error_msg,
                "stage2.completed_at": _now(),
            },
        )
        if applied:
            logger.error("Stage 2 permanently failed: doc=%s", document_id)
            await release(user_id, document_id)
        return

    # Generate tags
    tags = generate_tags(content)

    applied = await _conditional_update(
        document_id, run_id,
        {
            "status": "completed",
            "stage2.status": "completed",
            "stage2.error": None,
            "stage2.completed_at": _now(),
            "tags.keywords": tags,
            "tags.content_hash": content_hash,
        },
    )
    if not applied:
        logger.info("Stage 2 result discarded (run superseded): doc=%s", document_id)
        return

    # Cache results for future identical submissions
    await set_cached_result(user_id, content_hash, summary_text_from_doc(content), tags)

    # Release the rate-limit slot
    await release(user_id, document_id)

    logger.info("Stage 2 completed — pipeline done: doc=%s", document_id)


def summary_text_from_doc(content: str) -> str:
    """Helper to regenerate the summary for caching (same as stage 1)."""
    return generate_summary(content)


# ── Public API ──────────────────────────────────────────────

def start_pipeline(
    document_id: str,
    user_id: str,
    content: str,
    content_hash: str,
    run_id: str,
) -> asyncio.Task:
    """Launch the pipeline as a background task."""
    return registry.create(
        run_pipeline(document_id, user_id, content, content_hash, run_id)
    )
