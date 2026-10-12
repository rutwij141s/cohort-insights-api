"""Document business logic: submit, update, get, list, and by-ref lookup.

This module is the single place where MongoDB, Redis cache, the rate
limiter, and the pipeline all come together.  Routers are thin
delegates — all validation, orchestration, and error mapping lives here.
"""

import logging
from datetime import datetime, timezone

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException

from app.config import settings
from app.database import get_documents_collection
from app.services import rate_limiter
from app.services.cache import get_cached_result, set_cached_result
from app.services.pipeline import new_run_id, start_pipeline
from app.utils.hashing import compute_content_hash
from app.utils.mock_processing import generate_summary, generate_tags

logger = logging.getLogger(__name__)

# ── Helpers ─────────────────────────────────────────────────


def _to_object_id(doc_id: str) -> ObjectId:
    """Convert a string to ObjectId, raising 404 on invalid format."""
    try:
        return ObjectId(doc_id)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=404, detail="Document not found")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fresh_stages() -> dict:
    """Return the initial stage sub-documents for a new/restarted pipeline."""
    return {
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
    }


_ACTIVE_STATES = {"queued", "processing", "enriching"}


# ── Submit ──────────────────────────────────────────────────

async def submit_document(
    user_id: str,
    title: str,
    content: str,
    client_doc_ref: str | None,
) -> dict:
    """Handle POST /documents.

    Flow:
        1. If *client_doc_ref* already exists for the same user, treat the
           submission as an update to that document (new version).
        2. If *client_doc_ref* exists for a different user, return 409.
        3. Compute content_hash and check the content cache.  On a cache
           hit, store the document as ``completed`` immediately.
        4. Otherwise, acquire a rate-limit slot, create the document as
           ``queued``, and start the pipeline.
    """
    content_hash = compute_content_hash(content)

    # ── Crosswalk: repeat ref → update existing ──────────────
    if client_doc_ref is not None:
        existing = await _find_by_ref(client_doc_ref)
        if existing is not None:
            if existing["user_id"] != user_id:
                raise HTTPException(
                    status_code=409,
                    detail="client_doc_ref is already owned by another user",
                )
            # Same user, same ref → treat as an update (new version)
            return await _do_update(
                str(existing["_id"]),
                user_id,
                content,
                title=title,
                is_resubmit=True,
            )

    # ── Content cache check ──────────────────────────────────
    cached = await get_cached_result(user_id, content_hash)
    if cached is not None:
        return await _create_completed_from_cache(
            user_id, title, content, content_hash, client_doc_ref, cached
        )

    # ── Rate-limit check ─────────────────────────────────────
    # We need a doc_id to track in the rate limiter.  Pre-generate the
    # ObjectId so we can acquire the slot before the insert succeeds.
    doc_id = ObjectId()
    if not await rate_limiter.acquire(user_id, str(doc_id)):
        raise HTTPException(
            status_code=429,
            detail=(
                f"Concurrent document limit reached "
                f"(max {settings.rate_limit_max_concurrent})"
            ),
        )

    # ── Create document ──────────────────────────────────────
    run_id = new_run_id()
    now = _now()
    doc = {
        "_id": doc_id,
        "user_id": user_id,
        "title": title,
        "content": content,
        "content_hash": content_hash,
        "version": 1,
        "pipeline_run_id": run_id,
        "status": "queued",
        **_fresh_stages(),
        "summary": {"text": None, "content_hash": None},
        "tags": {"keywords": None, "content_hash": None},
        "created_at": now,
        "updated_at": now,
    }
    # Only include client_doc_ref when present — MongoDB sparse unique
    # indexes exclude absent fields but still index explicit null values,
    # so storing null would cause duplicate-key errors on any two documents
    # that have no ref.
    if client_doc_ref is not None:
        doc["client_doc_ref"] = client_doc_ref

    try:
        await get_documents_collection().insert_one(doc)
    except Exception as exc:
        # Duplicate key on client_doc_ref (race) or other error.
        # Release the slot we just acquired.
        await rate_limiter.release(user_id, str(doc_id))
        if "duplicate key" in str(exc).lower():
            raise HTTPException(
                status_code=409,
                detail="client_doc_ref already exists",
            )
        logger.error("Failed to insert document: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to create document")

    # ── Start pipeline ───────────────────────────────────────
    start_pipeline(str(doc_id), user_id, content, content_hash, run_id)

    logger.info("Document submitted: doc=%s user=%s", doc_id, user_id)
    return {"document_id": str(doc_id), "status": "queued", "cached": False}


async def _create_completed_from_cache(
    user_id: str,
    title: str,
    content: str,
    content_hash: str,
    client_doc_ref: str | None,
    cached: dict,
) -> dict:
    """Create a document in ``completed`` state using cached results."""
    run_id = new_run_id()
    now = _now()
    doc_id = ObjectId()

    doc: dict = {
        "_id": doc_id,
        "user_id": user_id,
        "title": title,
        "content": content,
        "content_hash": content_hash,
        "version": 1,
        "pipeline_run_id": run_id,
        "status": "completed",
        "stage1": {
            "status": "completed",
            "error": None,
            "attempt": 1,
            "started_at": now,
            "completed_at": now,
        },
        "stage2": {
            "status": "completed",
            "error": None,
            "attempt": 1,
            "started_at": now,
            "completed_at": now,
        },
        "summary": {"text": cached["summary"], "content_hash": content_hash},
        "tags": {"keywords": cached["tags"], "content_hash": content_hash},
        "created_at": now,
        "updated_at": now,
    }
    if client_doc_ref is not None:
        doc["client_doc_ref"] = client_doc_ref

    try:
        await get_documents_collection().insert_one(doc)
    except Exception as exc:
        if "duplicate key" in str(exc).lower():
            raise HTTPException(
                status_code=409,
                detail="client_doc_ref already exists",
            )
        logger.error("Failed to insert cached document: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to create document")

    logger.info("Document submitted (cached): doc=%s user=%s", doc_id, user_id)
    return {"document_id": str(doc_id), "status": "completed", "cached": True}


# ── Update (PATCH) ──────────────────────────────────────────

async def update_document(
    document_id: str,
    user_id: str,
    content: str,
) -> dict:
    """Handle PATCH /documents/{document_id}.

    Replaces content in place and reprocesses from scratch.  Uses
    optimistic concurrency via the ``version`` field: if two PATCHes
    race, the second gets HTTP 409.
    """
    return await _do_update(document_id, user_id, content)


async def _do_update(
    document_id: str,
    user_id: str,
    content: str,
    *,
    title: str | None = None,
    is_resubmit: bool = False,
) -> dict:
    """Shared update logic for PATCH and repeat-ref submissions."""
    oid = _to_object_id(document_id)
    coll = get_documents_collection()

    # ── Read & ownership check ──────────────────────────────
    doc = await coll.find_one({"_id": oid})
    if doc is None or doc["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Document not found")

    version = doc["version"]
    new_content_hash = compute_content_hash(content)

    # ── Content cache check ──────────────────────────────────
    cached = await get_cached_result(user_id, new_content_hash)
    if cached is not None:
        return await _apply_cached_update(
            oid, user_id, version, content, new_content_hash,
            cached, title, is_resubmit,
        )

    # ── Rate-limit check (only if doc was in terminal state) ─
    was_active = doc["status"] in _ACTIVE_STATES
    if not was_active:
        if not await rate_limiter.acquire(user_id, document_id):
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Concurrent document limit reached "
                    f"(max {settings.rate_limit_max_concurrent})"
                ),
            )

    # ── Optimistic-concurrency update ───────────────────────
    run_id = new_run_id()
    now = _now()
    set_fields: dict = {
        "content": content,
        "content_hash": new_content_hash,
        "pipeline_run_id": run_id,
        "status": "queued",
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
        "updated_at": now,
    }
    if title is not None:
        set_fields["title"] = title

    result = await coll.find_one_and_update(
        {"_id": oid, "version": version},
        {"$set": set_fields, "$inc": {"version": 1}},
        return_document=True,  # type: ignore[arg-type]
    )

    if result is None:
        # Version changed — another PATCH won the race
        if not was_active:
            await rate_limiter.release(user_id, document_id)
        raise HTTPException(
            status_code=409,
            detail="Document was modified by another request — please retry",
        )

    # ── Start new pipeline ───────────────────────────────────
    start_pipeline(document_id, user_id, content, new_content_hash, run_id)

    logger.info("Document updated: doc=%s user=%s version=%s", document_id, user_id, version + 1)
    return {"document_id": document_id, "status": "queued", "cached": False}


async def _apply_cached_update(
    oid: ObjectId,
    user_id: str,
    version: int,
    content: str,
    content_hash: str,
    cached: dict,
    title: str | None,
    is_resubmit: bool,
) -> dict:
    """Apply a PATCH/resubmit where the new content is already cached."""
    coll = get_documents_collection()

    # We need to know if the document was active to release the slot.
    doc = await coll.find_one({"_id": oid})
    if doc is None or doc["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Document not found")

    was_active = doc["status"] in _ACTIVE_STATES
    run_id = new_run_id()
    now = _now()

    set_fields: dict = {
        "content": content,
        "content_hash": content_hash,
        "pipeline_run_id": run_id,
        "status": "completed",
        "stage1": {
            "status": "completed",
            "error": None,
            "attempt": 1,
            "started_at": now,
            "completed_at": now,
        },
        "stage2": {
            "status": "completed",
            "error": None,
            "attempt": 1,
            "started_at": now,
            "completed_at": now,
        },
        "summary": {"text": cached["summary"], "content_hash": content_hash},
        "tags": {"keywords": cached["tags"], "content_hash": content_hash},
        "updated_at": now,
    }
    if title is not None:
        set_fields["title"] = title

    result = await coll.find_one_and_update(
        {"_id": oid, "version": version},
        {"$set": set_fields, "$inc": {"version": 1}},
        return_document=True,  # type: ignore[arg-type]
    )

    if result is None:
        raise HTTPException(
            status_code=409,
            detail="Document was modified by another request — please retry",
        )

    # Release the rate-limit slot if the document was previously active.
    # The old pipeline's release will be a no-op (pipeline_run_id changed).
    if was_active:
        await rate_limiter.release(user_id, str(oid))

    logger.info("Document updated (cached): doc=%s", oid)
    return {"document_id": str(oid), "status": "completed", "cached": True}


# ── Get single document ────────────────────────────────────

async def get_document(document_id: str, user_id: str) -> dict:
    """Handle GET /documents/{document_id}.

    Returns the full document with per-stage status and staleness
    information (content_hash vs summary/tags content_hash).
    """
    oid = _to_object_id(document_id)
    doc = await get_documents_collection().find_one({"_id": oid})

    if doc is None or doc["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Document not found")

    return _build_status_response(doc)


# ── List user documents ─────────────────────────────────────

async def list_documents(
    user_id: str,
    caller_user_id: str,
    page: int = 1,
    page_size: int = 20,
    status_filter: str | None = None,
) -> dict:
    """Handle GET /users/{user_id}/documents.

    The *caller_user_id* (from X-User-Id header) must match the path
    *user_id* — otherwise 404 to avoid confirming the user's documents
    exist.
    """
    if user_id != caller_user_id:
        raise HTTPException(status_code=404, detail="Not found")

    page = max(1, page)
    page_size = max(1, min(page_size, 100))

    query: dict = {"user_id": user_id}
    if status_filter is not None:
        query["status"] = status_filter

    coll = get_documents_collection()
    total = await coll.count_documents(query)

    skip = (page - 1) * page_size
    cursor = coll.find(query, {"content": 0}).sort("created_at", -1).skip(skip).limit(page_size)
    docs = await cursor.to_list(length=page_size)

    items = [
        {
            "document_id": str(d["_id"]),
            "title": d["title"],
            "status": d["status"],
            "created_at": d["created_at"],
        }
        for d in docs
    ]

    return {
        "documents": items,
        "page": page,
        "page_size": page_size,
        "total": total,
        "has_next": (skip + page_size) < total,
    }


# ── Crosswalk: lookup by external reference ─────────────────

async def get_document_by_ref(client_doc_ref: str, user_id: str) -> dict:
    """Handle GET /documents/by-ref/{client_doc_ref}.

    Resolves via the unique sparse index on ``client_doc_ref``
    (no collection scan).  Returns 404 for non-owner or not-found.
    """
    doc = await _find_by_ref(client_doc_ref)
    if doc is None or doc["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Document not found")
    return _build_status_response(doc)


# ── Internal helpers ───────────────────────────────────────

async def _find_by_ref(client_doc_ref: str) -> dict | None:
    """Find a document by client_doc_ref (uses the indexed lookup)."""
    return await get_documents_collection().find_one({"client_doc_ref": client_doc_ref})


def _build_status_response(doc: dict) -> dict:
    """Construct the API response dict from a raw MongoDB document."""
    summary = doc.get("summary") or {}
    tags = doc.get("tags") or {}
    stage1 = doc.get("stage1") or {}
    stage2 = doc.get("stage2") or {}

    return {
        "document_id": str(doc["_id"]),
        "user_id": doc["user_id"],
        "title": doc["title"],
        "status": doc["status"],
        "stage1": {
            "status": stage1.get("status", "queued"),
            "error": stage1.get("error"),
            "attempt": stage1.get("attempt", 0),
        },
        "stage2": {
            "status": stage2.get("status", "queued"),
            "error": stage2.get("error"),
            "attempt": stage2.get("attempt", 0),
        },
        "summary": summary.get("text"),
        "tags": tags.get("keywords"),
        "content_hash": doc["content_hash"],
        "summary_content_hash": summary.get("content_hash"),
        "tags_content_hash": tags.get("content_hash"),
        "version": doc.get("version", 1),
        "created_at": doc["created_at"],
        "updated_at": doc.get("updated_at", doc["created_at"]),
    }
