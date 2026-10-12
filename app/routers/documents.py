"""Document endpoints: submit, update, poll status, and crosswalk lookup.

Route ordering matters: ``/by-ref/{ref}`` MUST be registered before
``/{document_id}`` so FastAPI doesn't capture the literal "by-ref" as a
document_id.
"""

import logging

from fastapi import APIRouter, Depends, Path, status

from app.dependencies import require_user_id
from app.models import (
    DocumentStatusResponse,
    DocumentSubmit,
    DocumentSubmitResponse,
    DocumentUpdate,
    DocumentUpdateResponse,
)
from app.services import document_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/documents", tags=["documents"])


# ── POST /documents — submit a document ─────────────────────

@router.post(
    "",
    response_model=DocumentSubmitResponse,
    status_code=status.HTTP_201_CREATED,
)
async def submit_document(body: DocumentSubmit) -> DocumentSubmitResponse:
    """Accept user_id, title, content, and optional client_doc_ref.

    - Validates input via Pydantic.
    - Checks the content cache for an instant hit.
    - Enforces the per-user concurrent-document rate limit (HTTP 429).
    - Stores the document in MongoDB with status ``queued`` and starts
      the background pipeline.
    - If *client_doc_ref* already exists for the same user, treats the
      submission as an update (new version) to that document.
    """
    result = await document_service.submit_document(
        user_id=body.user_id,
        title=body.title,
        content=body.content,
        client_doc_ref=body.client_doc_ref,
    )
    return DocumentSubmitResponse(**result)


# ── GET /documents/by-ref/{client_doc_ref} — crosswalk ──────
#
# MUST be declared before /{document_id} so FastAPI doesn't
# capture the literal segment "by-ref" as a document_id.

@router.get(
    "/by-ref/{client_doc_ref}",
    response_model=DocumentStatusResponse,
    status_code=status.HTTP_200_OK,
)
async def get_document_by_ref(
    client_doc_ref: str = Path(..., description="External reference ID from partner CMS"),
    caller_user_id: str = Depends(require_user_id),
) -> DocumentStatusResponse:
    """Resolve a document by its external ``client_doc_ref``.

    Uses the unique sparse index on ``client_doc_ref`` (no collection
    scan).  Returns 404 for not-found or non-owner.
    """
    result = await document_service.get_document_by_ref(
        client_doc_ref=client_doc_ref,
        user_id=caller_user_id,
    )
    return DocumentStatusResponse(**result)


# ── GET /documents/{document_id} — poll status ──────────────

@router.get(
    "/{document_id}",
    response_model=DocumentStatusResponse,
    status_code=status.HTTP_200_OK,
)
async def get_document(
    document_id: str = Path(..., description="ObjectId of the document"),
    caller_user_id: str = Depends(require_user_id),
) -> DocumentStatusResponse:
    """Return the document's current position across both pipeline stages.

    Once complete, includes the summary and tags.  The response carries
    ``content_hash``, ``summary_content_hash``, and ``tags_content_hash``
    so a caller can tell unambiguously whether the stored results reflect
    the current content or are stale.
    """
    result = await document_service.get_document(
        document_id=document_id,
        user_id=caller_user_id,
    )
    return DocumentStatusResponse(**result)


# ── PATCH /documents/{document_id} — update content ─────────

@router.patch(
    "/{document_id}",
    response_model=DocumentUpdateResponse,
    status_code=status.HTTP_200_OK,
)
async def update_document(
    document_id: str = Path(..., description="ObjectId of the document"),
    body: DocumentUpdate = ...,
    caller_user_id: str = Depends(require_user_id),
) -> DocumentUpdateResponse:
    """Replace the document's content and reprocess from scratch.

    Only the owner (identified via ``X-User-Id`` header) may update the
    document.  Uses optimistic concurrency on ``version``: a racing PATCH
    that incremented the version returns HTTP 409.
    """
    result = await document_service.update_document(
        document_id=document_id,
        user_id=caller_user_id,
        content=body.content,
    )
    return DocumentUpdateResponse(**result)
