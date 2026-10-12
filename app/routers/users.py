"""User document listing endpoint."""

import logging

from fastapi import APIRouter, Depends, Path, Query

from app.dependencies import require_user_id
from app.models import PaginatedDocuments
from app.services import document_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["users"])


@router.get(
    "/users/{user_id}/documents",
    response_model=PaginatedDocuments,
)
async def list_user_documents(
    user_id: str = Path(..., description="User ID whose documents to list"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status_filter: str | None = Query(
        None, alias="status",
        description="Filter by document status: queued, processing, enriching, completed, failed",
    ),
    caller_user_id: str = Depends(require_user_id),
) -> PaginatedDocuments:
    """List a user's documents, newest first, with optional status filter.

    The ``X-User-Id`` header must match the path ``user_id`` — otherwise
    404 (we don't confirm that another user's documents exist).
    """
    result = await document_service.list_documents(
        user_id=user_id,
        caller_user_id=caller_user_id,
        page=page,
        page_size=page_size,
        status_filter=status_filter,
    )
    return PaginatedDocuments(**result)
