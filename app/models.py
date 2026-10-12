"""Pydantic models for request validation and response serialisation."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ── Request models ───────────────────────────────────────────

class DocumentSubmit(BaseModel):
    """Payload for POST /documents."""
    user_id: str = Field(..., min_length=1, max_length=100,
                         description="ID of the submitting user")
    title: str = Field(..., min_length=1, max_length=200,
                       description="Document title")
    content: str = Field(..., min_length=1,
                         description="Document content text")
    client_doc_ref: Optional[str] = Field(
        None, max_length=100,
        description="External reference ID from a partner CMS (optional, unique when present)"
    )


class DocumentUpdate(BaseModel):
    """Payload for PATCH /documents/{document_id}."""
    content: str = Field(..., min_length=1,
                         description="New content for the document")


# ── Response models ──────────────────────────────────────────

class DocumentSubmitResponse(BaseModel):
    """Response for POST /documents."""
    document_id: str
    status: str
    cached: bool = False


class DocumentUpdateResponse(BaseModel):
    """Response for PATCH /documents/{document_id}."""
    document_id: str
    status: str
    cached: bool = False


class StageInfo(BaseModel):
    """Per-stage pipeline status."""
    status: str  # queued | processing | completed | failed
    error: Optional[str] = None
    attempt: int = 0


class DocumentStatusResponse(BaseModel):
    """Response for GET /documents/{document_id}.

    The ``content_hash`` field is the hash of the *current* content.
    ``summary_content_hash`` and ``tags_content_hash`` are the hashes of
    the content that actually produced each derived field.  When all three
    match the results are guaranteed to reflect the current content —
    when they differ the results are stale and a re-run is in progress.
    """
    document_id: str
    user_id: str
    title: str
    status: str
    stage1: StageInfo
    stage2: StageInfo
    summary: Optional[str] = None
    tags: Optional[list[str]] = None
    content_hash: str
    summary_content_hash: Optional[str] = None
    tags_content_hash: Optional[str] = None
    version: int
    created_at: datetime
    updated_at: datetime


class DocumentListItem(BaseModel):
    """Compact representation for listing endpoints."""
    document_id: str
    title: str
    status: str
    created_at: datetime


class PaginatedDocuments(BaseModel):
    """Paginated response for GET /users/{user_id}/documents."""
    documents: list[DocumentListItem]
    page: int
    page_size: int
    total: int
    has_next: bool


class HealthResponse(BaseModel):
    """Response for GET /health."""
    status: str
    mongodb: str
    redis: str
