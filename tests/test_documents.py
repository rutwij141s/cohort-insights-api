"""Tests for the core document lifecycle: submit, poll, and update."""

import pytest

from tests.conftest import wait_for_status


@pytest.mark.integration
async def test_submit_document_returns_201(client):
    """POST /documents creates a document and returns 201 with document_id."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Test Document",
        "content": "This is a test document about machine learning and data science.",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert "document_id" in data
    assert data["status"] == "queued"
    assert data["cached"] is False


@pytest.mark.integration
async def test_submit_validation_errors(client):
    """Empty content or title returns 422 validation error."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Test",
        "content": "",
    })
    assert resp.status_code == 422

    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "",
        "content": "content",
    })
    assert resp.status_code == 422


@pytest.mark.integration
async def test_get_document_status_flow(client):
    """Document transitions through queued → processing → enriching → completed."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Flow Test",
        "content": "This document discusses artificial intelligence and neural networks extensively.",
    })
    assert resp.status_code == 201
    doc_id = resp.json()["document_id"]

    # Wait for completion
    data = await wait_for_status(client, doc_id, "user_a", {"completed"})

    assert data["status"] == "completed"
    assert data["summary"] is not None
    assert "Summary:" in data["summary"]
    assert data["tags"] is not None
    assert len(data["tags"]) > 0
    assert data["stage1"]["status"] == "completed"
    assert data["stage2"]["status"] == "completed"


@pytest.mark.integration
async def test_get_document_not_found(client):
    """GET with a non-existent document_id returns 404."""
    resp = await client.get(
        "/documents/507f1f77bcf86cd799439011",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_get_document_invalid_object_id(client):
    """GET with an invalid ObjectId format returns 404."""
    resp = await client.get(
        "/documents/not-a-valid-id",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_patch_document_reprocesses(client):
    """PATCH replaces content and triggers reprocessing from scratch."""
    # Submit original
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Original Title",
        "content": "Original content about cats and dogs and pets in general.",
    })
    assert resp.status_code == 201
    doc_id = resp.json()["document_id"]

    # Wait for initial processing
    data = await wait_for_status(client, doc_id, "user_a", {"completed"})
    old_summary = data["summary"]
    old_tags = data["tags"]
    old_version = data["version"]
    assert old_version == 1

    # PATCH with new content
    resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": "New content about quantum computing and physics and particles."},
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

    # Wait for reprocessing
    data = await wait_for_status(client, doc_id, "user_a", {"completed"})
    assert data["version"] == old_version + 1
    assert data["summary"] != old_summary
    # Tags should be different (different content)
    assert set(data["tags"]) != set(old_tags)


@pytest.mark.integration
async def test_patch_requires_user_header(client):
    """PATCH without X-User-Id header returns 401."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Test",
        "content": "Some content here for testing purposes today.",
    })
    doc_id = resp.json()["document_id"]

    resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": "New content"},
    )
    assert resp.status_code == 401


@pytest.mark.integration
async def test_patch_nonexistent_returns_404(client):
    """PATCH on a non-existent document returns 404."""
    resp = await client.patch(
        "/documents/507f1f77bcf86cd799439011",
        json={"content": "New content"},
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_list_user_documents_paginated(client):
    """GET /users/{user_id}/documents returns paginated results newest first."""
    # Create 5 documents — wait for each to complete to avoid hitting the
    # 3-concurrent-document rate limit.
    doc_ids = []
    for i in range(5):
        resp = await client.post("/documents", json={
            "user_id": "user_a",
            "title": f"Doc {i}",
            "content": f"Content number {i} with some unique words like testing number{i} document.",
        })
        assert resp.status_code == 201, f"Submit {i} failed: {resp.status_code} {resp.text}"
        doc_ids.append(resp.json()["document_id"])
        # Wait for completion before submitting the next one to stay under the rate limit
        await wait_for_status(client, doc_ids[-1], "user_a", {"completed"})

    # Page 1 with size 2
    resp = await client.get(
        "/users/user_a/documents?page=1&page_size=2",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["page"] == 1
    assert data["page_size"] == 2
    assert data["total"] == 5
    assert len(data["documents"]) == 2
    assert data["has_next"] is True

    # Page 2
    resp = await client.get(
        "/users/user_a/documents?page=2&page_size=2",
        headers={"X-User-Id": "user_a"},
    )
    data = resp.json()
    assert data["page"] == 2
    assert len(data["documents"]) == 2

    # Page 3 (last page)
    resp = await client.get(
        "/users/user_a/documents?page=3&page_size=2",
        headers={"X-User-Id": "user_a"},
    )
    data = resp.json()
    assert len(data["documents"]) == 1
    assert data["has_next"] is False


@pytest.mark.integration
async def test_list_with_status_filter(client):
    """Status filter returns only documents matching the status."""
    # Create 3 documents
    for i in range(3):
        await client.post("/documents", json={
            "user_id": "user_a",
            "title": f"Doc {i}",
            "content": f"Content about topic number {i} with words testing hello world.",
        })

    # All should be queued or processing initially
    resp = await client.get(
        "/users/user_a/documents?status=queued",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 200
    data = resp.json()
    for doc in data["documents"]:
        assert doc["status"] == "queued"


@pytest.mark.integration
async def test_list_empty_user(client):
    """Listing for a user with no documents returns empty list."""
    resp = await client.get(
        "/users/empty_user/documents",
        headers={"X-User-Id": "empty_user"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["documents"] == []
    assert data["total"] == 0
