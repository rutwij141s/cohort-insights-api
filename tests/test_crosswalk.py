"""Tests for the crosswalk / external reference feature.

client_doc_ref behavior:
    - POST with a new ref → creates a new document (201)
    - POST with the same ref, same user, same content → cached result (completed)
    - POST with the same ref, same user, different content → updates the existing
      document (new version, reprocessed)
    - POST with the same ref, different user → 409 Conflict
    - GET /documents/by-ref/{ref} → resolves via index
    - GET by-ref for non-owner → 404
"""

import pytest

from tests.conftest import wait_for_status


@pytest.mark.integration
async def test_submit_with_ref_and_lookup(client):
    """POST with client_doc_ref, then GET by-ref returns the document."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Crosswalk Test",
        "content": "Content about crosswalk identifier mapping systems integration.",
        "client_doc_ref": "ext_ref_001",
    })
    assert resp.status_code == 201
    doc_id = resp.json()["document_id"]

    # Wait for completion
    await wait_for_status(client, doc_id, "user_a", {"completed"})

    # Lookup by ref
    resp = await client.get(
        "/documents/by-ref/ext_ref_001",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["document_id"] == doc_id


@pytest.mark.integration
async def test_by_ref_not_found(client):
    """GET by-ref with a non-existent ref returns 404."""
    resp = await client.get(
        "/documents/by-ref/nonexistent_ref",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_by_ref_non_owner_returns_404(client):
    """GET by-ref for another user's document returns 404."""
    await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Owner Test",
        "content": "Content owned by user_a about ownership and access control.",
        "client_doc_ref": "ref_user_a",
    })

    # user_b tries to look up user_a's ref
    resp = await client.get(
        "/documents/by-ref/ref_user_a",
        headers={"X-User-Id": "user_b"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_repeat_ref_same_content_returns_cached(client):
    """Re-submitting the same ref with identical content returns cached result."""
    content = "Identical content about caching strategies and performance optimization."
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "First Submission",
        "content": content,
        "client_doc_ref": "repeat_ref_001",
    })
    assert resp.status_code == 201
    doc_id_1 = resp.json()["document_id"]

    # Wait for first processing to complete
    await wait_for_status(client, doc_id_1, "user_a", {"completed"})

    # Re-submit with same ref and same content
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Second Submission (same content)",
        "content": content,
        "client_doc_ref": "repeat_ref_001",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["document_id"] == doc_id_1  # Same document_id
    assert data["cached"] is True
    assert data["status"] == "completed"


@pytest.mark.integration
async def test_repeat_ref_different_content_updates(client):
    """Re-submitting the same ref with different content updates the document."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Original Content",
        "content": "First version content about dinosaurs and prehistoric animals.",
        "client_doc_ref": "repeat_ref_002",
    })
    assert resp.status_code == 201
    doc_id = resp.json()["document_id"]

    await wait_for_status(client, doc_id, "user_a", {"completed"})

    # Re-submit with same ref but different content
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Updated Content",
        "content": "Second version content about modern technology smartphones computers.",
        "client_doc_ref": "repeat_ref_002",
    })
    assert resp.status_code == 201
    data = resp.json()
    # Same document_id (update, not new)
    assert data["document_id"] == doc_id
    assert data["status"] == "queued"

    # Wait for reprocessing
    result = await wait_for_status(client, doc_id, "user_a", {"completed"})
    # Tags should reflect the new content
    tags_lower = [t.lower() for t in result["tags"]]
    new_words = {"modern", "technology", "smartphones", "computers"}
    assert any(w in new_words for w in tags_lower)


@pytest.mark.integration
async def test_repeat_ref_different_user_conflict(client):
    """Submitting a ref owned by another user returns 409."""
    # user_a creates with ref
    await client.post("/documents", json={
        "user_id": "user_a",
        "title": "User A Doc",
        "content": "Content owned by user_a for conflict testing purposes.",
        "client_doc_ref": "conflict_ref_001",
    })

    # user_b tries to use the same ref
    resp = await client.post("/documents", json={
        "user_id": "user_b",
        "title": "User B Doc",
        "content": "Content from user_b trying to use the same reference.",
        "client_doc_ref": "conflict_ref_001",
    })
    assert resp.status_code == 409
    assert "client_doc_ref" in resp.json()["detail"].lower()


@pytest.mark.integration
async def test_optional_client_doc_ref(client):
    """Documents without client_doc_ref are created normally."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "No Ref",
        "content": "Content without an external reference identifier attached.",
    })
    assert resp.status_code == 201
    assert resp.json()["status"] == "queued"


@pytest.mark.integration
async def test_by_ref_requires_user_header(client):
    """GET by-ref without X-User-Id returns 401."""
    await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Header Test",
        "content": "Content for testing header requirements on crosswalk.",
        "client_doc_ref": "header_test_ref",
    })

    resp = await client.get("/documents/by-ref/header_test_ref")
    assert resp.status_code == 401
