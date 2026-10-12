"""Tests for ownership enforcement.

Only the user_id that submitted a document may read or update it.
A request for another user's document returns 404 (not 403) so the
document's existence is not confirmed to a non-owner.
"""

import pytest

from tests.conftest import wait_for_status


@pytest.mark.integration
async def test_other_user_cannot_get_document(client):
    """User B cannot read user A's document (404, not 403)."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Private Doc",
        "content": "This is user_a's private document content about ownership testing.",
    })
    doc_id = resp.json()["document_id"]

    # user_b tries to access
    resp = await client.get(
        f"/documents/{doc_id}",
        headers={"X-User-Id": "user_b"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_other_user_cannot_patch_document(client):
    """User B cannot update user A's document."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Protected Doc",
        "content": "User_a's document that should not be patchable by others.",
    })
    doc_id = resp.json()["document_id"]

    resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": "Malicious update from user_b trying to overwrite."},
        headers={"X-User-Id": "user_b"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_other_user_cannot_list_documents(client):
    """User B listing user A's documents gets 404."""
    await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Doc",
        "content": "User_a content that user_b should not see listed.",
    })

    resp = await client.get(
        "/users/user_a/documents",
        headers={"X-User-Id": "user_b"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_owner_can_access_own_documents(client):
    """The owner can read and list their own documents."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "My Doc",
        "content": "My own document content for ownership positive test.",
    })
    doc_id = resp.json()["document_id"]

    # Owner can GET
    resp = await client.get(
        f"/documents/{doc_id}",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 200
    assert resp.json()["document_id"] == doc_id

    # Owner can LIST
    resp = await client.get(
        "/users/user_a/documents",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


@pytest.mark.integration
async def test_missing_user_header_returns_401(client):
    """GET /documents/{id} without X-User-Id returns 401."""
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Test",
        "content": "Content for testing missing header authentication.",
    })
    doc_id = resp.json()["document_id"]

    resp = await client.get(f"/documents/{doc_id}")
    assert resp.status_code == 401


@pytest.mark.integration
async def test_other_user_cannot_access_by_ref(client):
    """User B cannot look up user A's document by ref."""
    await client.post("/documents", json={
        "user_id": "user_a",
        "title": "Ref Doc",
        "content": "User_a document with external reference for ownership testing.",
        "client_doc_ref": "own_ref_001",
    })

    resp = await client.get(
        "/documents/by-ref/own_ref_001",
        headers={"X-User-Id": "user_b"},
    )
    assert resp.status_code == 404


@pytest.mark.integration
async def test_nonexistent_and_nonowner_indistinguishable(client):
    """404 for both nonexistent and non-owner documents (no existence leak)."""
    # Nonexistent document
    resp = await client.get(
        "/documents/507f1f77bcf86cd799439011",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 404

    # Another user's document
    resp = await client.post("/documents", json={
        "user_id": "user_b",
        "title": "Hidden",
        "content": "This document should be invisible to user_a completely.",
    })
    doc_id = resp.json()["document_id"]

    resp = await client.get(
        f"/documents/{doc_id}",
        headers={"X-User-Id": "user_a"},
    )
    assert resp.status_code == 404

    # Both return the same error structure (no distinction)
    # We just verify both are 404 — the detail message should not differ
    # in a way that leaks existence.
