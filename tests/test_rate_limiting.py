"""Tests for per-user concurrent-document rate limiting.

Each user may have at most 3 documents in {queued, processing, enriching}.
Exceeding the limit returns HTTP 429.
"""

import asyncio

import pytest

from tests.conftest import wait_for_status


@pytest.mark.integration
async def test_three_concurrent_documents_allowed(client):
    """A user can submit up to 3 concurrent documents."""
    for i in range(3):
        resp = await client.post("/documents", json={
            "user_id": "user_rate",
            "title": f"Doc {i}",
            "content": f"Content number {i} with unique words testing rate limiting.",
        })
        assert resp.status_code == 201


@pytest.mark.integration
async def test_fourth_document_rejected_429(client):
    """The 4th concurrent document is rejected with 429."""
    for i in range(3):
        await client.post("/documents", json={
            "user_id": "user_rate",
            "title": f"Doc {i}",
            "content": f"Content {i} for rate limiting testing purposes here.",
        })

    resp = await client.post("/documents", json={
        "user_id": "user_rate",
        "title": "Doc 3 (over limit)",
        "content": "This should be rejected due to rate limit exceeded.",
    })
    assert resp.status_code == 429


@pytest.mark.integration
async def test_slot_freed_after_completion(client):
    """After a document completes, the slot is freed for new submissions."""
    # Submit 3 documents
    doc_ids = []
    for i in range(3):
        resp = await client.post("/documents", json={
            "user_id": "user_rate",
            "title": f"Doc {i}",
            "content": f"Content {i} for testing slot freeing after completion.",
        })
        doc_ids.append(resp.json()["document_id"])

    # 4th should be rejected
    resp = await client.post("/documents", json={
        "user_id": "user_rate",
        "title": "Over limit",
        "content": "Should be rejected.",
    })
    assert resp.status_code == 429

    # Wait for first document to complete
    await wait_for_status(client, doc_ids[0], "user_rate", {"completed"})

    # Now a new submission should succeed
    resp = await client.post("/documents", json={
        "user_id": "user_rate",
        "title": "After slot freed",
        "content": "This should now be accepted because a slot was freed.",
    })
    assert resp.status_code == 201


@pytest.mark.integration
async def test_rate_limit_per_user_independent(client):
    """Rate limits are independent per user."""
    # user_a fills up
    for i in range(3):
        await client.post("/documents", json={
            "user_id": "user_a",
            "title": f"User A Doc {i}",
            "content": f"Content {i} for user_a rate limit testing.",
        })

    # user_a's 4th is rejected
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "User A over limit",
        "content": "Should be rejected for user_a.",
    })
    assert resp.status_code == 429

    # user_b can still submit
    resp = await client.post("/documents", json={
        "user_id": "user_b",
        "title": "User B Doc",
        "content": "User_b should be able to submit independently.",
    })
    assert resp.status_code == 201


@pytest.mark.integration
async def test_patch_completed_counts_toward_limit(client):
    """PATCH on a completed document re-enters the active pipeline and counts toward the limit."""
    # Submit and complete 3 documents
    doc_ids = []
    for i in range(3):
        resp = await client.post("/documents", json={
            "user_id": "user_patch",
            "title": f"Doc {i}",
            "content": f"Content {i} for patch rate limit testing words here.",
        })
        doc_ids.append(resp.json()["document_id"])

    # Wait for all to complete
    for doc_id in doc_ids:
        await wait_for_status(client, doc_id, "user_patch", {"completed"})

    # Now PATCH all 3 — they re-enter the active pipeline
    for doc_id in doc_ids:
        resp = await client.patch(
            f"/documents/{doc_id}",
            json={"content": f"Updated content for {doc_id} with new words testing."},
            headers={"X-User-Id": "user_patch"},
        )
        assert resp.status_code == 200

    # A new submission should be rejected (3 active from patches)
    resp = await client.post("/documents", json={
        "user_id": "user_patch",
        "title": "New doc",
        "content": "This should be rejected because patches took all slots.",
    })
    assert resp.status_code == 429


@pytest.mark.integration
async def test_cached_submission_does_not_consume_slot(client):
    """A cached submission (identical content) does not consume a rate-limit slot."""
    content = "Cached content for testing that cache hits don't consume rate limit slots."

    # First submission — processes and completes
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "First",
        "content": content,
    })
    doc_id = resp.json()["document_id"]
    await wait_for_status(client, doc_id, "user_cache", {"completed"})

    # Now submit 3 *new* documents (different content each time)
    for i in range(3):
        resp = await client.post("/documents", json={
            "user_id": "user_cache",
            "title": f"New Doc {i}",
            "content": f"Unique content {i} different from cached content here.",
        })
        assert resp.status_code == 201

    # Submit the cached content again — should still succeed (doesn't consume a slot)
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Cached",
        "content": content,
    })
    assert resp.status_code == 201
    assert resp.json()["cached"] is True
