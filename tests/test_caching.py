"""Tests for content-based caching.

Cache key is ``cache:{user_id}:{content_hash}`` — a function of the
content, NOT the document_id.  This means:
    - Submitting identical content returns cached results immediately.
    - PATCH to different content then PATCH back to original → cached.
    - Stale cache from old content never leaks into new content's results.
"""

import pytest

from tests.conftest import wait_for_status


@pytest.mark.integration
async def test_identical_content_returns_cached(client):
    """Submitting identical content after processing returns cached result."""
    content = "Cached content about caching mechanisms Redis and performance."

    # First submission — processes through the pipeline
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "First",
        "content": content,
    })
    assert resp.status_code == 201
    doc_id_1 = resp.json()["document_id"]
    assert resp.json()["cached"] is False

    data_1 = await wait_for_status(client, doc_id_1, "user_cache", {"completed"})
    summary_1 = data_1["summary"]
    tags_1 = data_1["tags"]

    # Second submission with identical content — should be cached
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Second (same content)",
        "content": content,
    })
    assert resp.status_code == 201
    data_2 = resp.json()
    assert data_2["cached"] is True
    assert data_2["status"] == "completed"
    doc_id_2 = data_2["document_id"]

    # The cached document should have the same summary and tags
    resp = await client.get(
        f"/documents/{doc_id_2}",
        headers={"X-User-Id": "user_cache"},
    )
    data_2_full = resp.json()
    assert data_2_full["summary"] == summary_1
    assert data_2_full["tags"] == tags_1


@pytest.mark.integration
async def test_cache_keyed_by_content_not_document_id(client):
    """Cache survives PATCH: a document PATCHed to new content then back to
    original content hits the cache."""
    original_content = "Original content about databases MongoDB Redis PostgreSQL."
    new_content = "Different content about astronomy planets stars galaxies."

    # Submit original content
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Cache Test",
        "content": original_content,
    })
    doc_id = resp.json()["document_id"]
    await wait_for_status(client, doc_id, "user_cache", {"completed"})

    # PATCH to different content (invalidates any doc_id-based cache)
    resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": new_content},
        headers={"X-User-Id": "user_cache"},
    )
    assert resp.status_code == 200
    await wait_for_status(client, doc_id, "user_cache", {"completed"})

    # PATCH back to original content — should hit the cache
    resp = await client.patch(
        f"/documents/{doc_id}",
        json={"content": original_content},
        headers={"X-User-Id": "user_cache"},
    )
    assert resp.status_code == 200
    assert resp.json()["cached"] is True
    assert resp.json()["status"] == "completed"


@pytest.mark.integration
async def test_different_users_have_separate_caches(client):
    """The same content submitted by different users uses separate cache entries."""
    content = "Shared content about distributed systems and consensus algorithms."

    # user_a submits and processes
    resp = await client.post("/documents", json={
        "user_id": "user_a",
        "title": "User A Doc",
        "content": content,
    })
    doc_a = resp.json()["document_id"]
    await wait_for_status(client, doc_a, "user_a", {"completed"})

    # user_b submits the same content — should NOT be cached (different user)
    resp = await client.post("/documents", json={
        "user_id": "user_b",
        "title": "User B Doc",
        "content": content,
    })
    assert resp.status_code == 201
    assert resp.json()["cached"] is False
    assert resp.json()["status"] == "queued"


@pytest.mark.integration
async def test_cache_hit_does_not_start_pipeline(client):
    """A cached submission creates a completed document without pipeline processing."""
    content = "Content for verifying that cache hits skip pipeline processing."

    # First submission — processes normally
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Process",
        "content": content,
    })
    doc_id = resp.json()["document_id"]
    await wait_for_status(client, doc_id, "user_cache", {"completed"})

    # Second submission — cached, should be immediately completed
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Cached",
        "content": content,
    })
    assert resp.json()["status"] == "completed"
    cached_doc_id = resp.json()["document_id"]

    # Verify stages are marked completed
    resp = await client.get(
        f"/documents/{cached_doc_id}",
        headers={"X-User-Id": "user_cache"},
    )
    data = resp.json()
    assert data["stage1"]["status"] == "completed"
    assert data["stage2"]["status"] == "completed"
    assert data["summary"] is not None
    assert data["tags"] is not None


@pytest.mark.integration
async def test_stale_cache_does_not_leak(client):
    """Old content's cache entry does not leak into new content's result."""
    content_a = "Content about elephants giraffes zebras and safari animals."
    content_b = "Content about rockets satellites orbits and space technology."

    # Process content_a
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Content A",
        "content": content_a,
    })
    await wait_for_status(client, resp.json()["document_id"], "user_cache", {"completed"})

    # Process content_b
    resp = await client.post("/documents", json={
        "user_id": "user_cache",
        "title": "Content B",
        "content": content_b,
    })
    data_b = await wait_for_status(
        client, resp.json()["document_id"], "user_cache", {"completed"}
    )

    # content_b's tags should relate to content_b, not content_a
    tags_lower = [t.lower() for t in data_b["tags"]]
    a_words = {"elephants", "giraffes", "zebras", "safari", "animals"}
    b_words = {"rockets", "satellites", "orbits", "space", "technology"}
    assert any(w in b_words for w in tags_lower)
    assert not any(w in a_words for w in tags_lower)
