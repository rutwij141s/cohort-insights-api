"""Tests for the /health endpoint."""

import pytest


@pytest.mark.integration
async def test_health_healthy(client):
    """GET /health returns 200 when MongoDB and Redis are reachable."""
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["mongodb"] == "healthy"
    assert data["redis"] == "healthy"
