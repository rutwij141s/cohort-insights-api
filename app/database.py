"""Async MongoDB client and collection access."""

import logging

from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None
_db = None


def get_client() -> AsyncIOMotorClient:
    """Return the shared Motor client, creating it on first call."""
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(settings.mongodb_url)
    return _client


def reset_client() -> None:
    """Discard the current client handle without closing it.

    Used in tests to force a new client on the next event loop.
    """
    global _client, _db
    _client = None
    _db = None


def get_db():
    """Return the Motor database handle."""
    global _db
    if _db is None:
        _db = get_client()[settings.mongodb_database]
    return _db


def get_documents_collection():
    """Return the documents collection."""
    return get_db().documents


async def create_indexes() -> None:
    """Create all required indexes on startup."""
    coll = get_documents_collection()
    await coll.create_index([("user_id", 1), ("created_at", -1)])
    await coll.create_index([("user_id", 1), ("status", 1), ("created_at", -1)])
    await coll.create_index("client_doc_ref", unique=True, sparse=True)
    logger.info("MongoDB indexes created")


async def close_client() -> None:
    """Close the MongoDB client on shutdown."""
    global _client, _db
    if _client is not None:
        _client.close()
        _client = None
        _db = None
        logger.info("MongoDB client closed")
