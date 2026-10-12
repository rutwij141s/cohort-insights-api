"""Async Redis client."""

import logging

import redis.asyncio as redis

from app.config import settings

logger = logging.getLogger(__name__)

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    """Return the shared async Redis client, creating it on first call."""
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


def reset_redis() -> None:
    """Discard the current client handle without closing it.

    Used in tests to force a new client to be created on the next call,
    avoiding 'attached to a different loop' errors when pytest-asyncio
    creates a new event loop between tests.
    """
    global _client
    _client = None


async def close_redis() -> None:
    """Close the Redis connection on shutdown."""
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        except Exception as exc:
            logger.debug("Redis close error (non-fatal): %s", exc)
        finally:
            _client = None
        logger.info("Redis client closed")
