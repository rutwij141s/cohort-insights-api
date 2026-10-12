"""Content-based cache backed by Redis.

The cache key is ``cache:{user_id}:{content_hash}`` — a function of the
content (via its SHA-256 hash) and the owning user, **never** the
document_id.  This guarantees that a PATCH (which changes content_hash)
cannot read a stale cache entry from the old content: the key simply
does not match.

Graceful degradation: if Redis is unreachable, every cache call
returns ``None`` / ``False`` and the pipeline runs normally.
"""

import json
import logging
from typing import Optional

from app.config import settings
from app.redis_client import get_redis

logger = logging.getLogger(__name__)

_CACHE_PREFIX = "cache"


def _key(user_id: str, content_hash: str) -> str:
    return f"{_CACHE_PREFIX}:{user_id}:{content_hash}"


async def get_cached_result(user_id: str, content_hash: str) -> Optional[dict]:
    """Return cached ``{summary, tags}`` for the content, or ``None``."""
    try:
        redis = get_redis()
        raw = await redis.get(_key(user_id, content_hash))
        if raw is None:
            return None
        # Refresh TTL on read so popular entries stay warm
        await redis.expire(_key(user_id, content_hash), settings.cache_ttl)
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Cache read failed (degrading to miss): %s", exc)
        return None


async def set_cached_result(
    user_id: str, content_hash: str, summary: str, tags: list[str]
) -> None:
    """Store computed results in the cache with a TTL."""
    try:
        redis = get_redis()
        payload = json.dumps({"summary": summary, "tags": tags})
        await redis.set(_key(user_id, content_hash), payload, ex=settings.cache_ttl)
    except Exception as exc:
        logger.warning("Cache write failed (non-fatal): %s", exc)


async def invalidate(user_id: str, content_hash: str) -> None:
    """Explicitly remove a cache entry."""
    try:
        redis = get_redis()
        await redis.delete(_key(user_id, content_hash))
    except Exception as exc:
        logger.warning("Cache invalidate failed (non-fatal): %s", exc)
