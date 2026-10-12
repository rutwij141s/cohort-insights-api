"""Per-user concurrency rate limiter backed by Redis.

Tracks the set of document_ids a user currently has in the active
pipeline states {queued, processing, enriching}.  The limit is the
*number of concurrent active documents*, not a requests-per-second
throttle.

Design
------
A Redis **set** per user stores the active document_ids::

    rate_limit:{user_id}  →  { doc_id_1, doc_id_2, … }

Check-and-add is performed atomically via a Lua script so there is no
window where two concurrent submissions could both pass the limit check.

Graceful degradation: if Redis is unreachable the limiter **fails open**
(submissions are allowed).  This trades strict quota enforcement for
availability.  See README for rationale.

The Lua script is registered once and reused via ``evalsha``.
"""

import logging
from typing import Optional

from app.config import settings
from app.redis_client import get_redis

logger = logging.getLogger(__name__)

_RATE_LIMIT_PREFIX = "rate_limit"
_KEY_TTL = 3600  # 1 hour safety-net TTL; refreshed on every operation

# Atomically add a document_id to the set and check the cardinality.
# Returns 1 if the document is accepted, 0 if it would exceed the limit.
_LUA_SCRIPT = """
local key = KEYS[1]
local doc_id = ARGV[1]
local max_concurrent = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

redis.call('SADD', key, doc_id)
local count = redis.call('SCARD', key)

if count > max_concurrent then
    redis.call('SREM', key, doc_id)
    return 0
end

redis.call('EXPIRE', key, ttl)
return count
"""

# SHA of the registered script (populated on first use)
_script_sha: Optional[str] = None


def _key(user_id: str) -> str:
    return f"{_RATE_LIMIT_PREFIX}:{user_id}"


async def _ensure_script() -> str:
    """Register the Lua script and cache its SHA."""
    global _script_sha
    if _script_sha is None:
        redis = get_redis()
        _script_sha = await redis.script_load(_LUA_SCRIPT)
    return _script_sha


async def _eval_acquire(redis, user_id: str, document_id: str) -> int:
    """Run the Lua check-and-add, re-registering the script if evicted."""
    sha = await _ensure_script()
    try:
        return await redis.evalsha(
            sha,
            1,
            _key(user_id),
            document_id,
            str(settings.rate_limit_max_concurrent),
            str(_KEY_TTL),
        )
    except Exception as exc:
        # NOSCRIPT or connection issue — re-register and retry once
        if "NOSCRIPT" in str(exc).upper():
            global _script_sha
            _script_sha = None
            sha = await _ensure_script()
            return await redis.evalsha(
                sha,
                1,
                _key(user_id),
                document_id,
                str(settings.rate_limit_max_concurrent),
                str(_KEY_TTL),
            )
        raise


async def acquire(user_id: str, document_id: str) -> bool:
    """Try to add *document_id* to the user's active set.

    Returns ``True`` if the document is accepted (under the limit),
    ``False`` if the user already has the maximum number of concurrent
    documents.
    """
    try:
        redis = get_redis()
        result = await _eval_acquire(redis, user_id, document_id)
        if result == 0:
            logger.info(
                "Rate limit exceeded for user=%s (limit=%d)",
                user_id,
                settings.rate_limit_max_concurrent,
            )
            return False
        logger.debug(
            "Rate limit acquired: user=%s doc=%s count=%s",
            user_id, document_id, result,
        )
        return True
    except Exception as exc:
        # Fail open — allow the submission but log the issue
        logger.warning("Rate limiter unavailable (failing open): %s", exc)
        return True


async def release(user_id: str, document_id: str) -> None:
    """Remove *document_id* from the user's active set."""
    try:
        redis = get_redis()
        await redis.srem(_key(user_id), document_id)
        logger.debug("Rate limit released: user=%s doc=%s", user_id, document_id)
    except Exception as exc:
        logger.warning("Rate limiter release failed (non-fatal): %s", exc)


async def get_active_count(user_id: str) -> int:
    """Return the number of active documents for *user_id*."""
    try:
        redis = get_redis()
        return await redis.scard(_key(user_id))
    except Exception as exc:
        logger.warning("Rate limiter count failed: %s", exc)
        return 0


async def clear_user(user_id: str) -> None:
    """Remove all entries for a user (used in tests / reconciliation)."""
    try:
        redis = get_redis()
        await redis.delete(_key(user_id))
    except Exception as exc:
        logger.warning("Rate limiter clear failed: %s", exc)
