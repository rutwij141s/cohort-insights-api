"""Content hashing utilities."""

import hashlib


def compute_content_hash(content: str) -> str:
    """Return the SHA-256 hex digest of *content*.

    Used as the cache key, staleness marker, and content-comparison
    primitive throughout the application.
    """
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
