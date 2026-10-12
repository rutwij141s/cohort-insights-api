"""Shared FastAPI dependencies."""

from fastapi import Header, HTTPException


async def require_user_id(
    x_user_id: str | None = Header(None, alias="X-User-Id"),
) -> str:
    """Extract and validate the caller's user ID from the X-User-Id header.

    Returns 401 (not 422) when the header is missing so the error is
    semantically correct for an authentication-style check.
    """
    if not x_user_id:
        raise HTTPException(
            status_code=401,
            detail="X-User-Id header is required",
        )
    return x_user_id
