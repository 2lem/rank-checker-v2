import os

from fastapi import HTTPException, Request, status


DEBUG_TOKEN_ENV = "DEBUG_TOKEN"

def get_debug_token() -> str | None:
    token = os.getenv(DEBUG_TOKEN_ENV)
    return token or None


def require_debug_tools(request: Request) -> None:
    debug_token = get_debug_token()
    if not debug_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Debug token not configured.",
        )

    request_token = request.headers.get("X-Debug-Token")
    if not request_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing debug token.")
    if request_token != debug_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid debug token.")
