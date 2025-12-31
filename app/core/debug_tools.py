import os

from fastapi import HTTPException, Request, status


DEBUG_TOKEN_ENV = "DEBUG_TOKEN"
DEBUG_TOOLS_ENV = "DEBUG_TOOLS"

def get_debug_token() -> str | None:
    token = os.getenv(DEBUG_TOKEN_ENV)
    return token or None


def _debug_stability_enabled() -> bool:
    return os.getenv("DEBUG_STABILITY") == "1"


def _debug_tools_enabled() -> bool:
    return os.getenv(DEBUG_TOOLS_ENV) == "1"


def require_debug_tools(request: Request) -> None:
    reveal_errors = _debug_stability_enabled()
    debug_token = get_debug_token()
    if not debug_token:
        if reveal_errors:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Debug token not configured.",
            )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

    request_token = request.headers.get("X-Debug-Token")
    if not request_token:
        if reveal_errors:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing debug token.",
            )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    if request_token != debug_token:
        if reveal_errors:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid debug token.",
            )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")


def require_debug_tools_enabled(request: Request) -> None:
    if not _debug_tools_enabled():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    require_debug_tools(request)
