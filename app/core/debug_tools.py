import os

from fastapi import HTTPException, Request, status


DEBUG_TOOLS_ENV = "DEBUG_TOOLS"
DEBUG_TOKEN_ENV = "DEBUG_TOKEN"


def debug_tools_enabled() -> bool:
    return os.getenv(DEBUG_TOOLS_ENV) == "1"


def get_debug_token() -> str | None:
    token = os.getenv(DEBUG_TOKEN_ENV)
    return token or None


def require_debug_tools(request: Request) -> None:
    if not debug_tools_enabled():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    debug_token = get_debug_token()
    if debug_token:
        request_token = request.headers.get("X-Debug-Token")
        if request_token != debug_token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid debug token.")
