from .debug import router as debug_router
from .playlists import router as playlists_router
from .scans import router as scans_router

__all__ = ["debug_router", "playlists_router", "scans_router"]
