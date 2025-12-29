from app.api.routes.basic_rank_checker import router as basic_rank_checker_router
from app.api.routes.debug import router as debug_router
from app.api.routes.playlists import router as playlists_router
from app.api.scans import router as scans_router

__all__ = ["basic_rank_checker_router", "debug_router", "playlists_router", "scans_router"]
