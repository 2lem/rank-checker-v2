import os

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

TOKEN_URL = "https://accounts.spotify.com/api/token"
SEARCH_URL = "https://api.spotify.com/v1/search"
PLAYLIST_URL = "https://api.spotify.com/v1/playlists/{}"
MARKETS_URL = "https://api.spotify.com/v1/markets"
RESULTS_LIMIT = 20

SPOTIFY_CONNECT_TIMEOUT = int(os.getenv("SPOTIFY_CONNECT_TIMEOUT", "5"))
SPOTIFY_READ_TIMEOUT = int(os.getenv("SPOTIFY_READ_TIMEOUT", "20"))
SPOTIFY_REQUEST_TIMEOUT = (SPOTIFY_CONNECT_TIMEOUT, SPOTIFY_READ_TIMEOUT)
SPOTIFY_MAX_CONCURRENCY = int(os.getenv("SPOTIFY_MAX_CONCURRENCY", "3"))
SPOTIFY_MAX_RETRY_AFTER = int(os.getenv("SPOTIFY_MAX_RETRY_AFTER", "60"))
