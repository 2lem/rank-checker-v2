import os

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

TOKEN_URL = "https://accounts.spotify.com/api/token"
SEARCH_URL = "https://api.spotify.com/v1/search"
PLAYLIST_URL = "https://api.spotify.com/v1/playlists/{}"
MARKETS_URL = "https://api.spotify.com/v1/markets"
RESULTS_LIMIT = 20

SPOTIFY_REQUEST_TIMEOUT = 20
