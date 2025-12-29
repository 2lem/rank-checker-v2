import os
import re
import time
import base64
import logging
import textwrap
import threading
from html import escape
from uuid import UUID, uuid4
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import streamlit as st
from streamlit.components.v1 import html as st_html
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

CSV_EXPORT_COLUMNS = [
    "searched_at",
    "keyword",
    "country",
    "rank",
    "playlist_name",
    "playlist_owner",
    "playlist_followers",
    "songs_count",
    "playlist_last_track_added_at",
    "playlist_description",
    "playlist_url",
    "is_your_playlist",
]

COUNTRIES = {
    "Argentina": "AR",
    "Australia": "AU",
    "Austria": "AT",
    "Belgium": "BE",
    "Bolivia": "BO",
    "Brazil": "BR",
    "Bulgaria": "BG",
    "Canada": "CA",
    "Chile": "CL",
    "Colombia": "CO",
    "Costa Rica": "CR",
    "Croatia": "HR",
    "Cyprus": "CY",
    "Czechia": "CZ",
    "Denmark": "DK",
    "Dominican Republic": "DO",
    "Ecuador": "EC",
    "Egypt": "EG",
    "El Salvador": "SV",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Guatemala": "GT",
    "Honduras": "HN",
    "Hong Kong": "HK",
    "Hungary": "HU",
    "Iceland": "IS",
    "India": "IN",
    "Indonesia": "ID",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Malaysia": "MY",
    "Mexico": "MX",
    "Morocco": "MA",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Nicaragua": "NI",
    "Norway": "NO",
    "Panama": "PA",
    "Paraguay": "PY",
    "Peru": "PE",
    "Philippines": "PH",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Saudi Arabia": "SA",
    "Singapore": "SG",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "South Africa": "ZA",
    "South Korea": "KR",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Taiwan": "TW",
    "Thailand": "TH",
    "Turkey": "TR",
    "United Arab Emirates": "AE",
    "United Kingdom": "GB",
    "United States": "US",
    "Uruguay": "UY",
    "Venezuela": "VE",
    "Vietnam": "VN",
}
COUNTRY_NAMES = sorted(COUNTRIES.keys())

load_dotenv()

_db_smoke_test_started = False
_db_smoke_test_lock = threading.Lock()
_db_schema_setup_started = False
_db_schema_setup_lock = threading.Lock()


def _is_production_environment() -> bool:
    return os.getenv("APP_ENV", "").strip().lower() == "production"


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


DB_STATUS_INDICATOR_ENABLED = _is_truthy(os.getenv("SHOW_DB_STATUS_INDICATOR"))
DEBUG_UI_ENABLED = False


def _is_debug_ui_enabled() -> bool:
    global DEBUG_UI_ENABLED
    if DEBUG_UI_ENABLED:
        return True

    env_value = os.getenv("DEBUG_UI")
    if env_value is not None:
        DEBUG_UI_ENABLED = _is_truthy(env_value)
        return DEBUG_UI_ENABLED

    try:
        secrets_value = st.secrets.get("DEBUG_UI")
        if secrets_value is not None:
            DEBUG_UI_ENABLED = _is_truthy(str(secrets_value))
            return DEBUG_UI_ENABLED
    except Exception:
        pass

    return False


def _get_query_params() -> dict:
    try:
        params = st.query_params
        if params is not None:
            return dict(params)
    except Exception:
        pass

    try:
        return st.experimental_get_query_params()
    except Exception:
        return {}


def _update_query_params(params: dict):
    try:
        st.query_params.clear()
        for key, value in params.items():
            if value is None:
                continue
            st.query_params[key] = value
    except Exception:
        try:
            st.experimental_set_query_params(**{k: v for k, v in params.items() if v is not None})
        except Exception:
            pass


def _flatten_query_params(params: dict) -> dict:
    flat: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, list):
            if value:
                flat[key] = value[-1]
        else:
            flat[key] = value
    return flat


def safe_html(html: str) -> str:
    if not html:
        return ""
    html = textwrap.dedent(html)
    return html.strip()


def render_scroll_anchor(anchor_id: str) -> None:
    st.markdown(f'<div id="{anchor_id}"></div>', unsafe_allow_html=True)


def scroll_to(anchor_id: str, offset_px: int = 0) -> None:
    st_html(
        f"""
        <script>
        (function() {{
          const anchorId = "{anchor_id}";
          const offsetPx = {offset_px};
          const maxAttempts = 20;
          const delayMs = 50;
          const correctionDelayMs = 500;
          const offsetDelayMs = 200;

          function getParentDoc() {{
            try {{
              return window.parent && window.parent.document ? window.parent.document : null;
            }} catch (err) {{
              return null;
            }}
          }}

          function getTopDoc() {{
            try {{
              return window.top && window.top.document ? window.top.document : null;
            }} catch (err) {{
              return null;
            }}
          }}

          function findAnchor() {{
            const parentDoc = getParentDoc();
            if (parentDoc) {{
              const parentEl = parentDoc.getElementById(anchorId);
              if (parentEl) {{
                return {{ element: parentEl, scrollWindow: window.parent || window }};
              }}
            }}

            const topDoc = getTopDoc();
            if (topDoc) {{
              const topEl = topDoc.getElementById(anchorId);
              if (topEl) {{
                return {{ element: topEl, scrollWindow: window.top || window }};
              }}
            }}

            const selfEl = document.getElementById(anchorId);
            if (selfEl) {{
              return {{ element: selfEl, scrollWindow: window }};
            }}
            return null;
          }}

          function getTopScrollWindow() {{
            try {{
              if (window.top && window.top !== window) {{
                return window.top;
              }}
            }} catch (err) {{
              // ignore
            }}
            try {{
              if (window.parent && window.parent !== window) {{
                return window.parent;
              }}
            }} catch (err) {{
              // ignore
            }}
            return window;
          }}

          function applyOffset(scrollWindow) {{
            if (!offsetPx) return;
            setTimeout(() => {{
              try {{
                scrollWindow.scrollBy(0, offsetPx);
              }} catch (err) {{
                window.scrollBy(0, offsetPx);
              }}
            }}, offsetDelayMs);
          }}

          function applyFinalCorrection(target, scrollWindow) {{
            if (!target) return;
            setTimeout(() => {{
              try {{
                const rect = target.getBoundingClientRect();
                if (rect.top < -8) {{
                  const adjust = rect.top + 8;
                  if (adjust < 0) {{
                    scrollWindow.scrollBy(0, adjust);
                  }}
                }}
              }} catch (err) {{
                // ignore
              }}
            }}, correctionDelayMs);
          }}

          function attemptScroll(attempt) {{
            if (anchorId === "page_top") {{
              const scrollWindow = getTopScrollWindow();
              scrollWindow.scrollTo({{ top: 0, behavior: "smooth" }});
              return;
            }}
            const found = findAnchor();
            if (found && found.element) {{
              const scrollWindow = found.scrollWindow || found.element.ownerDocument?.defaultView || window;
              found.element.scrollIntoView({{ behavior: "smooth", block: "start" }});
              applyOffset(scrollWindow);
              applyFinalCorrection(found.element, scrollWindow);
              return;
            }}
            if (attempt >= maxAttempts) return;
            setTimeout(() => attemptScroll(attempt + 1), delayMs);
          }}

          attemptScroll(0);
        }})();
        </script>
        """,
        height=0,
    )


def request_scroll(anchor_id: str, offset_px: int = -12) -> None:
    clamped_offset = min(0, max(-40, int(offset_px)))
    st.session_state["_pending_scroll"] = {"id": anchor_id, "offset": clamped_offset}


def consume_pending_scroll() -> None:
    payload = st.session_state.pop("_pending_scroll", None)
    if not payload:
        return
    anchor_id = payload.get("id") if isinstance(payload, dict) else None
    if not anchor_id:
        return
    scroll_to(anchor_id, payload.get("offset", 0))


def _normalize_uuid_param(raw_value) -> str | None:
    if isinstance(raw_value, list):
        raw_value = raw_value[0] if raw_value else None
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        return str(UUID(value))
    except Exception:
        return None


def resolve_tracked_playlist_id() -> str | None:
    params = _flatten_query_params(_get_query_params())
    raw_param = params.get("tp")
    normalized_param = _normalize_uuid_param(raw_param)
    session_value = _normalize_uuid_param(st.session_state.get("selected_tracked_playlist_id"))
    if raw_param is not None:
        if normalized_param:
            if session_value != normalized_param:
                st.session_state["selected_tracked_playlist_id"] = normalized_param
            return normalized_param
        return None
    return session_value


def _basic_state_key(tracked_playlist_id: str, suffix: str) -> str:
    return f"basic_{tracked_playlist_id}_{suffix}"


def _latest_results_state_key(tracked_playlist_id: str) -> str:
    return f"latest_results_{tracked_playlist_id}"


def _latest_results_visibility_state_key(tracked_playlist_id: str) -> str:
    return f"show_latest_results_{tracked_playlist_id}"


def _database_smoke_test():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if not _is_production_environment():
            logger.info("Database connection test skipped: DATABASE_URL not set.")
        return

    try:
        with psycopg2.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        if not _is_production_environment():
            logger.info("DB OK")
    except Exception as exc:
        if not _is_production_environment():
            logger.error("Database connection test failed: %s", exc)


def _initialize_database_schema():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if not _is_production_environment():
            logger.info("Database init skipped: DATABASE_URL not set.")
        return

    statements = [
        """
        CREATE TABLE IF NOT EXISTS playlists (
            id UUID PRIMARY KEY,
            spotify_playlist_id TEXT NOT NULL UNIQUE,
            playlist_url TEXT,
            display_name TEXT,
            playlist_image_url TEXT,
            playlist_owner TEXT,
            playlist_followers INTEGER,
            songs_count INTEGER,
            playlist_last_track_added_at TEXT,
            playlist_description TEXT,
            metadata_refreshed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tracked_playlists (
            id UUID PRIMARY KEY,
            account_id UUID,
            playlist_id UUID NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
            display_name TEXT,
            target_countries TEXT[] NOT NULL DEFAULT '{}'::text[],
            target_keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (account_id, playlist_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS scans (
            id UUID PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            countries TEXT[] NOT NULL DEFAULT '{}',
            keywords TEXT[] NOT NULL DEFAULT '{}',
            playlist_url TEXT,
            total_requests INTEGER,
            tracked_playlist_id UUID REFERENCES tracked_playlists(id)
        );
        """,
        """
        ALTER TABLE scans
        ADD COLUMN IF NOT EXISTS tracked_playlist_id UUID REFERENCES tracked_playlists(id);
        """,
        """
        CREATE TABLE IF NOT EXISTS scan_results (
            id UUID PRIMARY KEY,
            scan_id UUID NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
            searched_at TEXT,
            keyword TEXT NOT NULL,
            country TEXT NOT NULL,
            rank INTEGER,
            playlist_id TEXT,
            playlist_name TEXT,
            playlist_owner TEXT,
            playlist_followers INTEGER,
            songs_count INTEGER,
            playlist_last_track_added_at TEXT,
            playlist_description TEXT,
            playlist_url TEXT,
            is_your_playlist BOOLEAN,
            snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS display_name TEXT;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS playlist_image_url TEXT;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS playlist_owner TEXT;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS playlist_followers INTEGER;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS songs_count INTEGER;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS playlist_last_track_added_at TEXT;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS playlist_description TEXT;",
        "ALTER TABLE playlists ADD COLUMN IF NOT EXISTS metadata_refreshed_at TIMESTAMPTZ;",
        "ALTER TABLE tracked_playlists ADD COLUMN IF NOT EXISTS display_name TEXT;",
        "ALTER TABLE tracked_playlists ADD COLUMN IF NOT EXISTS target_countries TEXT[] NOT NULL DEFAULT '{}'::text[];",
        "UPDATE tracked_playlists SET target_countries = '{}'::text[] WHERE target_countries IS NULL;",
        "ALTER TABLE tracked_playlists ADD COLUMN IF NOT EXISTS target_keywords TEXT[] NOT NULL DEFAULT '{}'::text[];",
        "UPDATE tracked_playlists SET target_keywords = '{}'::text[] WHERE target_keywords IS NULL;",
        "ALTER TABLE scans ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS searched_at TEXT;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS playlist_followers INTEGER;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS songs_count INTEGER;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS playlist_last_track_added_at TEXT;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS playlist_description TEXT;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS playlist_url TEXT;",
        "ALTER TABLE scan_results ADD COLUMN IF NOT EXISTS is_your_playlist BOOLEAN;",
        "CREATE INDEX IF NOT EXISTS idx_scan_results_scan_id ON scan_results (scan_id);",
        "CREATE INDEX IF NOT EXISTS idx_scan_results_keyword_country ON scan_results (keyword, country);",
        "CREATE INDEX IF NOT EXISTS idx_scans_created_at ON scans (created_at);",
        """
        DO $$
        BEGIN
            IF to_regclass('public.scans') IS NOT NULL THEN
                IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'scans_tracked_playlist_id_fkey') THEN
                    ALTER TABLE scans DROP CONSTRAINT scans_tracked_playlist_id_fkey;
                END IF;
                ALTER TABLE scans
                    ADD CONSTRAINT scans_tracked_playlist_id_fkey
                    FOREIGN KEY (tracked_playlist_id) REFERENCES tracked_playlists(id) ON DELETE CASCADE;
            END IF;
        EXCEPTION WHEN undefined_table THEN
            NULL;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF to_regclass('public.scheduled_scans') IS NOT NULL THEN
                IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'scheduled_scans_tracked_playlist_id_fkey') THEN
                    ALTER TABLE scheduled_scans DROP CONSTRAINT scheduled_scans_tracked_playlist_id_fkey;
                END IF;
                ALTER TABLE scheduled_scans
                    ADD CONSTRAINT scheduled_scans_tracked_playlist_id_fkey
                    FOREIGN KEY (tracked_playlist_id) REFERENCES tracked_playlists(id) ON DELETE CASCADE;
            END IF;
        EXCEPTION WHEN undefined_table THEN
            NULL;
        END $$;
        """,
    ]

    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
            conn.commit()
        if not _is_production_environment():
            logger.info("Database init completed.")
    except Exception as exc:
        logger.error("Database init failed: %s", exc)


def start_database_schema_setup():
    global _db_schema_setup_started
    with _db_schema_setup_lock:
        if _db_schema_setup_started:
            return
        _db_schema_setup_started = True

    thread = threading.Thread(target=_initialize_database_schema, name="db-schema-setup", daemon=True)
    thread.start()


def start_database_smoke_test():
    global _db_smoke_test_started
    with _db_smoke_test_lock:
        if _db_smoke_test_started:
            return
        _db_smoke_test_started = True

    thread = threading.Thread(target=_database_smoke_test, name="db-smoke-test", daemon=True)
    thread.start()


start_database_smoke_test()
start_database_schema_setup()


def fetch_scan_results_for_export(scan_id: str) -> pd.DataFrame | None:
    scan_id = (scan_id or "").strip()
    if not scan_id:
        return pd.DataFrame(columns=CSV_EXPORT_COLUMNS)

    try:
        UUID(scan_id)
    except Exception:
        return pd.DataFrame(columns=CSV_EXPORT_COLUMNS)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if not _is_production_environment():
            logger.info("History export skipped: DATABASE_URL not set.")
        return None

    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        searched_at,
                        keyword,
                        country,
                        rank,
                        playlist_name,
                        playlist_owner,
                        playlist_followers,
                        songs_count,
                        playlist_last_track_added_at,
                        playlist_description,
                        playlist_url,
                        is_your_playlist
                    FROM scan_results
                    WHERE scan_id = %s
                    ORDER BY searched_at, keyword, country, rank;
                    """,
                    (scan_id,),
                )
                rows = cur.fetchall()
        return pd.DataFrame(rows, columns=CSV_EXPORT_COLUMNS)
    except Exception as exc:
        logger.error("History export failed: %s", exc)
        return None


def handle_history_export_if_requested():
    params = _get_query_params()
    raw_scan_id = params.get("export_scan_id") if isinstance(params, dict) else None
    if isinstance(raw_scan_id, list):
        raw_scan_id = raw_scan_id[0] if raw_scan_id else None
    export_scan_id = (raw_scan_id or "").strip()
    if not export_scan_id:
        return

    df = fetch_scan_results_for_export(export_scan_id)
    if df is None:
        st.error("History CSV export failed or is disabled.")
        st.stop()

    csv_data = df.to_csv(index=False, columns=CSV_EXPORT_COLUMNS)
    file_name = f"scan_{export_scan_id}_results.csv"
    st.info(f"History export ready for scan_id={export_scan_id}")
    st.download_button(
        "Download History CSV",
        data=csv_data.encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
    )
    st.stop()


def persist_scan(
    playlist_url: str,
    keywords: list[str],
    countries: list[str],
    results: list[dict],
    searched_at: str | None,
    tracked_playlist_id: str | None = None,
):
    playlist_url = (playlist_url or "").strip()
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_url or not playlist_id:
        logger.info("DB persist: skipped (no/invalid playlist_url)")
        return None

    tracked_playlist_id = (tracked_playlist_id or "").strip() or None
    validated_tracked_playlist_id = None
    if tracked_playlist_id:
        try:
            validated_tracked_playlist_id = str(UUID(tracked_playlist_id))
        except Exception:
            validated_tracked_playlist_id = None

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if not _is_production_environment():
            logger.info("DB persist: skipped (no DATABASE_URL)")
        return None

    try:
        conn = psycopg2.connect(database_url, connect_timeout=10)
    except Exception as exc:
        logger.error("DB persist error: %s", exc)
        return None

    try:
        total_requests = len(keywords or []) * len(countries or [])
        scan_id = uuid4()
        scan_id_str = str(scan_id)
        finished_at = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO playlists (id, spotify_playlist_id, playlist_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (spotify_playlist_id) DO UPDATE
                SET playlist_url = EXCLUDED.playlist_url
                RETURNING id;
                """,
                (str(uuid4()), playlist_id, playlist_url),
            )
            playlist_row_id = cur.fetchone()[0]
            tracked_playlist_id_to_use = validated_tracked_playlist_id
            if tracked_playlist_id_to_use:
                cur.execute(
                    """
                    SELECT playlist_id FROM tracked_playlists
                    WHERE id = %s
                    LIMIT 1;
                    """,
                    (tracked_playlist_id_to_use,),
                )
                tracked_playlist_row = cur.fetchone()
                if tracked_playlist_row:
                    existing_playlist_row_id = tracked_playlist_row[0]
                    if existing_playlist_row_id != playlist_row_id:
                        cur.execute(
                            """
                            UPDATE tracked_playlists
                            SET playlist_id = %s
                            WHERE id = %s;
                            """,
                            (playlist_row_id, tracked_playlist_id_to_use),
                        )
                else:
                    tracked_playlist_id_to_use = None

            if not tracked_playlist_id_to_use:
                cur.execute(
                    """
                    SELECT id FROM tracked_playlists
                    WHERE account_id IS NULL AND playlist_id = %s
                    LIMIT 1;
                    """,
                    (playlist_row_id,),
                )
                existing_tracked_playlist_row = cur.fetchone()
                if existing_tracked_playlist_row:
                    tracked_playlist_id_to_use = existing_tracked_playlist_row[0]
                else:
                    cur.execute(
                        """
                        INSERT INTO tracked_playlists (id, account_id, playlist_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT ON CONSTRAINT tracked_playlists_account_id_playlist_id_key DO UPDATE
                        SET account_id = EXCLUDED.account_id
                        RETURNING id;
                        """,
                        (validated_tracked_playlist_id or str(uuid4()), None, playlist_row_id),
                    )
                    tracked_playlist_id_to_use = cur.fetchone()[0]

            if not tracked_playlist_id_to_use:
                tracked_playlist_id_to_use = None

            cur.execute(
                """
                INSERT INTO scans (id, countries, keywords, playlist_url, total_requests, tracked_playlist_id, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    scan_id_str,
                    countries or [],
                    keywords or [],
                    playlist_url,
                    total_requests,
                    tracked_playlist_id_to_use,
                    finished_at,
                ),
            )
            if results:
                cur.executemany(
                    """
                    INSERT INTO scan_results (
                        id,
                        scan_id,
                        searched_at,
                        keyword,
                        country,
                        rank,
                        playlist_id,
                        playlist_name,
                        playlist_owner,
                        playlist_followers,
                        songs_count,
                        playlist_last_track_added_at,
                        playlist_description,
                        playlist_url,
                        is_your_playlist
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            str(uuid4()),
                            scan_id_str,
                            result.get("searched_at") or searched_at,
                            result.get("keyword"),
                            result.get("country"),
                            result.get("rank"),
                            result.get("playlist_id"),
                            result.get("playlist_name"),
                            result.get("playlist_owner"),
                            result.get("playlist_followers"),
                            result.get("songs_count"),
                            result.get("playlist_last_track_added_at"),
                            result.get("playlist_description"),
                            result.get("playlist_url"),
                            result.get("is_your_playlist"),
                        )
                        for result in results
                    ],
                )
        conn.commit()
        logger.info("DB persist: inserted scan=%s results=%s", scan_id_str, len(results or []))
        return scan_id
    except Exception as exc:
        logger.error("DB persist error: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


@st.cache_resource(show_spinner=False)
def get_db_connection_status() -> bool:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if not _is_production_environment():
            logger.info("Sidebar DB check skipped: DATABASE_URL not set.")
        return False

    try:
        with psycopg2.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except Exception as exc:
        if not _is_production_environment():
            logger.warning("Sidebar DB check failed: %s", exc)
        return False

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

TOKEN_URL = "https://accounts.spotify.com/api/token"
SEARCH_URL = "https://api.spotify.com/v1/search"
PLAYLIST_URL = "https://api.spotify.com/v1/playlists/{}"
MARKETS_URL = "https://api.spotify.com/v1/markets"
RESULTS_LIMIT = 20


def extract_playlist_id(text: str):
    text = (text or "").strip()
    m = re.search(r"(?:open\.spotify\.com/playlist/|spotify:playlist:)([A-Za-z0-9]+)", text)
    return m.group(1) if m else None


def normalize_spotify_playlist_url(url: str) -> str:
    return (url or "").split("?", 1)[0].strip()


def get_access_token():
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise ValueError("SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET missing from .env.")

    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    headers = {"Authorization": f"Basic {b64_auth}"}
    data = {"grant_type": "client_credentials"}

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]


def spotify_get(url: str, token: str, params=None):
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "2"))
            time.sleep(retry_after)
            continue
        r.raise_for_status()
        return r.json() or {}


@st.cache_data(ttl=24 * 60 * 60)
def get_spotify_markets():
    token = get_access_token()
    data = spotify_get(MARKETS_URL, token)
    markets = data.get("markets") or []
    return [m for m in markets if isinstance(m, str)]


def get_country_selection_options(show_warning: bool = True) -> tuple[dict, list[str]]:
    country_map = COUNTRIES
    country_names = COUNTRY_NAMES
    try:
        markets = get_spotify_markets()
        filtered = {name: code for name, code in COUNTRIES.items() if code in markets}
        if filtered:
            country_map = filtered
            country_names = sorted(filtered.keys())
        elif show_warning:
            st.warning("Spotify markets response was empty or unmatched; showing the configured country list.")
    except Exception:
        if show_warning:
            st.warning("Spotify markets could not be loaded; showing the configured country list.")
    return country_map, country_names


def search_playlists(keyword: str, market: str, token: str, limit: int = 50, offset: int = 0):
    params = {
        "q": keyword,
        "type": "playlist",
        "market": market,
        "limit": limit,
        "offset": offset,
    }
    data = spotify_get(SEARCH_URL, token, params=params)
    items = ((data.get("playlists") or {}).get("items") or [])
    items = [x for x in items if isinstance(x, dict)]
    return items


def search_playlists_with_pagination(keyword: str, market: str, token: str, target_count: int = RESULTS_LIMIT):
    offsets = [0, 50, 100]
    collected = []
    seen_ids = set()

    for offset in offsets:
        items = search_playlists(keyword, market, token, limit=50, offset=offset)
        for item in items:
            pid = item.get("id")
            if pid and pid in seen_ids:
                continue

            if pid:
                seen_ids.add(pid)
            collected.append(item)

            if len(collected) >= target_count:
                break

        if len(collected) >= target_count:
            break

    actual_count = len(collected)

    if actual_count < target_count:
        placeholder_count = target_count - actual_count
        for _ in range(placeholder_count):
            collected.append(
                {
                    "id": None,
                    "name": "N/A",
                    "external_urls": {"spotify": ""},
                    "description": "",
                    "placeholder": True,
                }
            )

    return collected, actual_count


def ensure_utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_last_updated_display(last_track_added_at: str, now_dt: datetime) -> str:
    if not last_track_added_at:
        return "N/A"

    cleaned = last_track_added_at.replace("Z", "+00:00")
    try:
        last_added_dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return "Unknown"

    last_added_dt = ensure_utc_aware(last_added_dt)
    now_dt = ensure_utc_aware(now_dt)
    if not last_added_dt or not now_dt:
        return "Unknown"

    delta = now_dt - last_added_dt
    total_hours = int(delta.total_seconds() // 3600)
    if total_hours < 24:
        hours = max(total_hours, 0)
        return f"{hours} hours ago"

    days = delta.days
    if days < 30:
        return f"{max(days, 1)} days ago"

    months = max(days // 30, 1)
    return f"{months} months ago"


def get_latest_track_added_at(playlist_id: str, snapshot_id: str) -> str | None:
    if not playlist_id or not snapshot_id:
        return None

    token = st.session_state.get("access_token") or get_access_token()
    latest_dt: datetime | None = None
    limit = 50
    offset = 0

    while True:
        data = spotify_get(
            f"{PLAYLIST_URL.format(playlist_id)}/tracks",
            token,
            params={
                "fields": "items(added_at),total",
                "limit": limit,
                "offset": offset,
            },
        )

        items = data.get("items") or []
        total = data.get("total")
        total = total if isinstance(total, int) else offset + len(items)

        for item in items:
            added_at = item.get("added_at")
            if not added_at:
                continue
            try:
                added_dt = datetime.fromisoformat(added_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if added_dt.tzinfo is None:
                added_dt = added_dt.replace(tzinfo=timezone.utc)

            if latest_dt is None or added_dt > latest_dt:
                latest_dt = added_dt

        offset += limit
        if offset >= total or not items:
            break

    if latest_dt is None:
        return None

    return (
        latest_dt.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def fetch_playlist_details(playlist_ids, token: str, cache: dict):
    unique_ids = [pid for pid in dict.fromkeys(playlist_ids) if pid and pid not in cache]
    if not unique_ids:
        return

    def fetch_one(pid: str):
        detail = spotify_get(
            PLAYLIST_URL.format(pid),
            token,
            params={
                "fields": "name,external_urls.spotify,followers.total,tracks.total,description,images,snapshot_id,owner.display_name,owner.id",
            },
        )
        followers = (detail.get("followers") or {}).get("total")
        tracks_total = (detail.get("tracks") or {}).get("total")
        snapshot_id = detail.get("snapshot_id")
        owner_info = detail.get("owner") or {}
        playlist_owner = owner_info.get("display_name") or owner_info.get("id")
        playlist_last_track_added_at = None
        if tracks_total and snapshot_id:
            playlist_last_track_added_at = get_latest_track_added_at(pid, snapshot_id)
        images = detail.get("images") or []
        playlist_image_url = images[0].get("url") if images else ""
        cache[pid] = {
            "playlist_name": detail.get("name", "-"),
            "playlist_url": (detail.get("external_urls") or {}).get("spotify", ""),
            "playlist_description": detail.get("description", ""),
            "playlist_followers": followers,
            "songs_count": tracks_total,
            "playlist_last_track_added_at": playlist_last_track_added_at,
            "playlist_image": playlist_image_url,
            "playlist_image_url": playlist_image_url,
            "playlist_snapshot_id": snapshot_id,
            "playlist_owner": playlist_owner,
        }
        time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_pid = {executor.submit(fetch_one, pid): pid for pid in unique_ids}
        for future in as_completed(future_to_pid):
            try:
                future.result()
            except Exception:
                failed_pid = future_to_pid.get(future)
                if failed_pid:
                    cache[failed_pid] = {
                        "playlist_name": "-",
                        "playlist_url": "",
                        "playlist_description": "",
                        "playlist_followers": None,
                        "songs_count": None,
                        "playlist_last_track_added_at": None,
                        "playlist_image": "",
                        "playlist_owner": None,
                    }


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_relative_hours_or_days(value: datetime | str | None) -> str:
    if isinstance(value, str):
        value = _parse_iso_datetime(value)
    if value is None:
        return "N/A"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = now - value
    hours = int(delta.total_seconds() // 3600)
    if hours < 24:
        return f"{max(hours, 0)} hours ago"
    days = max(delta.days, 1)
    return f"{days} days ago"


def format_relative_time_or_na(value: datetime | str | None) -> str:
    if isinstance(value, str):
        value = _parse_iso_datetime(value)
    if value is None:
        return "NA"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = now - value
    hours = int(delta.total_seconds() // 3600)
    if hours < 24:
        return f"{max(hours, 0)} hours ago"
    days = max(delta.days, 1)
    return f"{days} days ago"


def format_relative_time_or_dash(value: datetime | str | None) -> str:
    display = format_relative_time_or_na(value)
    if display.upper() in {"NA", "N/A"}:
        return "—"
    return display


def format_stat_value(value):
    if value is None:
        return "NA"
    if isinstance(value, (int, float)):
        return f"{value:,}"
    return str(value)


def truncate_title(text: str, max_length: int = 64) -> str:
    text = (text or "").strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "…"


def _normalize_target_countries(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        countries = []
        for item in value:
            if isinstance(item, str):
                name = item.strip()
                if name and name not in countries:
                    countries.append(name)
        return countries
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _normalize_target_keywords(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        keywords = []
        for item in value:
            if isinstance(item, str):
                keyword = item.strip()
                if keyword and keyword not in keywords:
                    keywords.append(keyword)
        return keywords
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _resolve_latest_playlist_stats(
    *,
    playlist_owner,
    playlist_followers,
    songs_count,
    playlist_last_track_added_at,
    metadata_refreshed_at,
    scan_owner=None,
    scan_followers=None,
    scan_songs_count=None,
    scan_last_track_added_at=None,
    scan_snapshot_at=None,
    scan_completed_at=None,
) -> dict:
    scan_time = scan_completed_at or scan_snapshot_at
    playlist_time = metadata_refreshed_at
    use_scan = bool(scan_time and (playlist_time is None or scan_time >= playlist_time))
    if use_scan:
        resolved_owner = scan_owner or playlist_owner
        resolved_followers = scan_followers if scan_followers is not None else playlist_followers
        resolved_songs_count = scan_songs_count if scan_songs_count is not None else songs_count
        resolved_last_track_added_at = scan_last_track_added_at or playlist_last_track_added_at
        resolved_scanned_at = scan_time
    else:
        resolved_owner = playlist_owner
        resolved_followers = playlist_followers
        resolved_songs_count = songs_count
        resolved_last_track_added_at = playlist_last_track_added_at
        resolved_scanned_at = playlist_time
    return {
        "owner": resolved_owner,
        "followers": resolved_followers,
        "songs_count": resolved_songs_count,
        "last_track_added_at": resolved_last_track_added_at,
        "scanned_at": resolved_scanned_at,
    }


def fetch_spotify_playlist_metadata(playlist_id: str) -> dict:
    token = st.session_state.get("access_token") or get_access_token()
    meta_cache: dict[str, dict] = {}
    fetch_playlist_details([playlist_id], token, meta_cache)
    base_meta = meta_cache.get(playlist_id) or {}
    playlist_url = base_meta.get("playlist_url") or f"https://open.spotify.com/playlist/{playlist_id}"
    base_meta["playlist_url"] = playlist_url
    base_meta["playlist_image_url"] = base_meta.get("playlist_image") or base_meta.get("playlist_image_url") or ""
    return base_meta


def upsert_tracked_playlist(
    playlist_url_input: str,
    target_countries_input: list[str] | None = None,
    target_keywords_input: list[str] | None = None,
) -> tuple[bool, str]:
    playlist_url_input = normalize_spotify_playlist_url(playlist_url_input)
    playlist_id = extract_playlist_id(playlist_url_input)
    if not playlist_id and re.fullmatch(r"[A-Za-z0-9]+", playlist_url_input):
        playlist_id = playlist_url_input
    if not playlist_id:
        return False, "Invalid Spotify playlist URL."

    country_map, _ = get_country_selection_options(show_warning=False)
    if target_countries_input is not None and not isinstance(target_countries_input, (list, tuple)):
        return False, "Please select at least one target country."
    target_countries = _normalize_target_countries(target_countries_input)
    if not target_countries:
        return False, "Please select at least one target country."
    if len(target_countries) > 5:
        return False, "You can select up to 5 target countries."
    for name in target_countries:
        if name not in country_map and name not in COUNTRIES:
            return False, f"Unsupported country selected: {name}"

    if target_keywords_input is not None and not isinstance(target_keywords_input, (list, tuple)):
        return False, "Please enter at least one target keyword."
    target_keywords = _normalize_target_keywords(target_keywords_input)
    if not target_keywords:
        return False, "Please enter at least one target keyword."
    if len(target_keywords) > 5:
        return False, "You can select up to 5 target keywords."

    if not os.getenv("DATABASE_URL"):
        return False, "Database not configured."

    try:
        playlist_meta = fetch_spotify_playlist_metadata(playlist_id)
    except Exception as exc:
        logger.error("Spotify metadata fetch failed: %s", exc)
        return False, f"Spotify playlist metadata could not be retrieved: {exc}"

    playlist_url = playlist_meta.get("playlist_url") or f"https://open.spotify.com/playlist/{playlist_id}"
    display_name = playlist_meta.get("playlist_name") or "Tracked Playlist"
    playlist_owner = playlist_meta.get("playlist_owner")
    playlist_followers = playlist_meta.get("playlist_followers")
    songs_count = playlist_meta.get("songs_count")
    playlist_last_track_added_at = playlist_meta.get("playlist_last_track_added_at")
    playlist_description = playlist_meta.get("playlist_description")
    playlist_image_url = playlist_meta.get("playlist_image_url") or ""
    metadata_refreshed_at = datetime.now(timezone.utc)

    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)
    except Exception as exc:
        logger.error("DB connection failed for upsert: %s", exc)
        return False, "Database connection failed."

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO playlists (
                        id,
                        spotify_playlist_id,
                        playlist_url,
                        display_name,
                        playlist_image_url,
                        playlist_owner,
                        playlist_followers,
                        songs_count,
                        playlist_last_track_added_at,
                        playlist_description,
                        metadata_refreshed_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (spotify_playlist_id) DO UPDATE
                    SET
                        playlist_url = EXCLUDED.playlist_url,
                        display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), playlists.display_name),
                        playlist_image_url = COALESCE(NULLIF(EXCLUDED.playlist_image_url, ''), playlists.playlist_image_url),
                        playlist_owner = EXCLUDED.playlist_owner,
                        playlist_followers = EXCLUDED.playlist_followers,
                        songs_count = EXCLUDED.songs_count,
                        playlist_last_track_added_at = EXCLUDED.playlist_last_track_added_at,
                        playlist_description = EXCLUDED.playlist_description,
                        metadata_refreshed_at = EXCLUDED.metadata_refreshed_at
                    RETURNING id;
                    """,
                    (
                        str(uuid4()),
                        playlist_id,
                        playlist_url,
                        display_name,
                        playlist_image_url,
                        playlist_owner,
                        playlist_followers,
                        songs_count,
                        playlist_last_track_added_at,
                        playlist_description,
                        metadata_refreshed_at,
                    ),
                )
                playlist_row_id = cur.fetchone()[0]
                cur.execute(
                    """
                    SELECT 1
                    FROM tracked_playlists tp
                    WHERE tp.account_id IS NULL
                      AND tp.playlist_id = %s
                    LIMIT 1;
                    """,
                    (playlist_row_id,),
                )
                if cur.fetchone():
                    return False, "Playlist already exists for this account. Try with a different Playlist URL."
                cur.execute(
                    """
                    INSERT INTO tracked_playlists (
                        id,
                        account_id,
                        playlist_id,
                        display_name,
                        target_countries,
                        target_keywords
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, playlist_id) DO UPDATE
                    SET display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), tracked_playlists.display_name),
                        target_countries = EXCLUDED.target_countries,
                        target_keywords = EXCLUDED.target_keywords
                    RETURNING id;
                    """,
                    (str(uuid4()), None, playlist_row_id, display_name, target_countries, target_keywords),
                )
                tracked_playlist_id = cur.fetchone()[0]
        logger.info("Tracked playlist saved: %s", tracked_playlist_id)
        return True, "Playlist saved."
    except Exception as exc:
        logger.exception("Upsert tracked playlist failed")
        try:
            conn.rollback()
        except Exception:
            pass
        return False, "Playlist could not be saved. Please try again later."
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_tracked_playlist_target_countries(
    tracked_playlist_id: str,
    target_countries_input: list[str] | None,
) -> tuple[bool, str]:
    tracked_playlist_id = (tracked_playlist_id or "").strip()
    if not tracked_playlist_id:
        return False, "Tracked playlist missing."
    if target_countries_input is not None and not isinstance(target_countries_input, (list, tuple)):
        return False, "Please select at least one target country."
    target_countries = _normalize_target_countries(target_countries_input)
    if not target_countries:
        return False, "Please select at least one target country."
    if len(target_countries) > 5:
        return False, "You can select up to 5 target countries."

    country_map, _ = get_country_selection_options(show_warning=False)
    for name in target_countries:
        if name not in country_map and name not in COUNTRIES:
            return False, f"Unsupported country selected: {name}"

    if not os.getenv("DATABASE_URL"):
        return False, "Database not configured."

    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)
    except Exception as exc:
        logger.error("DB connection failed for target country update: %s", exc)
        return False, "Database connection failed."

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tracked_playlists SET target_countries = %s WHERE id = %s;",
                    (target_countries, tracked_playlist_id),
                )
                if cur.rowcount == 0:
                    return False, "Tracked playlist not found."
    except Exception as exc:
        logger.error("Failed updating tracked playlist targets: %s", exc)
        return False, "Target countries could not be updated."
    finally:
        conn.close()

    return True, "Target countries updated."


def update_tracked_playlist_target_keywords(
    tracked_playlist_id: str,
    target_keywords_input: list[str] | None,
) -> tuple[bool, str]:
    tracked_playlist_id = (tracked_playlist_id or "").strip()
    if not tracked_playlist_id:
        return False, "Tracked playlist missing."
    if target_keywords_input is not None and not isinstance(target_keywords_input, (list, tuple)):
        return False, "Please enter at least one target keyword."
    target_keywords = _normalize_target_keywords(target_keywords_input)
    if not target_keywords:
        return False, "Please enter at least one target keyword."
    if len(target_keywords) > 5:
        return False, "You can select up to 5 target keywords."

    if not os.getenv("DATABASE_URL"):
        return False, "Database not configured."

    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)
    except Exception as exc:
        logger.error("DB connection failed for target keyword update: %s", exc)
        return False, "Database connection failed."

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(target_keywords, '{}'::text[]) FROM tracked_playlists WHERE id = %s;",
                    (tracked_playlist_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False, "Tracked playlist not found."
                existing_keywords = _normalize_target_keywords(row[0] or [])
                missing_keywords = [kw for kw in existing_keywords if kw not in target_keywords]
                if missing_keywords:
                    return False, "Existing target keywords cannot be removed. Please contact support."
                cur.execute(
                    "UPDATE tracked_playlists SET target_keywords = %s WHERE id = %s;",
                    (target_keywords, tracked_playlist_id),
                )
                if cur.rowcount == 0:
                    return False, "Tracked playlist not found."
    except Exception as exc:
        logger.error("Failed updating tracked playlist keywords: %s", exc)
        return False, "Target keywords could not be updated."
    finally:
        conn.close()

    return True, "Target keywords updated."


def refresh_tracked_playlist_metadata(tracked_playlist_id: str) -> tuple[bool, str, dict | None]:
    tracked_playlist_id = (tracked_playlist_id or "").strip()
    if not tracked_playlist_id:
        return False, "Tracked playlist missing.", None
    try:
        UUID(tracked_playlist_id)
    except Exception:
        return False, "Tracked playlist is invalid.", None

    if not os.getenv("DATABASE_URL"):
        return False, "Database not configured.", None

    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=10)
    except Exception as exc:
        logger.error("DB connection failed for refresh: %s", exc)
        return False, "Database connection failed.", None

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.id, p.spotify_playlist_id
                    FROM tracked_playlists tp
                    JOIN playlists p ON tp.playlist_id = p.id
                    WHERE tp.id = %s
                    LIMIT 1;
                    """,
                    (tracked_playlist_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False, "Tracked playlist not found.", None
                playlist_row_id, spotify_playlist_id = row
        playlist_meta = fetch_spotify_playlist_metadata(spotify_playlist_id)
        playlist_url = playlist_meta.get("playlist_url") or f"https://open.spotify.com/playlist/{spotify_playlist_id}"
        display_name = playlist_meta.get("playlist_name") or "Tracked Playlist"
        playlist_owner = playlist_meta.get("playlist_owner")
        playlist_followers = playlist_meta.get("playlist_followers")
        songs_count = playlist_meta.get("songs_count")
        playlist_last_track_added_at = playlist_meta.get("playlist_last_track_added_at")
        playlist_description = playlist_meta.get("playlist_description")
        playlist_image_url = playlist_meta.get("playlist_image_url") or ""
        metadata_refreshed_at = datetime.now(timezone.utc)

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE playlists
                    SET
                        playlist_url = %s,
                        display_name = %s,
                        playlist_image_url = %s,
                        playlist_owner = %s,
                        playlist_followers = %s,
                        songs_count = %s,
                        playlist_last_track_added_at = %s,
                        playlist_description = %s,
                        metadata_refreshed_at = %s
                    WHERE id = %s;
                    """,
                    (
                        playlist_url,
                        display_name,
                        playlist_image_url,
                        playlist_owner,
                        playlist_followers,
                        songs_count,
                        playlist_last_track_added_at,
                        playlist_description,
                        metadata_refreshed_at,
                        playlist_row_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE tracked_playlists
                    SET display_name = COALESCE(NULLIF(%s, ''), tracked_playlists.display_name)
                    WHERE id = %s;
                    """,
                    (display_name, tracked_playlist_id),
                )
        refreshed_detail = fetch_tracked_playlist_detail(tracked_playlist_id)
        return True, "Playlist stats refreshed.", refreshed_detail
    except Exception as exc:
        logger.error("Refresh tracked playlist metadata failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False, "Playlist stats could not be refreshed. Please try again.", None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_tracked_playlists_query() -> list[tuple]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return []

    with psycopg2.connect(database_url, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tp.id,
                    tp.display_name AS tracked_display_name,
                    p.spotify_playlist_id,
                    p.playlist_url,
                    p.display_name AS playlist_display_name,
                    p.playlist_image_url,
                    p.playlist_owner,
                    p.playlist_followers,
                    p.songs_count AS playlist_songs_count,
                    p.playlist_last_track_added_at,
                    p.metadata_refreshed_at,
                    COALESCE(tp.target_countries, '{}'::text[]) AS target_countries,
                    COALESCE(tp.target_keywords, '{}'::text[]) AS target_keywords,
                    COALESCE(saved_scans.count, 0) AS saved_scans_count,
                    latest.owner,
                    latest.followers,
                    latest.songs_count,
                    latest.last_track_added_at,
                    latest.snapshot_at,
                    latest.scan_completed_at
                FROM tracked_playlists tp
                JOIN playlists p ON tp.playlist_id = p.id
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) FROM scans s WHERE s.tracked_playlist_id = tp.id
                ) AS saved_scans(count) ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        sr.playlist_owner AS owner,
                        sr.playlist_followers AS followers,
                        sr.songs_count AS songs_count,
                        sr.playlist_last_track_added_at AS last_track_added_at,
                        sr.snapshot_at AS snapshot_at,
                        COALESCE(s.finished_at, s.created_at) AS scan_completed_at
                    FROM scans s
                    JOIN scan_results sr ON sr.scan_id = s.id
                    WHERE s.tracked_playlist_id = tp.id
                      AND sr.playlist_id = p.spotify_playlist_id
                    ORDER BY COALESCE(s.finished_at, s.created_at) DESC, sr.snapshot_at DESC
                    LIMIT 1
                ) AS latest ON TRUE
                ORDER BY tp.created_at DESC;
                """
            )
            return cur.fetchall()


def fetch_tracked_playlists_from_db(refresh_token: int) -> list[dict]:
    _ = refresh_token
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return []

    debug_enabled = _is_debug_ui_enabled()
    debug_mode = _is_truthy(os.getenv("DEBUG_MODE"))
    if debug_enabled:
        logger.info("DEBUG_UI: fetch_tracked_playlists_from_db -> running tracked playlists SQL query")

    start_time = time.perf_counter()
    try:
        rows = _fetch_tracked_playlists_query()
    except Exception as exc:
        if isinstance(exc, OperationalError):
            logger.warning("Fetching tracked playlists failed (will retry once): %s", exc)
            try:
                rows = _fetch_tracked_playlists_query()
            except Exception as retry_exc:
                logger.error("Fetching tracked playlists retry failed: %s", retry_exc)
                return []
        else:
            logger.error("Fetching tracked playlists failed: %s", exc)
            return []

    duration = time.perf_counter() - start_time
    tracked_playlists: list[dict] = []
    for row in rows:
        (
            tracked_id,
            tracked_display_name,
            spotify_playlist_id,
            playlist_url,
            playlist_display_name,
            playlist_image_url,
            playlist_owner,
            playlist_followers,
            playlist_songs_count,
            playlist_last_track_added_at,
            metadata_refreshed_at,
            target_countries,
            target_keywords,
            saved_scans_count,
            owner,
            followers,
            songs_count,
            last_track_added_at,
            snapshot_at,
            scan_completed_at,
        ) = row
        resolved_stats = _resolve_latest_playlist_stats(
            playlist_owner=playlist_owner,
            playlist_followers=playlist_followers,
            songs_count=playlist_songs_count,
            playlist_last_track_added_at=playlist_last_track_added_at,
            metadata_refreshed_at=metadata_refreshed_at,
            scan_owner=owner,
            scan_followers=followers,
            scan_songs_count=songs_count,
            scan_last_track_added_at=last_track_added_at,
            scan_snapshot_at=snapshot_at,
            scan_completed_at=scan_completed_at,
        )
        normalized_target_countries = _normalize_target_countries(target_countries or [])
        normalized_target_keywords = _normalize_target_keywords(target_keywords or [])
        tracked_playlists.append(
            {
                "id": tracked_id,
                "display_name": tracked_display_name or playlist_display_name or "Tracked Playlist",
                "spotify_playlist_id": spotify_playlist_id,
                "playlist_url": playlist_url,
                "saved_scans_count": saved_scans_count or 0,
                "owner": resolved_stats.get("owner"),
                "followers": resolved_stats.get("followers"),
                "songs_count": resolved_stats.get("songs_count"),
                "last_track_added_at": resolved_stats.get("last_track_added_at"),
                "snapshot_at": snapshot_at,
                "scanned_at": resolved_stats.get("scanned_at"),
                "playlist_image_url": playlist_image_url or "",
                "playlist_display_name": playlist_display_name or "",
                "target_countries": normalized_target_countries,
                "target_keywords": normalized_target_keywords,
            }
        )
    if debug_enabled:
        logger.info("DEBUG_UI: fetch_tracked_playlists_from_db -> %s tracked playlists returned", len(tracked_playlists))
    if debug_mode:
        logger.info(
            "DEBUG_MODE: tracked playlists query returned %s rows in %.3f seconds",
            len(tracked_playlists),
            duration,
        )
    return tracked_playlists


def fetch_tracked_playlist_by_id(tracked_playlist_id: str) -> dict | None:
    tracked_playlist_id = (tracked_playlist_id or "").strip()
    try:
        UUID(tracked_playlist_id)
    except Exception:
        return None

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None

    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        tp.id,
                        COALESCE(tp.display_name, p.display_name, 'Tracked Playlist') AS display_name,
                        p.spotify_playlist_id,
                        p.playlist_url,
                        p.playlist_image_url,
                        p.playlist_owner,
                        p.playlist_followers,
                        p.songs_count,
                        p.playlist_last_track_added_at,
                        COALESCE(tp.target_countries, '{}'::text[]) AS target_countries,
                        COALESCE(tp.target_keywords, '{}'::text[]) AS target_keywords,
                        p.playlist_description,
                        p.metadata_refreshed_at
                    FROM tracked_playlists tp
                    JOIN playlists p ON tp.playlist_id = p.id
                    WHERE tp.id = %s
                    LIMIT 1;
                    """,
                    (tracked_playlist_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        (
            tp_id,
            display_name,
            spotify_playlist_id,
            playlist_url,
            playlist_image_url,
            playlist_owner,
            playlist_followers,
            songs_count,
            playlist_last_track_added_at,
            target_countries,
            target_keywords,
            playlist_description,
            metadata_refreshed_at,
        ) = row
        normalized_target_countries = _normalize_target_countries(target_countries or [])
        normalized_target_keywords = _normalize_target_keywords(target_keywords or [])
        return {
            "id": tp_id,
            "display_name": display_name,
            "spotify_playlist_id": spotify_playlist_id,
            "playlist_url": playlist_url,
            "playlist_image_url": playlist_image_url,
            "playlist_owner": playlist_owner,
            "playlist_followers": playlist_followers,
            "songs_count": songs_count,
            "playlist_last_track_added_at": playlist_last_track_added_at,
            "playlist_description": playlist_description,
            "metadata_refreshed_at": metadata_refreshed_at,
            "target_countries": normalized_target_countries,
            "target_keywords": normalized_target_keywords,
        }
    except Exception as exc:
        logger.error("Fetching tracked playlist detail failed: %s", exc)
        return None


def fetch_tracked_playlist_detail(tracked_playlist_id: str) -> dict | None:
    return fetch_tracked_playlist_by_id(tracked_playlist_id)


def fetch_latest_scan_for_tracked_playlist(tracked_playlist_id: str) -> dict | None:
    tracked_playlist_id = (tracked_playlist_id or "").strip()
    try:
        UUID(tracked_playlist_id)
    except Exception:
        return None

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None

    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        s.id,
                        s.created_at,
                        s.finished_at,
                        s.countries,
                        s.keywords,
                        s.total_requests,
                        sr.playlist_owner,
                        sr.playlist_followers,
                        sr.songs_count,
                        sr.playlist_last_track_added_at,
                        sr.snapshot_at
                    FROM scans s
                    JOIN tracked_playlists tp ON tp.id = s.tracked_playlist_id
                    JOIN playlists p ON p.id = tp.playlist_id
                    LEFT JOIN LATERAL (
                        SELECT
                            playlist_owner,
                            playlist_followers,
                            songs_count,
                            playlist_last_track_added_at,
                            snapshot_at
                        FROM scan_results
                        WHERE scan_id = s.id
                          AND playlist_id = p.spotify_playlist_id
                        ORDER BY snapshot_at DESC
                        LIMIT 1
                    ) AS sr ON TRUE
                    WHERE s.tracked_playlist_id = %s
                    ORDER BY s.created_at DESC
                    LIMIT 1;
                    """,
                    (tracked_playlist_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        (
            scan_id,
            created_at,
            finished_at,
            countries,
            keywords,
            total_requests,
            playlist_owner,
            playlist_followers,
            songs_count,
            playlist_last_track_added_at,
            snapshot_at,
        ) = row
        return {
            "id": scan_id,
            "created_at": created_at,
            "finished_at": finished_at,
            "countries": countries or [],
            "keywords": keywords or [],
            "total_requests": total_requests,
            "owner": playlist_owner,
            "followers": playlist_followers,
            "songs_count": songs_count,
            "last_track_added_at": playlist_last_track_added_at,
            "snapshot_at": snapshot_at,
        }
    except Exception as exc:
        logger.error("Fetching latest scan failed: %s", exc)
        return None


def fetch_scan_results_for_scan(scan_id: str) -> list[dict] | None:
    scan_id = (scan_id or "").strip()
    try:
        UUID(scan_id)
    except Exception:
        return []

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None

    columns = [
        "searched_at",
        "keyword",
        "country",
        "rank",
        "playlist_id",
        "playlist_name",
        "playlist_owner",
        "playlist_followers",
        "songs_count",
        "playlist_last_track_added_at",
        "playlist_description",
        "playlist_url",
        "is_your_playlist",
    ]

    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        searched_at,
                        keyword,
                        country,
                        rank,
                        playlist_id,
                        playlist_name,
                        playlist_owner,
                        playlist_followers,
                        songs_count,
                        playlist_last_track_added_at,
                        playlist_description,
                        playlist_url,
                        is_your_playlist
                    FROM scan_results
                    WHERE scan_id = %s
                    ORDER BY keyword, country, rank NULLS LAST, searched_at;
                    """,
                    (scan_id,),
                )
                rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as exc:
        logger.error("Fetching scan results failed: %s", exc)
        return None


def _format_datetime_display(value: datetime | str | None) -> tuple[str, str]:
    dt = None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = _parse_iso_datetime(value)

    if dt:
        dt = dt.astimezone(timezone.utc)
        iso_value = dt.isoformat(timespec="seconds").replace("+00:00", "Z")
        human_value = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        return human_value, iso_value

    raw_value = str(value or "")
    if not raw_value:
        return "N/A", ""
    return raw_value, raw_value


def build_results_payload_from_scan(
    tracked_playlist_detail: dict, latest_scan: dict, scan_results_rows: list[dict]
) -> dict | None:
    if not scan_results_rows:
        return None

    playlist_id = tracked_playlist_detail.get("spotify_playlist_id") or ""
    playlist_link = tracked_playlist_detail.get("playlist_url") or (
        f"https://open.spotify.com/playlist/{playlist_id}" if playlist_id else ""
    )
    playlist_title = tracked_playlist_detail.get("display_name") or "Tracked Playlist"
    playlist_image_url = tracked_playlist_detail.get("playlist_image_url") or ""

    playlist_meta_cache: dict[str, dict] = {}
    for row in scan_results_rows:
        pid = row.get("playlist_id") or (playlist_id if row.get("is_your_playlist") and playlist_id else None)
        if not pid:
            continue
        if pid in playlist_meta_cache:
            continue
        playlist_meta_cache[pid] = {
            "playlist_name": row.get("playlist_name") or "-",
            "playlist_url": row.get("playlist_url") or "",
            "playlist_description": row.get("playlist_description") or "",
            "playlist_followers": row.get("playlist_followers"),
            "songs_count": row.get("songs_count"),
            "playlist_last_track_added_at": row.get("playlist_last_track_added_at"),
            "playlist_owner": row.get("playlist_owner"),
        }

    searched_at_values = [row.get("searched_at") for row in scan_results_rows if row.get("searched_at")]
    searched_at_display, searched_at_iso = _format_datetime_display(
        searched_at_values[0] if searched_at_values else latest_scan.get("created_at")
    )

    grouped_results: dict[str, dict[str, list[dict]]] = {}
    keyword_order: list[str] = []
    country_order: dict[str, list[str]] = {}
    download_rows: list[dict] = []
    summary_rows: list[str] = []

    for row in scan_results_rows:
        kw = row.get("keyword") or ""
        country = row.get("country") or ""
        grouped_results.setdefault(kw, {}).setdefault(country, []).append(row)
        if kw not in keyword_order:
            keyword_order.append(kw)
        country_order.setdefault(kw, [])
        if country not in country_order[kw]:
            country_order[kw].append(country)

        download_rows.append(
            {
                "searched_at": row.get("searched_at") or searched_at_display,
                "keyword": kw,
                "country": country,
                "rank": row.get("rank"),
                "playlist_name": row.get("playlist_name"),
                "playlist_owner": row.get("playlist_owner"),
                "playlist_url": row.get("playlist_url"),
                "playlist_description": row.get("playlist_description"),
                "is_your_playlist": row.get("is_your_playlist"),
                "playlist_followers": row.get("playlist_followers"),
                "songs_count": row.get("songs_count"),
                "playlist_last_track_added_at": row.get("playlist_last_track_added_at"),
            }
        )

    keyword_results: list[dict] = []
    for kw in keyword_order:
        country_entries = []
        for country in country_order.get(kw, []):
            rows = grouped_results.get(kw, {}).get(country, [])
            market = COUNTRIES.get(country) or country
            per_country_results = []
            found_rank = None
            for row in rows:
                pid = row.get("playlist_id") or (playlist_id if row.get("is_your_playlist") and playlist_id else None)
                per_country_results.append(
                    {
                        "id": pid or "",
                        "name": row.get("playlist_name") or "-",
                        "external_urls": {"spotify": row.get("playlist_url") or ""},
                        "description": row.get("playlist_description") or "",
                        "owner": {"display_name": row.get("playlist_owner")},
                        "placeholder": False,
                    }
                )
                if playlist_id and pid == playlist_id and found_rank is None:
                    found_rank = row.get("rank") or "N/A"

            actual_count = len(per_country_results)
            if actual_count == 0:
                summary_rows.append(f"{searched_at_display} - keyword: '{kw}', country: {country} -> no results found.")
            elif playlist_id:
                if found_rank:
                    summary_rows.append(
                        f"{searched_at_display} - keyword: '{kw}', country: {country} ({market}) -> ranking: #{found_rank}"
                    )
                else:
                    summary_rows.append(
                        f"{searched_at_display} - keyword: '{kw}', country: {country} ({market}) -> your playlist was not listed."
                    )
            else:
                summary_rows.append(
                    f"{searched_at_display} - keyword: '{kw}', country: {country} ({market}) -> {actual_count} results listed."
                )

            country_entries.append(
                {
                    "country": country,
                    "market": market,
                    "results": per_country_results,
                    "actual_count": actual_count,
                    "keyword": kw,
                }
            )
        keyword_results.append({"keyword": kw, "countries": country_entries})

    return {
        "scan_id": latest_scan.get("id"),
        "searched_at_str": searched_at_display,
        "searched_at_iso": searched_at_iso or searched_at_display,
        "summary_rows": summary_rows,
        "download_rows": download_rows,
        "keyword_results": keyword_results,
        "playlist_meta_cache": playlist_meta_cache,
        "playlist_id": playlist_id,
        "playlist_title": playlist_title,
        "playlist_link": playlist_link,
        "playlist_image_url": playlist_image_url,
    }


def delete_tracked_playlist_and_related(tracked_playlist_id: str) -> tuple[bool, str]:
    tracked_playlist_id = (tracked_playlist_id or "").strip()
    try:
        UUID(tracked_playlist_id)
    except Exception:
        return False, "Invalid tracked playlist id."

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return False, "Database not configured."

    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT playlist_id FROM tracked_playlists WHERE id = %s LIMIT 1;",
                        (tracked_playlist_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return False, "Tracked playlist not found."
                    playlist_row_id = row[0]

                    cur.execute(
                        """
                        DELETE FROM scan_results
                        WHERE scan_id IN (
                            SELECT id FROM scans WHERE tracked_playlist_id = %s
                        );
                        """,
                        (tracked_playlist_id,),
                    )
                    cur.execute(
                        "DELETE FROM scans WHERE tracked_playlist_id = %s;",
                        (tracked_playlist_id,),
                    )
                    cur.execute("SELECT to_regclass('public.saved_scans');")
                    saved_scans_table = cur.fetchone()
                    if saved_scans_table and saved_scans_table[0]:
                        cur.execute(
                            "DELETE FROM saved_scans WHERE tracked_playlist_id = %s;",
                            (tracked_playlist_id,),
                        )
                    cur.execute("SELECT to_regclass('public.scheduled_scans');")
                    scheduled_scans_table = cur.fetchone()
                    if scheduled_scans_table and scheduled_scans_table[0]:
                        cur.execute(
                            "DELETE FROM scheduled_scans WHERE tracked_playlist_id = %s;",
                            (tracked_playlist_id,),
                        )
                    cur.execute(
                        "DELETE FROM tracked_playlists WHERE id = %s;",
                        (tracked_playlist_id,),
                    )
                    cur.execute(
                        """
                        DELETE FROM playlists p
                        WHERE p.id = %s
                          AND NOT EXISTS (
                              SELECT 1 FROM tracked_playlists tp WHERE tp.playlist_id = p.id
                          );
                        """,
                        (playlist_row_id,),
                    )
        return True, "Tracked playlist deleted."
    except Exception as exc:
        logger.exception("Deleting tracked playlist failed: %s", exc)
        detail = (str(exc) or exc.__class__.__name__).splitlines()[0]
        return False, f"Delete failed: {exc.__class__.__name__}: {detail}"


def run_delete_cascade_smoke_test():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.info("Delete cascade smoke test skipped: DATABASE_URL not set.")
        return

    playlist_row_id = str(uuid4())
    tracked_playlist_id = str(uuid4())
    scan_id = str(uuid4())
    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO playlists (id, spotify_playlist_id, playlist_url, display_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (spotify_playlist_id) DO NOTHING;
                        """,
                        (playlist_row_id, f"smoke-{playlist_row_id}", "https://example.com", "Smoke Test Playlist"),
                    )
                    cur.execute(
                        """
                        INSERT INTO tracked_playlists (id, account_id, playlist_id, display_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (account_id, playlist_id) DO NOTHING;
                        """,
                        (tracked_playlist_id, None, playlist_row_id, "Smoke Test Playlist"),
                    )
                    cur.execute(
                        """
                        INSERT INTO scans (id, countries, keywords, playlist_url, total_requests, tracked_playlist_id, finished_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (id) DO NOTHING;
                        """,
                        (scan_id, ["TR"], ["smoke"], "https://example.com", 1, tracked_playlist_id),
                    )
                    cur.execute(
                        """
                        INSERT INTO scan_results (id, scan_id, searched_at, keyword, country, rank, playlist_id, playlist_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        """,
                        (str(uuid4()), scan_id, datetime.now(timezone.utc).isoformat(), "smoke", "TR", 1, "pl-1", "Smoke Playlist"),
                    )
            success, msg = delete_tracked_playlist_and_related(tracked_playlist_id)
            if not success:
                raise RuntimeError(f"Smoke test delete failed: {msg}")
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM scans WHERE tracked_playlist_id = %s;", (tracked_playlist_id,))
                remaining_scans = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM scan_results WHERE scan_id = %s;", (scan_id,))
                remaining_results = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM tracked_playlists WHERE id = %s;", (tracked_playlist_id,))
                remaining_tracked = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM playlists WHERE id = %s;", (playlist_row_id,))
                remaining_playlists = cur.fetchone()[0]
                if any([remaining_scans, remaining_results, remaining_tracked, remaining_playlists]):
                    raise RuntimeError(
                        f"Smoke test residual rows -> scans:{remaining_scans}, results:{remaining_results}, tracked:{remaining_tracked}, playlists:{remaining_playlists}"
                    )
        logger.info("Delete cascade smoke test passed for tracked_playlist_id=%s", tracked_playlist_id)
    except Exception as exc:
        logger.exception("Delete cascade smoke test failed: %s", exc)


def render_results(
    results_data,
    tracked_playlist_id: str | None = None,
    show_hide_controls: bool = False,
    hide_controls_config: dict | None = None,
):
    if not results_data:
        return

    now_str = results_data.get("searched_at_str", "")
    now_iso = results_data.get("searched_at_iso", now_str)
    try:
        now_dt = datetime.fromisoformat(now_iso)
    except ValueError:
        now_dt = datetime.now()

    playlist_id = results_data.get("playlist_id")
    playlist_title = results_data.get("playlist_title") or "Playlist"
    playlist_link = results_data.get("playlist_link") or ""
    playlist_image_url = results_data.get("playlist_image_url") or ""
    summary_rows = results_data.get("summary_rows", [])
    download_rows = results_data.get("download_rows", [])
    keyword_results = results_data.get("keyword_results", [])
    playlist_meta_cache = results_data.get("playlist_meta_cache", {})

    st.subheader(f"Search Summary: {now_str}")
    effective_hide_controls = hide_controls_config
    if effective_hide_controls is None and show_hide_controls and tracked_playlist_id:
        effective_hide_controls = {
            "state_key": _latest_results_visibility_state_key(tracked_playlist_id),
            "state_value": False,
            "button_key": f"hide_latest_{tracked_playlist_id}",
            "label": "Hide Results",
        }

    download_target = st
    hide_button_target = None
    if effective_hide_controls and tracked_playlist_id:
        download_target, hide_button_target = st.columns(2)
    else:
        download_target = st.empty()

    if playlist_id:
        st.markdown("### Playlist Ranking Summary")
        playlist_meta = playlist_meta_cache.get(playlist_id) or {}
        playlist_name = playlist_meta.get("playlist_name") or playlist_title or "Playlist"
        playlist_cover = playlist_meta.get("playlist_image") or playlist_image_url
        playlist_followers = playlist_meta.get("playlist_followers")
        songs_count = playlist_meta.get("songs_count")
        playlist_last_track_added_at = playlist_meta.get("playlist_last_track_added_at")
        last_updated_display = format_last_updated_display(playlist_last_track_added_at, now_dt)
        card_link_target = (
            playlist_meta.get("playlist_url")
            or playlist_link
            or f"https://open.spotify.com/playlist/{playlist_id}"
        )
        followers_display = playlist_followers if playlist_followers is not None else "N/A"
        songs_display = songs_count if songs_count is not None else "N/A"

        st.markdown(
            """
            <style>
            .playlist-summary-card {
                display: inline-flex;
                align-items: center;
                gap: 12px;
                width: 100%;
                padding: 12px 14px;
                border: 1px solid rgba(0,0,0,0.08);
                border-radius: 12px;
                background: #fff;
                text-decoration: none;
                box-shadow: 0 2px 6px rgba(0, 0, 0, 0.04);
                transition: box-shadow 0.2s ease, transform 0.2s ease;
            }
            .playlist-summary-card:hover {
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
                transform: translateY(-1px);
            }
            .playlist-summary-cover {
                width: 52px;
                height: 52px;
                border-radius: 10px;
                object-fit: cover;
                background: #f5f5f5;
            }
            .playlist-summary-content {
                display: flex;
                flex-direction: column;
                gap: 4px;
            }
            .playlist-summary-title {
                margin: 0;
                font-weight: 700;
                color: #111;
                font-size: 16px;
                line-height: 1.2;
            }
            .playlist-summary-meta {
                margin: 0;
                color: #666;
                font-size: 13px;
                line-height: 1.4;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            <a class="playlist-summary-card" href="{card_link_target}" target="_blank" rel="noopener noreferrer">
                <img class="playlist-summary-cover" src="{playlist_cover}" alt="Playlist Cover" />
                <div class="playlist-summary-content">
                    <div class="playlist-summary-title">{playlist_name}</div>
                    <div class="playlist-summary-meta">Followers: {followers_display} • Songs Count: {songs_display} • Last Updated: {last_updated_display}</div>
                </div>
            </a>
            """,
            unsafe_allow_html=True,
        )
        for row in summary_rows:
            if "ranking" in row:
                st.success(row)
            else:
                st.info(row)
    else:
        st.markdown("### Search Summary Details")
        for row in summary_rows:
            st.info(row)

    df = pd.DataFrame(download_rows, columns=CSV_EXPORT_COLUMNS)
    csv_data = df.to_csv(index=False)
    download_target.download_button(
        "Download results as CSV",
        data=csv_data.encode("utf-8"),
        file_name=f"search_results_{now_str.replace(' ', '_').replace(':', '-')}.csv",
        mime="text/csv",
    )

    if hide_button_target is not None and tracked_playlist_id:
        hide_label = effective_hide_controls.get("label", "Hide Results")
        hide_button_key = effective_hide_controls.get("button_key") or f"hide_results_{tracked_playlist_id}"
        hide_clicked = hide_button_target.button(
            hide_label,
            key=hide_button_key,
        )
        if hide_clicked:
            state_key_to_update = effective_hide_controls.get("state_key")
            state_value = effective_hide_controls.get("state_value", False)
            if state_key_to_update:
                st.session_state[state_key_to_update] = state_value
            hide_handler = effective_hide_controls.get("on_hide")
            if callable(hide_handler):
                hide_handler()
            request_scroll("page_top", offset_px=0)
            st.rerun()

    for keyword_data in keyword_results:
        kw = keyword_data["keyword"]
        st.markdown(f"## Keyword: {kw}")
        for country_data in keyword_data["countries"]:
            selected_country = country_data["country"]
            market = country_data["market"]
            results = country_data["results"]
            actual_count = country_data["actual_count"]

            st.markdown(f"### Country: {selected_country} ({market})")
            if not results:
                st.warning("No results for this keyword and country.")
                continue

            subtitle_suffix = "" if actual_count >= RESULTS_LIMIT else f" - {actual_count} real results, remaining placeholders"
            st.subheader(
                f"Top {RESULTS_LIMIT} Results (country: {selected_country} ({market}), keyword: '{kw}'){subtitle_suffix}"
            )

            for i, p in enumerate(results, start=1):
                pid = p.get("id", "")
                is_placeholder = p.get("placeholder", False)
                cached_meta = playlist_meta_cache.get(pid) or {}
                name = cached_meta.get("playlist_name", p.get("name", "-"))
                url = cached_meta.get("playlist_url", (p.get("external_urls") or {}).get("spotify", ""))
                followers = None if is_placeholder else cached_meta.get("playlist_followers")
                songs_count = None if is_placeholder else cached_meta.get("songs_count")
                playlist_last_track_added_at = None if is_placeholder else cached_meta.get("playlist_last_track_added_at")
                playlist_owner = cached_meta.get("playlist_owner")
                if not playlist_owner:
                    owner_info = p.get("owner") or {}
                    playlist_owner = owner_info.get("display_name") or owner_info.get("id")
                followers_display = followers if followers is not None else "N/A"
                songs_display = songs_count if songs_count is not None else "N/A"
                last_updated_display = format_last_updated_display(playlist_last_track_added_at, now_dt)
                detail_line = (
                    f"Owner: {playlist_owner or 'N/A'} - Followers: {followers_display} - Songs Count: {songs_display} - Last Updated: {last_updated_display}"
                )
                if playlist_id and pid == playlist_id:
                    st.markdown(f"**#{i}  {name}  (YOUR PLAYLIST)**")
                    st.caption(detail_line)
                else:
                    st.write(f"#{i}  {name}")
                    st.caption(detail_line)

                if url:
                    st.caption(url)


def render_tracked_card_styles():
    st.markdown(
        """
        <style>
        .tracked-section {
            background: #fff;
            border: 1px solid rgba(0, 0, 0, 0.08);
            border-radius: 12px;
            max-height: 60px;
            padding-left: 20px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.04);
            margin-bottom: 18px;
            width: 100%;
        }
        .tracked-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 12px;
        }
        .tracked-header h2 {
            margin: 0;
            font-size: 20px;
            font-weight: 800;
            color: #0c0c0c;
        }
        :root {
            --tracked-card-padding-y: 14px;
            --tracked-card-padding-x: 16px;
            --tracked-card-inner-gap: 14px;
            --tracked-thumb-size: 88px;
        }
        .tracked-card-list {
            display: flex;
            flex-direction: column;
            gap: 12px;
            margin-bottom: 16px;
            width: 100%;
        }
        .tracked-card {
            background: #fff;
            margin-top: 12px;
            border: 1px solid rgba(0, 0, 0, 0.08);
            border-radius: 12px;
            padding: var(--tracked-card-padding-y) var(--tracked-card-padding-x);
            box-shadow: 0 2px 6px rgba(0,0,0,0.04);
            position: relative;
            transition: transform 0.1s ease, box-shadow 0.1s ease;
            width: 100%;
            box-sizing: border-box;
        }
        .tracked-card-link {
            text-decoration: none;
            color: inherit;
            display: block;
        }
        .tracked-card-link,
        .tracked-card-link:visited,
        .tracked-card-link:hover,
        .tracked-card-link:active,
        .tracked-card a,
        .tracked-card a:visited,
        .tracked-card a:hover,
        .tracked-card a:active {
            color: inherit;
            text-decoration: none;
            outline: none;
        }
        .tracked-card-link:focus-visible .tracked-card,
        .tracked-card-link:hover .tracked-card {
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            transform: translateY(-1px);
        }
        .tracked-card-clickable {
            cursor: pointer;
        }
        .tracked-card-inner {
            display: flex;
            align-items: flex-start;
            gap: var(--tracked-card-inner-gap);
            min-width: 0;
        }
        @media (max-width: 640px) {
            :root {
                --tracked-card-inner-gap: 12px;
                --tracked-thumb-size: 72px;
            }
            .tracked-card-inner {
                gap: var(--tracked-card-inner-gap);
            }
        }
        .tracked-thumb {
            width: var(--tracked-thumb-size);
            height: var(--tracked-thumb-size);
            border-radius: 12px;
            overflow: hidden;
            background: linear-gradient(135deg, #f5f5f5, #e9e9e9);
            display: flex;
            align-items: center;
            justify-content: center;
            flex: 0 0 auto;
        }
        .tracked-thumb img {
            width: 100%;
            height: 100%;
            object-fit: cover;
        }
        .tracked-pill-row {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
            min-width: 0;
        }
        .tracked-pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: #ff4c00;
            color: #fff;
            border-radius: 8px;
            padding: 4px 8px;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.02em;
            white-space: nowrap;
            text-decoration: none;
            border: none;
            cursor: pointer;
            appearance: none;
        }
        .tracked-pill:visited,
        .tracked-pill:hover,
        .tracked-pill:active {
            color: #fff;
            text-decoration: none;
        }
        .manage-pill {
            background: #0d72b3;
        }
        .tracked-open-dashboard {
            position: relative;
        }
        .tracked-manage-title {
            font-weight: 700;
            color: #111;
            margin-bottom: 12px;
        }
        .tracked-manage-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin: 8px 0 12px;
        }
        .tracked-manage-chip {
            padding: 6px 10px;
            border-radius: 999px;
            background: #efefef;
            color: #7b7b7b;
            font-size: 12px;
            font-weight: 600;
        }
        @media (max-width: 640px) {
            div[data-testid="stHorizontalBlock"]:has(div[class*="st-key-manage_action_"]) {
                flex-direction: column;
                align-items: center;
            }
            div[data-testid="stHorizontalBlock"]:has(div[class*="st-key-manage_action_"]) > div {
                width: 100%;
                max-width: 360px;
            }
        }
        div[data-testid="stVerticalBlock"]:has(.tracked-card) {
            position: relative;
        }
        div[data-testid="stElementContainer"][class*="st-key-open_dashboard_"] {
            position: absolute;
            top: 0;
            left: 0;
            z-index: 5;
            width: 0;
            height: 0;
            overflow: visible;
            padding: 0 !important;
            margin: 0 !important;
        }
        div[data-testid="stElementContainer"][class*="st-key-open_dashboard_"] > div[data-testid="stButton"] {
            position: absolute;
            top: var(--tracked-card-padding-y);
            left: calc(var(--tracked-card-padding-x) + var(--tracked-thumb-size) + var(--tracked-card-inner-gap));
            margin: 0;
        }
        div[data-testid="stElementContainer"][class*="st-key-open_dashboard_"] > div[data-testid="stButton"] button {
            background: #ff4c00;
            color: #fff;
            border-radius: 8px;
            padding: 4px 8px;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.02em;
            border: none;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            line-height: 1.2;
            opacity: 0;
            position: relative;
            z-index: 6;
            white-space: nowrap;
            box-shadow: none;
        }
        div[data-testid="stElementContainer"][class*="st-key-open_dashboard_"] > div[data-testid="stButton"] button:focus-visible {
            outline: 2px solid #111;
        }
        div[data-testid="stElementContainer"][class*="st-key-manage_tracked_"] {
            position: absolute;
            top: 0;
            left: 0;
            z-index: 5;
            width: 0;
            height: 0;
            overflow: visible;
            padding: 0 !important;
            margin: 0 !important;
        }
        div[data-testid="stElementContainer"][class*="st-key-manage_tracked_"] > div[data-testid="stButton"] {
            position: absolute;
            top: var(--tracked-card-padding-y);
            left: calc(var(--tracked-card-padding-x) + var(--tracked-thumb-size) + var(--tracked-card-inner-gap) + 124px);
            margin: 0;
        }
        div[data-testid="stElementContainer"][class*="st-key-manage_tracked_"] > div[data-testid="stButton"] button {
            background: #0d72b3;
            color: #fff;
            border-radius: 8px;
            padding: 4px 8px;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.02em;
            border: none;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            line-height: 1.2;
            opacity: 0;
            position: relative;
            z-index: 6;
            white-space: nowrap;
            box-shadow: none;
        }
        div[data-testid="stElementContainer"][class*="st-key-manage_tracked_"] > div[data-testid="stButton"] button:focus-visible {
            outline: 2px solid #111;
        }
        .tracked-title {
            margin: 0;
            font-weight: 700;
            font-size: 18px;
            color: #0c0c0c;
            line-height: 1.35;
            word-break: break-word;
            min-width: 0;
        }
        .tracked-meta-line {
            color: #5a5a5a;
            font-size: 13px;
            line-height: 1.5;
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            align-items: center;
            overflow-wrap: anywhere;
            min-width: 0;
        }
        .tracked-meta-line + .tracked-meta-line {
            margin-top: 4px;
        }
        .tracked-meta-group {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            min-width: 0;
        }
        .tracked-meta-label {
            font-weight: 700;
            color: #2b2b2b;
        }
        .tracked-meta-value {
            color: #424242;
            overflow-wrap: anywhere;
        }
        .tracked-meta-subtle {
            color: #444;
            font-weight: 600;
        }
        .tracked-add-row .stButton>button {
            width: 100%;
            background: #fff;
            border: 1px solid rgba(0,0,0,0.08);
            color: #111;
            font-weight: 700;
            padding: 12px;
            border-radius: 12px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.04);
        }
        .tracked-add-row .stButton>button:hover {
            border-color: rgba(0,0,0,0.18);
        }
        .tracked-inline-form {
            background: #fff;
            border: 1px dashed rgba(0,0,0,0.12);
            border-radius: 12px;
            padding: 16px;
            box-shadow: inset 0 1px 3px rgba(0,0,0,0.04);
        }
        .tracked-inline-form .stTextInput>div>div>input {
            background: #fff;
        }
        .tracked-card.detail-header-card {
            padding: 18px 20px;
            --tracked-card-padding-y: 18px;
            --tracked-card-padding-x: 20px;
        }
        .detail-header-grid {
            display: flex;
            align-items: flex-start;
            gap: 16px;
            min-width: 0;
        }
        .detail-title-row {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
        }
        .tracked-card-content {
            display: flex;
            flex-direction: column;
            gap: 8px;
            min-width: 0;
            overflow: hidden;
        }
        .detail-title {
            font-size: 22px;
            line-height: 1.3;
            flex: 1 1 auto;
        }
        .tracked-card-actions {
            display: flex;
            justify-content: flex-end;
        }
        .tracked-primary-link {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            padding: 10px 16px;
            background: #111;
            color: #fff !important;
            border-radius: 10px;
            font-weight: 700;
            text-decoration: none;
            box-shadow: 0 2px 6px rgba(0,0,0,0.08);
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }
        .tracked-primary-link:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
        }
        .detail-header-card .tracked-pill-row {
            flex-wrap: nowrap;
            gap: 12px;
        }
        .spotify-pill {
            background: #1DB954;
            color: #fff !important;
            border-radius: 999px;
            padding: 6px 12px;
            font-size: 11px;
            font-weight: 800;
            letter-spacing: 0.02em;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-decoration: none !important;
            border: none;
            outline: none;
            white-space: nowrap;
        }
        .spotify-pill:hover {
            background: #18a647;
        }
        .refresh-pill {
            background: #ff3900;
            color: #fff !important;
            border-radius: 999px;
            padding: 6px 12px;
            font-size: 11px;
            font-weight: 800;
            letter-spacing: 0.02em;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-decoration: none !important;
            border: none;
            outline: none;
            white-space: nowrap;
            cursor: pointer;
        }
        .refresh-pill:hover {
            background: #2d1b16;
        }
        @media (max-width: 640px) {
            .detail-header-grid {
                gap: 12px;
            }
            .tracked-card-actions {
                justify-content: flex-start;
                margin-top: 8px;
            }
            .tracked-primary-link {
                width: 100%;
            }
        }

        /* Add New Playlist button full width */
            div[data-testid="stElementContainer"].st-key-open_add_playlist {
              width: 100% !important;
        }

            div[data-testid="stElementContainer"].st-key-open_add_playlist button {
              width: 100% !important;
        }

        /* Refresh Stats button mirrors Add New Playlist */
            div[data-testid="stElementContainer"][class*="st-key-refresh_stats_toggle_"] {
              width: 100% !important;
              margin: 0 !important;
        }

            div[data-testid="stElementContainer"][class*="st-key-refresh_stats_toggle_"] button {
              width: 100% !important;
              margin: 0 !important;
        }


        /* Save / Cancel element containers: kill fit-content */
            div[data-testid="stElementContainer"].st-key-add_playlist_save,
            div[data-testid="stElementContainer"].st-key-add_playlist_cancel {
              width: 100% !important;
              flex: 1 1 0% !important;
              min-width: 0 !important;
        }

        /* Make the actual buttons fill their container */
            div[data-testid="stElementContainer"].st-key-add_playlist_save button,
            div[data-testid="stElementContainer"].st-key-add_playlist_cancel button {
              width: 100% !important;
        }

        /* Manage action buttons mirror Save / Cancel sizing */
            div[data-testid="stElementContainer"][class*="st-key-manage_action_"] {
              width: 100% !important;
              flex: 1 1 0% !important;
              min-width: 0 !important;
        }

            div[data-testid="stElementContainer"][class*="st-key-manage_action_"] button {
              width: 100% !important;
        }

        @media (max-width: 640px) {
            .tracked-thumb {
                width: var(--tracked-thumb-size);
                height: var(--tracked-thumb-size);
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_tracked_playlist_card_html(
    *,
    title: str,
    owner: str | None,
    followers: str | None,
    songs_count: str | None,
    last_updated: str | None = None,
    scanned_display: str | None = None,
    playlist_image_url: str | None = None,
    spotify_playlist_id: str | None = None,
    playlist_url: str | None = None,
    variant: str = "list",
    card_href: str | None = None,
    card_target: str | None = None,
    open_dashboard_label: str = "Open Dashboard",
):
    thumb_html = f'<img src="{playlist_image_url}" alt="Playlist artwork" />' if playlist_image_url else ""
    spotify_pill_html = ""
    if variant == "detail" and playlist_url:
        spotify_pill_html = (
            f'<a class="tracked-pill spotify-pill" href="{escape(playlist_url)}" target="_blank" rel="noopener noreferrer">'
            f"{escape('View on Spotify')}</a>"
        )
    meta_fields_line_one = [
        ("Owner:", owner or "—"),
        ("Followers:", followers if followers is not None else "—"),
        ("Songs Count:", songs_count if songs_count is not None else "—"),
    ]
    meta_fields_line_two = [
        ("Scanned:", scanned_display if scanned_display is not None else "—"),
        ("Last Updated:", last_updated if last_updated is not None else "—"),
    ]

    def _build_tracked_card_body_html(
        *,
        pill_row_html: str,
        outer_class: str,
        inner_class: str,
    ) -> str:
        return safe_html(
            f"""
            <div class="{outer_class}">
                <div class="{inner_class}">
                    <div class="tracked-thumb">{thumb_html}</div>
                    <div class="tracked-card-content">
                        <div class="tracked-pill-row">
                            {pill_row_html}
                        </div>
                        <div class="tracked-title" title="{escape(title)}">{escape(title)}</div>
                        <div class="tracked-meta-line">{build_meta_line(meta_fields_line_one)}</div>
                        <div class="tracked-meta-line tracked-meta-subtle">{build_meta_line(meta_fields_line_two)}</div>
                    </div>
                </div>
            </div>
            """
        )

    def build_meta_line(fields: list[tuple[str, str]]) -> str:
        parts = []
        for label, value in fields:
            parts.append(
                f'<span class="tracked-meta-group"><span class="tracked-meta-label">{escape(label)}</span>'
                f'<span class="tracked-meta-value">{escape(str(value))}</span></span>'
            )
        return "".join(parts)

    if variant == "detail":
        pill_row_html = spotify_pill_html
        card_body = _build_tracked_card_body_html(
            pill_row_html=pill_row_html,
            outer_class="tracked-card detail-header-card",
            inner_class="tracked-card-inner detail-header-grid",
        )
    else:
        open_dashboard_pill = (
            f'<span class="tracked-pill tracked-open-dashboard" title="{escape(open_dashboard_label)}">'
            f"{escape(open_dashboard_label)}</span>"
        )
        manage_pill = '<span class="tracked-pill manage-pill">Manage</span>'
        card_body = _build_tracked_card_body_html(
            pill_row_html=f"{open_dashboard_pill}{manage_pill}",
            outer_class="tracked-card",
            inner_class="tracked-card-inner",
        )
    final_html = card_body

    if card_href and variant != "detail":
        target_attr = f' target="{card_target}" rel="noopener noreferrer"' if card_target else ""
        final_html = f'<a class="tracked-card-link" href="{escape(card_href)}"{target_attr}>{card_body}</a>'

    if not final_html.startswith(("<div", "<a")):
        logger.warning("Tracked playlist card HTML may be indented or escaped.")

    return safe_html(final_html)


def render_tracked_playlist_card(card_html: str, container=None):
    html = safe_html(card_html)
    if container is None:
        st.markdown(html, unsafe_allow_html=True)
    else:
        container.markdown(html, unsafe_allow_html=True)


def render_tracked_playlists_section(tracked_playlists: list[dict], debug_enabled: bool = False, db_connection_ok: bool = True):
    show_add_form = bool(st.session_state.get("show_add_playlist_form"))
    add_in_progress = bool(st.session_state.get("add_in_progress"))
    render_tracked_card_styles()
    if "open_manage_tracked_playlist_id" not in st.session_state:
        st.session_state["open_manage_tracked_playlist_id"] = None
    if "open_manage_edit_tracked_playlist_id" not in st.session_state:
        st.session_state["open_manage_edit_tracked_playlist_id"] = None

    def _safe_update_list_state(state_key: str, new_list: list[str]) -> None:
        normalized_list: list[str] = []
        seen: set[str] = set()
        for item in new_list:
            item = (item or "").strip()
            if not item or item in seen:
                continue
            normalized_list.append(item)
            seen.add(item)
        st.session_state[state_key] = normalized_list

    section = st.container()
    with section:
        st.markdown(
            """
            <div class="tracked-section">
                <div class="tracked-header">
                    <h2>Tracked Playlists</h2>
                </div>
                <div class="tracked-card-list">
            """,
            unsafe_allow_html=True,
        )
        if debug_enabled:
            st.caption(f"Tracked playlists loaded: {len(tracked_playlists)}")

        card_container = st.container()

        with card_container:
            clicked = st.button(
                "➕ Add New Playlist",
                key="open_add_playlist",
                disabled=add_in_progress,
            )
            if clicked:
                st.session_state["show_add_playlist_form"] = True
                st.session_state["show_add_playlist"] = True
                st.session_state["add_playlist_error"] = None
                show_add_form = True
                request_scroll("tracked_add_section", offset_px=-12)



    def normalize_add_playlist_url():
        key = st.session_state.get("current_add_playlist_input_key")
        if not key:
            return
        value = (st.session_state.get(key) or "").strip()
        normalized_value = value.split("?", 1)[0]
        if normalized_value != value:
            st.session_state[key] = normalized_value

    def render_add_form(target_container, err_placeholder):
        input_key = f"add_playlist_url_input_{st.session_state['add_playlist_input_version']}"
        st.session_state["current_add_playlist_input_key"] = input_key
        target_countries_key = f"add_playlist_target_countries_{st.session_state['add_playlist_input_version']}"
        target_keywords_key = f"add_playlist_target_keywords_{st.session_state['add_playlist_input_version']}"
        _country_map, country_names = get_country_selection_options()
        if target_countries_key not in st.session_state:
            st.session_state[target_countries_key] = []
        if target_keywords_key not in st.session_state:
            st.session_state[target_keywords_key] = ""
        url_value = target_container.text_input(
            "Spotify Playlist URL",
            key=input_key,
            placeholder="https://open.spotify.com/playlist/...",
            on_change=normalize_add_playlist_url,
            disabled=add_in_progress,
        )
        target_container.multiselect(
            "Target Countries",
            options=country_names,
            key=target_countries_key,
            disabled=add_in_progress,
            help="Select up to 5 countries.",
        )
        target_container.text_input(
            "Target Keywords",
            key=target_keywords_key,
            disabled=add_in_progress,
            help="Enter up to 5 keywords separated by commas.",
        )
        col_save, col_cancel = target_container.columns(2)

        with col_save:
            save_clicked = st.button("Save", key="add_playlist_save", disabled=add_in_progress)

        with col_cancel:
            cancel_clicked = st.button("Cancel", key="add_playlist_cancel", disabled=add_in_progress)


        if save_clicked:
            trimmed_url_value = (st.session_state.get(input_key) or "").strip().split("?", 1)[0]
            selected_countries = st.session_state.get(target_countries_key) or []
            raw_keywords = st.session_state.get(target_keywords_key) or ""
            keywords_raw_list = [kw.strip() for kw in raw_keywords.split(",")] if isinstance(raw_keywords, str) else []
            normalized_keywords: list[str] = []
            has_duplicate = False
            for kw in keywords_raw_list:
                if not kw:
                    continue
                if kw in normalized_keywords:
                    has_duplicate = True
                else:
                    normalized_keywords.append(kw)
            if not trimmed_url_value:
                st.session_state["add_playlist_error"] = "Please enter a valid Spotify playlist URL."
                err_placeholder.error(st.session_state["add_playlist_error"])
                st.stop()
            if not selected_countries:
                st.session_state["add_playlist_error"] = "Please select at least one target country."
                err_placeholder.error(st.session_state["add_playlist_error"])
                st.stop()
            if len(selected_countries) > 5:
                st.session_state["add_playlist_error"] = "You can select up to 5 target countries."
                err_placeholder.error(st.session_state["add_playlist_error"])
                st.stop()
            if not normalized_keywords:
                st.session_state["add_playlist_error"] = "Please enter at least one target keyword."
                err_placeholder.error(st.session_state["add_playlist_error"])
                st.stop()
            if has_duplicate:
                st.session_state["add_playlist_error"] = "Duplicate keywords are not allowed."
                err_placeholder.error(st.session_state["add_playlist_error"])
                st.stop()
            if len(normalized_keywords) > 5:
                st.session_state["add_playlist_error"] = "You can select up to 5 target keywords."
                err_placeholder.error(st.session_state["add_playlist_error"])
                st.stop()
            st.session_state["add_in_progress"] = True
            try:
                with st.spinner("Please wait, adding playlist and fetching Spotify metadata..."):
                    success, message = upsert_tracked_playlist(
                        trimmed_url_value,
                        selected_countries,
                        normalized_keywords,
                    )
            finally:
                st.session_state["add_in_progress"] = False
            if success:
                st.cache_data.clear()
                try:
                    st.cache_resource.clear()
                except Exception:
                    pass
                st.session_state["show_add_playlist_form"] = False
                st.session_state["show_add_playlist"] = False
                st.session_state["add_playlist_error"] = None
                st.session_state["add_playlist_input_version"] += 1
                st.session_state.pop(input_key, None)
                st.session_state.pop("current_add_playlist_input_key", None)
                st.session_state.pop(target_countries_key, None)
                st.session_state.pop(target_keywords_key, None)
                request_scroll("page_top", offset_px=0)
                st.rerun()
            else:
                st.session_state["add_playlist_error"] = message
                err_placeholder.error(message)
                st.stop()

        if cancel_clicked:
            st.session_state["show_add_playlist_form"] = False
            st.session_state["show_add_playlist"] = False
            st.session_state["add_playlist_error"] = None
            st.session_state["add_playlist_input_version"] += 1
            st.session_state.pop(input_key, None)
            st.session_state.pop("current_add_playlist_input_key", None)
            st.session_state.pop(target_countries_key, None)
            st.session_state.pop(target_keywords_key, None)
            request_scroll("page_top", offset_px=0)
            st.rerun()

    if show_add_form:
        form_holder = card_container.container()
        with form_holder:
            render_scroll_anchor("tracked_add_section")
            err_placeholder = st.empty()
            if st.session_state.get("add_playlist_error"):
                err_placeholder.error(st.session_state["add_playlist_error"])
            render_add_form(form_holder, err_placeholder)
          
    open_dashboard_label = "Open Dashboard"
    for idx, item in enumerate(tracked_playlists):
        display_name = item.get("display_name") or "Tracked Playlist"
        followers_display = format_stat_value(item.get("followers"))
        if str(followers_display).upper() in {"NA", "N/A"}:
            followers_display = "—"
        songs_display = format_stat_value(item.get("songs_count"))
        if str(songs_display).upper() in {"NA", "N/A"}:
            songs_display = "—"
        last_updated_display = format_relative_time_or_dash(item.get("last_track_added_at"))
        scanned_display = format_relative_time_or_dash(item.get("scanned_at"))
        playlist_image_url = item.get("playlist_image_url") or ""
        spotify_playlist_id = item.get("spotify_playlist_id")
        if not playlist_image_url and spotify_playlist_id:
            try:
                meta = fetch_spotify_playlist_metadata(spotify_playlist_id)
                playlist_image_url = meta.get("playlist_image_url") or playlist_image_url
            except Exception:
                playlist_image_url = playlist_image_url

        tp_id = item.get("id")
        card_html = build_tracked_playlist_card_html(
            title=display_name,
            owner=item.get("owner") or "N/A",
            followers=followers_display,
            songs_count=songs_display,
            last_updated=last_updated_display,
            scanned_display=scanned_display,
            playlist_image_url=playlist_image_url,
            card_href=None,
            open_dashboard_label=open_dashboard_label,
        )
        card_block = card_container.container()
        with card_block:
            render_tracked_playlist_card(card_html, card_block)
            open_dashboard_key = f"open_dashboard_{tp_id}" if tp_id else f"open_dashboard_unknown_{idx}"
            open_dashboard_clicked = card_block.button(
                open_dashboard_label,
                key=open_dashboard_key,
                disabled=not tp_id,
                help="Open the tracked playlist dashboard",
            )
            if open_dashboard_clicked and tp_id:
                params = _flatten_query_params(_get_query_params())
                params["tp"] = str(tp_id)
                _update_query_params(params)
                st.session_state["selected_tracked_playlist_id"] = str(tp_id)
                st.rerun()
            manage_key = f"manage_tracked_{tp_id}" if tp_id else f"manage_tracked_unknown_{idx}"
            manage_clicked = card_block.button(
                "Manage",
                key=manage_key,
                disabled=not tp_id,
                help="Manage tracked playlist settings",
            )
            if manage_clicked and tp_id:
                current_open = st.session_state.get("open_manage_tracked_playlist_id")
                if current_open == str(tp_id):
                    st.session_state["open_manage_tracked_playlist_id"] = None
                    if st.session_state.get("open_manage_edit_tracked_playlist_id") == str(tp_id):
                        st.session_state["open_manage_edit_tracked_playlist_id"] = None
                else:
                    st.session_state["open_manage_tracked_playlist_id"] = str(tp_id)
                    if st.session_state.get("open_manage_edit_tracked_playlist_id") not in {None, str(tp_id)}:
                        st.session_state["open_manage_edit_tracked_playlist_id"] = None
                    request_scroll(f"tracked_manage_section_{tp_id}", offset_px=-12)

            if tp_id and st.session_state.get("open_manage_tracked_playlist_id") == str(tp_id):
                render_scroll_anchor(f"tracked_manage_section_{tp_id}")
                manage_section_container = card_block.container()
                with manage_section_container:
                    st.markdown('<div class="tracked-manage-title">Manage Playlist</div>', unsafe_allow_html=True)
                    action_message = st.empty()
                    edit_col, scan_col, delete_col = st.columns(3)
                    with edit_col:
                        edit_clicked = st.button(
                            "Edit Configuration",
                            key=f"manage_action_edit_{tp_id}",
                        )
                    with scan_col:
                        scan_clicked = st.button(
                            "Scan History",
                            key=f"manage_action_scan_{tp_id}",
                        )
                    with delete_col:
                        delete_clicked = st.button(
                            "Delete Playlist",
                            key=f"manage_action_delete_{tp_id}",
                        )
                    if scan_clicked or delete_clicked:
                        action_message.info("This feature will be available soon.")

                    if edit_clicked:
                        current_edit = st.session_state.get("open_manage_edit_tracked_playlist_id")
                        if current_edit == str(tp_id):
                            st.session_state["open_manage_edit_tracked_playlist_id"] = None
                        else:
                            st.session_state["open_manage_edit_tracked_playlist_id"] = str(tp_id)
                            request_scroll(f"tracked_manage_section_{tp_id}", offset_px=-12)

                    if st.session_state.get("open_manage_edit_tracked_playlist_id") == str(tp_id):
                        st.subheader("Edit Target Countries")
                        existing_countries = _normalize_target_countries(item.get("target_countries") or [])
                        st.markdown("Existing target countries (read-only):")
                        if existing_countries:
                            chips = "".join(
                                f'<span class="tracked-manage-chip">{escape(country)}</span>'
                                for country in existing_countries
                            )
                            st.markdown(f'<div class="tracked-manage-chip-row">{chips}</div>', unsafe_allow_html=True)
                        else:
                            st.caption("No target countries saved yet.")

                        country_map, country_names = get_country_selection_options()
                        available_countries = [name for name in country_names if name not in existing_countries]
                        add_limit = max(0, 5 - len(existing_countries))
                        new_countries_key = f"manage_new_countries_{tp_id}"
                        if new_countries_key not in st.session_state:
                            st.session_state[new_countries_key] = []
                        if add_limit == 0:
                            st.info("You have reached the maximum of 5 target countries.")
                        new_selection = st.multiselect(
                            "Add new countries",
                            options=available_countries,
                            key=new_countries_key,
                            disabled=add_limit == 0,
                            max_selections=add_limit if add_limit > 0 else None,
                            help="Select new countries to add. Existing countries cannot be removed.",
                        )
                        if any(country in existing_countries for country in new_selection):
                            st.session_state[new_countries_key] = [
                                country for country in new_selection if country not in existing_countries
                            ]
                            st.error(
                                "Existing target countries cannot be removed. To remove a country, please contact Support."
                            )
                            new_selection = st.session_state[new_countries_key]

                        st.subheader("Edit Target Keywords")
                        existing_keywords = _normalize_target_keywords(item.get("target_keywords") or [])
                        st.markdown("Existing target keywords (read-only):")
                        if existing_keywords:
                            chips = "".join(
                                f'<span class="tracked-manage-chip">{escape(keyword)}</span>'
                                for keyword in existing_keywords
                            )
                            st.markdown(f'<div class="tracked-manage-chip-row">{chips}</div>', unsafe_allow_html=True)
                        else:
                            st.caption("No target keywords saved yet.")

                        input_version_key = f"manage_keywords_input_version_{tp_id}"
                        new_keywords_key = f"manage_new_keywords_input_{tp_id}"
                        pending_keywords_key = f"manage_pending_keywords_{tp_id}"
                        input_key = None
                        keyword_form_key = f"manage_add_keywords_form_{tp_id}"
                        try:
                            if input_version_key not in st.session_state:
                                st.session_state[input_version_key] = 0
                            if pending_keywords_key not in st.session_state:
                                st.session_state[pending_keywords_key] = []
                            input_key = f"{new_keywords_key}_{st.session_state[input_version_key]}"
                            pending_keywords = st.session_state.get(pending_keywords_key, [])
                            add_keyword_limit = max(0, 5 - len(existing_keywords) - len(pending_keywords))
                            if add_keyword_limit == 0:
                                st.info("You have reached the maximum of 5 target keywords.")
                            with st.form(key=keyword_form_key, clear_on_submit=True):
                                st.text_input(
                                    "Add new keywords",
                                    key=input_key,
                                    disabled=add_keyword_limit == 0,
                                    help="Enter new keywords separated by commas. Existing keywords cannot be removed.",
                                )
                                add_keyword_clicked = st.form_submit_button(
                                    "Add Keyword",
                                    disabled=not tp_id or add_keyword_limit == 0,
                                )
                            if add_keyword_clicked:
                                raw_keywords = st.session_state.get(input_key) or ""
                                parsed_keywords = (
                                    [kw.strip() for kw in raw_keywords.split(",")]
                                    if isinstance(raw_keywords, str)
                                    else []
                                )
                                normalized_new_keywords: list[str] = []
                                has_duplicate = False
                                for kw in parsed_keywords:
                                    if not kw:
                                        continue
                                    if kw in normalized_new_keywords:
                                        has_duplicate = True
                                    else:
                                        normalized_new_keywords.append(kw)
                                if not normalized_new_keywords:
                                    st.error("Please enter at least one target keyword.")
                                else:
                                    pending_keywords = st.session_state.get(pending_keywords_key, [])
                                    if any(
                                        kw in existing_keywords or kw in pending_keywords for kw in normalized_new_keywords
                                    ):
                                        has_duplicate = True
                                    merged_keywords = _normalize_target_keywords(
                                        existing_keywords + pending_keywords + normalized_new_keywords
                                    )
                                    if len(merged_keywords) > 5:
                                        st.error("You can select up to 5 target keywords.")
                                    elif has_duplicate or len(merged_keywords) != len(set(merged_keywords)):
                                        st.error("Duplicate keywords are not allowed.")
                                    else:
                                        pending_next = list(pending_keywords)
                                        pending_next.extend(normalized_new_keywords)
                                        _safe_update_list_state(pending_keywords_key, pending_next)
                                        st.session_state[input_version_key] += 1
                                        st.rerun()
                            if pending_keywords:
                                st.markdown("New keywords to be added:")
                                chips = "".join(
                                    f'<span class="tracked-manage-chip">{escape(keyword)}</span>'
                                    for keyword in list(pending_keywords)
                                )
                                st.markdown(
                                    f'<div class="tracked-manage-chip-row">{chips}</div>',
                                    unsafe_allow_html=True,
                                )
                        except Exception:
                            logger.exception("Keyword UI crashed (tracked playlists). Resetting transient UI state.")
                            if input_key:
                                st.session_state.pop(input_key, None)
                            st.session_state.pop(pending_keywords_key, None)
                            st.session_state.pop(input_version_key, None)
                            st.rerun()
                        pending_countries = list(new_selection or [])
                        pending_keywords = st.session_state.get(pending_keywords_key, [])
                        has_pending_changes = bool(pending_countries or pending_keywords)
                        save_clicked = st.button(
                            "Save Configuration",
                            key=f"manage_save_all_{tp_id}",
                            disabled=not tp_id or not has_pending_changes,
                        )
                        if save_clicked:
                            if not has_pending_changes:
                                st.info("No changes to save.")
                            else:
                                countries_updated = True
                                keywords_updated = True
                                if pending_countries:
                                    merged_countries = _normalize_target_countries(
                                        existing_countries + list(pending_countries)
                                    )
                                    if len(merged_countries) > 5:
                                        st.error("You can select up to 5 target countries.")
                                        countries_updated = False
                                    elif len(merged_countries) != len(set(merged_countries)):
                                        st.error("Duplicate countries are not allowed.")
                                        countries_updated = False
                                    else:
                                        for name in merged_countries:
                                            if name not in country_map and name not in COUNTRIES:
                                                st.error(f"Unsupported country selected: {name}")
                                                countries_updated = False
                                                break
                                    if countries_updated:
                                        success, message = update_tracked_playlist_target_countries(
                                            str(tp_id),
                                            merged_countries,
                                        )
                                        if not success:
                                            st.error(message)
                                            countries_updated = False
                                if countries_updated and pending_keywords:
                                    merged_keywords = _normalize_target_keywords(
                                        existing_keywords + pending_keywords
                                    )
                                    if not merged_keywords:
                                        st.error("Please enter at least one target keyword.")
                                        keywords_updated = False
                                    elif len(merged_keywords) > 5:
                                        st.error("You can select up to 5 target keywords.")
                                        keywords_updated = False
                                    elif len(merged_keywords) != len(set(merged_keywords)):
                                        st.error("Duplicate keywords are not allowed.")
                                        keywords_updated = False
                                    else:
                                        success, message = update_tracked_playlist_target_keywords(
                                            str(tp_id),
                                            merged_keywords,
                                        )
                                        if not success:
                                            st.error(message)
                                            keywords_updated = False
                                if countries_updated and keywords_updated:
                                    st.success("Configuration saved.")
                                    st.session_state["tracked_playlists_refresh_token"] += 1
                                    st.session_state.pop(new_countries_key, None)
                                    st.session_state.pop(pending_keywords_key, None)
                                    st.session_state[input_version_key] += 1
                                    if st.session_state.get("open_manage_edit_tracked_playlist_id") == str(tp_id):
                                        st.session_state["open_manage_edit_tracked_playlist_id"] = None
                                    if st.session_state.get("open_manage_tracked_playlist_id") == str(tp_id):
                                        pass
                                        st.session_state["open_manage_tracked_playlist_id"] = None
                                    request_scroll("page_top", offset_px=0)
                                    time.sleep(0.8)
                                    st.rerun()

    if db_connection_ok and not tracked_playlists:
        card_container.info("No tracked playlists yet. Add one to begin collecting Saved Scans.")

    st.markdown("""
        </div>
    </div>
    """, unsafe_allow_html=True)
    consume_pending_scroll()


def render_back_to_tracked_playlists_button(tracked_playlist_id: str | None = None):
    if st.button("← Back to Tracked Playlists", key="back_to_tracked_playlists"):
        params = _get_query_params()
        params.pop("tp", None)
        _update_query_params(params)
        st.session_state.pop("selected_tracked_playlist_id", None)
        st.session_state.pop("_tp_resolve_retry", None)
        if tracked_playlist_id:
            st.session_state.pop(_latest_results_visibility_state_key(tracked_playlist_id), None)
            st.session_state.pop(f"show_basic_scan_{tracked_playlist_id}", None)
            st.session_state.pop(_basic_state_key(tracked_playlist_id, "show_results"), None)
        st.rerun()


def render_tracked_playlist_detail_page(detail: dict, latest_scan: dict | None):
    render_back_to_tracked_playlists_button(detail.get("id"))
    render_tracked_card_styles()

    render_scroll_anchor("dedicated_header")
    title = detail.get("display_name") or "Tracked Playlist"
    spotify_playlist_id = detail.get("spotify_playlist_id") or ""
    spotify_playlist_id_display = spotify_playlist_id or "N/A"
    playlist_url = detail.get("playlist_url") or ""
    playlist_owner = detail.get("playlist_owner")
    playlist_followers = detail.get("playlist_followers")
    songs_count = detail.get("songs_count")
    playlist_image_url = detail.get("playlist_image_url") or ""
    tracked_playlist_id = detail.get("id") or ""
    if not tracked_playlist_id:
        st.error("Tracked playlist missing. Please return to the list and re-open.")
        return
    detail_override_key = _basic_state_key(tracked_playlist_id, "detail_override")
    detail_override = st.session_state.get(detail_override_key)
    if detail_override:
        detail = detail_override
    target_countries = []
    for country in detail.get("target_countries") or []:
        if isinstance(country, str):
            cleaned = country.strip()
            if cleaned:
                target_countries.append(cleaned)
    target_keywords = []
    for keyword in detail.get("target_keywords") or []:
        if isinstance(keyword, str):
            cleaned = keyword.strip()
            if cleaned:
                target_keywords.append(cleaned)
    latest_scan_override_key = _basic_state_key(tracked_playlist_id, "latest_scan_override")
    latest_scan_override = st.session_state.get(latest_scan_override_key)
    if latest_scan_override:
        latest_scan = latest_scan_override

    refresh_message_key = _basic_state_key(tracked_playlist_id, "refresh_stats_message")
    refresh_error_key = _basic_state_key(tracked_playlist_id, "refresh_stats_error")
    refresh_in_progress_key = _basic_state_key(tracked_playlist_id, "refresh_stats_in_progress")
    if refresh_message_key in st.session_state:
        st.success(st.session_state.pop(refresh_message_key))
    if refresh_error_key in st.session_state:
        st.error(st.session_state.pop(refresh_error_key))

    if refresh_in_progress_key not in st.session_state:
        st.session_state[refresh_in_progress_key] = False

    resolved_stats = _resolve_latest_playlist_stats(
        playlist_owner=playlist_owner,
        playlist_followers=playlist_followers,
        songs_count=songs_count,
        playlist_last_track_added_at=detail.get("playlist_last_track_added_at"),
        metadata_refreshed_at=detail.get("metadata_refreshed_at"),
        scan_owner=latest_scan.get("owner") if latest_scan else None,
        scan_followers=latest_scan.get("followers") if latest_scan else None,
        scan_songs_count=latest_scan.get("songs_count") if latest_scan else None,
        scan_last_track_added_at=latest_scan.get("last_track_added_at") if latest_scan else None,
        scan_snapshot_at=latest_scan.get("snapshot_at") if latest_scan else None,
        scan_completed_at=(latest_scan.get("finished_at") or latest_scan.get("created_at")) if latest_scan else None,
    )

    followers_line = format_stat_value(resolved_stats.get("followers")) if resolved_stats.get("followers") is not None else None
    if followers_line and str(followers_line).upper() in {"NA", "N/A"}:
        followers_line = "—"
    tracks_line = format_stat_value(resolved_stats.get("songs_count")) if resolved_stats.get("songs_count") is not None else None
    if tracks_line and str(tracks_line).upper() in {"NA", "N/A"}:
        tracks_line = "—"
    last_updated_display = format_relative_time_or_dash(resolved_stats.get("last_track_added_at"))
    scanned_display = format_relative_time_or_dash(resolved_stats.get("scanned_at"))
    show_refresh_pill = bool(playlist_url)
    header_card_html = build_tracked_playlist_card_html(
        title=title,
        owner=resolved_stats.get("owner"),
        followers=followers_line,
        songs_count=tracks_line,
        last_updated=last_updated_display,
        scanned_display=scanned_display,
        playlist_image_url=playlist_image_url,
        spotify_playlist_id=spotify_playlist_id_display,
        variant="detail",
        card_href=None,
        card_target=None,
        playlist_url=playlist_url,
    )
    header_block = st.container()
    with header_block:
        render_tracked_playlist_card(header_card_html)

    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)

    refresh_clicked = False
    if show_refresh_pill:
        refresh_clicked = st.button(
            "Refresh Stats",
            key=f"refresh_stats_toggle_{tracked_playlist_id}",
            disabled=st.session_state[refresh_in_progress_key],
            help="Refresh playlist metadata without running a scan.",
        )

    if refresh_clicked:
        st.session_state[refresh_in_progress_key] = True
        try:
            with st.spinner("Refreshing playlist stats..."):
                success, message, refreshed_detail = refresh_tracked_playlist_metadata(tracked_playlist_id)
            if success:
                detail = refreshed_detail or detail
                st.session_state[detail_override_key] = detail
                st.session_state["tracked_playlists_refresh_token"] += 1
                st.session_state[refresh_message_key] = "Playlist stats refreshed."
            else:
                st.session_state[refresh_error_key] = message
        finally:
            st.session_state[refresh_in_progress_key] = False
        st.rerun()

    st.divider()
    st.subheader("Latest Scan")
    latest_results_state_key = _latest_results_state_key(tracked_playlist_id)
    latest_results_visibility_key = _latest_results_visibility_state_key(tracked_playlist_id)
    if latest_results_visibility_key not in st.session_state:
        st.session_state[latest_results_visibility_key] = False
    latest_results_data = st.session_state.get(latest_results_state_key)
    latest_results_rendered = False
    if latest_scan:
        if latest_results_data and latest_results_data.get("scan_id") != latest_scan.get("id"):
            latest_results_data = None
            st.session_state.pop(latest_results_state_key, None)
            st.session_state[latest_results_visibility_key] = False

        created_display, _ = _format_datetime_display(latest_scan.get("created_at"))
        countries_count = len(latest_scan.get("countries") or [])
        keywords_count = len(latest_scan.get("keywords") or [])
        total_requests = latest_scan.get("total_requests")
        follower_value = resolved_stats.get("followers")
        if follower_value is None:
            followers_display = "—"
        elif isinstance(follower_value, (int, float)):
            followers_display = f"{int(follower_value):,}" if isinstance(follower_value, int) else f"{follower_value:,}"
        else:
            followers_display = str(follower_value)
        latest_card = st.container(border=True)
        with latest_card:
            st.write(f"Last run at: {created_display}")
            col_followers, col_countries, col_keywords, col_requests = st.columns(4)
            col_followers.metric("Followers", followers_display)
            col_countries.metric("Countries", countries_count)
            col_keywords.metric("Keywords", keywords_count)
            col_requests.metric("Total requests", total_requests if total_requests is not None else "N/A")

            view_error = st.empty()
            if st.button("View Latest Results", key=f"view_latest_{tracked_playlist_id}"):
                st.session_state[latest_results_visibility_key] = True
                try:
                    with st.spinner("Loading latest results..."):
                        scan_results_rows = fetch_scan_results_for_scan(str(latest_scan.get("id")))
                    if scan_results_rows is None:
                        view_error.error("Latest results could not be loaded. Please try again later.")
                        st.session_state[latest_results_visibility_key] = False
                    elif len(scan_results_rows) == 0:
                        view_error.info("Latest scan has no results yet.")
                        st.session_state.pop(latest_results_state_key, None)
                        st.session_state[latest_results_visibility_key] = False
                        latest_results_data = None
                    else:
                        payload = build_results_payload_from_scan(detail, latest_scan, scan_results_rows)
                        if payload:
                            st.session_state[latest_results_state_key] = payload
                            latest_results_data = payload
                            request_scroll("scan_results", offset_px=-12)
                        else:
                            view_error.info("Latest scan has no results to display.")
                            st.session_state[latest_results_visibility_key] = False
                except Exception as exc:
                    logger.error("Loading latest results failed: %s", exc)
                    view_error.error("Latest results could not be loaded. Please try again.")
                    st.session_state[latest_results_visibility_key] = False
    else:
        st.info("No scans yet.")
        st.session_state.pop(latest_results_state_key, None)
        st.session_state[latest_results_visibility_key] = False

    if latest_results_data and st.session_state.get(latest_results_visibility_key):
        render_scroll_anchor("scan_results")
        render_results(latest_results_data, tracked_playlist_id=tracked_playlist_id, show_hide_controls=True)
        latest_results_rendered = True

    st.divider()
    basic_scan_visibility_key = f"show_basic_scan_{tracked_playlist_id}"
    is_searching_key = _basic_state_key(tracked_playlist_id, "is_searching")
    run_search_key = _basic_state_key(tracked_playlist_id, "run_search")
    keywords_key = _basic_state_key(tracked_playlist_id, "keywords")
    countries_selected_key = _basic_state_key(tracked_playlist_id, "countries_selected")
    prev_countries_key = _basic_state_key(tracked_playlist_id, "prev_countries")
    search_payload_key = _basic_state_key(tracked_playlist_id, "search_payload")
    reset_after_search_key = _basic_state_key(tracked_playlist_id, "reset_after_search")
    last_results_key = _basic_state_key(tracked_playlist_id, "last_results")
    playlist_display_key = _basic_state_key(tracked_playlist_id, "playlist_url_display")
    run_button_key = _basic_state_key(tracked_playlist_id, "run_basic_scan")
    basic_results_visibility_key = _basic_state_key(tracked_playlist_id, "show_results")

    if basic_scan_visibility_key not in st.session_state:
        st.session_state[basic_scan_visibility_key] = False
    if is_searching_key not in st.session_state:
        st.session_state[is_searching_key] = False
    if run_search_key not in st.session_state:
        st.session_state[run_search_key] = False
    if search_payload_key not in st.session_state:
        st.session_state[search_payload_key] = None
    if reset_after_search_key not in st.session_state:
        st.session_state[reset_after_search_key] = False
    if basic_results_visibility_key not in st.session_state:
        st.session_state[basic_results_visibility_key] = True

    basic_results_data = st.session_state.get(last_results_key)
    basic_results_visible = st.session_state.get(basic_results_visibility_key, True)

    header_container = st.container()
    with header_container:
        title_col, button_col = st.columns([3, 1])
        with title_col:
            st.subheader("Basic Scan (Manual)")
        with button_col:
            toggle_label = "Hide Basic Scan" if st.session_state[basic_scan_visibility_key] else "Open Basic Scan"
            toggle_disabled = st.session_state[is_searching_key]
            should_render_toggle = not (
                basic_results_data and basic_results_visible and st.session_state[basic_scan_visibility_key]
            )
            if should_render_toggle:
                if st.button(
                    toggle_label,
                    key=_basic_state_key(tracked_playlist_id, "toggle_basic_scan"),
                    disabled=toggle_disabled,
                ):
                    st.session_state[basic_scan_visibility_key] = not st.session_state[basic_scan_visibility_key]
                    st.rerun()

    if not st.session_state[basic_scan_visibility_key]:
        st.caption("Run a manual scan for this playlist.")
    else:
        st.caption("Scan this tracked playlist with its stored target keywords and target countries.")

        if not playlist_url or not spotify_playlist_id:
            st.warning("Playlist details missing. Please re-add this tracked playlist to run Basic Scan.")
            st.stop()

        country_map, country_names = get_country_selection_options()
        merged_country_map = dict(country_map)
        for target_country in target_countries:
            if target_country in COUNTRIES and target_country not in merged_country_map:
                merged_country_map[target_country] = COUNTRIES[target_country]
        country_map = merged_country_map
        country_options = sorted(set(country_names + target_countries))
        default_country_selection = list(target_countries)

        if keywords_key not in st.session_state:
            st.session_state[keywords_key] = list(target_keywords)
        if countries_selected_key not in st.session_state:
            st.session_state[countries_selected_key] = default_country_selection
        if prev_countries_key not in st.session_state:
            st.session_state[prev_countries_key] = st.session_state[countries_selected_key]
        if st.session_state[countries_selected_key] != default_country_selection:
            st.session_state[countries_selected_key] = default_country_selection
        if st.session_state[prev_countries_key] != st.session_state[countries_selected_key]:
            st.session_state[prev_countries_key] = st.session_state[countries_selected_key]

        if st.session_state.get(reset_after_search_key):
            st.session_state[countries_selected_key] = default_country_selection
            st.session_state[keywords_key] = list(target_keywords)
            st.session_state[prev_countries_key] = st.session_state[countries_selected_key]
            st.session_state[reset_after_search_key] = False

        if st.session_state.get(keywords_key) != target_keywords:
            st.session_state[keywords_key] = list(target_keywords)

        def handle_country_change():
            if st.session_state[is_searching_key]:
                st.stop()
            current = st.session_state.get(countries_selected_key, [])
            prev = st.session_state.get(prev_countries_key, [])
            if len(current) > 5:
                st.session_state[countries_selected_key] = prev
                st.rerun()
            st.session_state[prev_countries_key] = st.session_state[countries_selected_key]

        def start_basic_search():
            if st.session_state[is_searching_key]:
                return
            if not target_keywords:
                return
            if not st.session_state.get(countries_selected_key):
                return
            request_scroll("scan_running", offset_px=-12)
            payload = {
                "playlist_url": normalize_spotify_playlist_url(playlist_url),
                "keywords": [kw.strip() for kw in target_keywords if kw.strip()],
                "countries": list(st.session_state.get(countries_selected_key, [])),
            }
            st.session_state[search_payload_key] = payload
            st.session_state[is_searching_key] = True
            st.session_state[run_search_key] = True

        inputs_disabled = st.session_state[is_searching_key]

        if inputs_disabled:
            st.info("Scan in progress… please wait until it completes.")

        st.text_input(
            "Playlist URL",
            value=normalize_spotify_playlist_url(playlist_url),
            key=playlist_display_key,
            disabled=True,
        )

        st.subheader("Target Keywords")
        if not target_keywords:
            st.warning("No target keywords defined for this playlist.")
        else:
            chips = "".join(f'<span class="tracked-manage-chip">{escape(keyword)}</span>' for keyword in target_keywords)
            st.markdown(f'<div class="tracked-manage-chip-row">{chips}</div>', unsafe_allow_html=True)

        st.subheader("Target Countries")
        if not default_country_selection:
            st.warning("No target countries defined for this playlist.")

        st.multiselect(
            "Countries (from tracked playlist)",
            options=country_options,
            key=countries_selected_key,
            on_change=handle_country_change,
            disabled=True,
            help="Target countries are defined when the playlist is added. They cannot be changed here.",
        )

        run_button_disabled = (
            inputs_disabled or not st.session_state.get(countries_selected_key) or not target_keywords
        )
        st.button("Run Basic Scan", key=run_button_key, disabled=run_button_disabled, on_click=start_basic_search)

    render_scroll_anchor("scan_running")
    if st.session_state.get(run_search_key) and st.session_state.get(search_payload_key):
        try:
            payload = st.session_state[search_payload_key] or {}
            payload_keywords = payload.get("keywords") or []
            payload_countries = payload.get("countries") or []
            payload_playlist_url = (payload.get("playlist_url") or "").strip()

            if not payload_playlist_url:
                st.error("Playlist URL is missing.")
                st.session_state[run_search_key] = False
                st.session_state[is_searching_key] = False
                st.session_state[search_payload_key] = None
                st.stop()

            if not payload_keywords:
                st.error("Please add at least one keyword.")
                st.session_state[run_search_key] = False
                st.session_state[is_searching_key] = False
                st.session_state[search_payload_key] = None
                st.stop()

            country_pairs = [
                (country, country_map.get(country)) for country in payload_countries if country_map.get(country)
            ]
            if not country_pairs:
                st.error("Please select at least one country.")
                st.session_state[run_search_key] = False
                st.session_state[is_searching_key] = False
                st.session_state[search_payload_key] = None
                st.stop()

            if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
                st.error("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are missing from .env.")
                st.session_state[run_search_key] = False
                st.session_state[is_searching_key] = False
                st.session_state[search_payload_key] = None
                st.stop()

            playlist_id = spotify_playlist_id or extract_playlist_id(payload_playlist_url)
            if not playlist_id:
                st.error("Invalid playlist link. Please verify the playlist URL for this tracked playlist.")
                st.session_state[run_search_key] = False
                st.session_state[is_searching_key] = False
                st.session_state[search_payload_key] = None
                st.stop()

            try:
                token = get_access_token()
            except Exception as e:
                st.error(f"Failed to obtain token: {e}")
                st.session_state[run_search_key] = False
                st.session_state[is_searching_key] = False
                st.session_state[search_payload_key] = None
                st.stop()
            st.session_state["access_token"] = token

            with st.spinner("Running searches..."):
                playlist_meta_cache: dict[str, dict] = {}
                playlist_header = {"title": None, "url": None, "image": ""}
                if playlist_id:
                    try:
                        fetch_playlist_details([playlist_id], token, playlist_meta_cache)
                    except Exception:
                        playlist_meta_cache[playlist_id] = {
                            "playlist_name": "Playlist",
                            "playlist_url": f"https://open.spotify.com/playlist/{playlist_id}",
                            "playlist_description": "",
                            "playlist_followers": None,
                            "songs_count": None,
                            "playlist_last_track_added_at": None,
                            "playlist_image": "",
                            "playlist_owner": None,
                        }
                    cached_own = playlist_meta_cache.get(playlist_id, {})
                    playlist_header = {
                        "title": cached_own.get("playlist_name") or "Playlist",
                        "url": cached_own.get("playlist_url") or f"https://open.spotify.com/playlist/{playlist_id}",
                        "image": cached_own.get("playlist_image") or "",
                    }

                now_dt = datetime.now()
                now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
                now_iso = now_dt.isoformat(timespec="seconds")
                trimmed_keywords = payload_keywords

                progress = st.progress(0)
                status = st.empty()
                total_steps = len(trimmed_keywords) * len(country_pairs)
                current_step = 0

                summary_rows = []
                download_rows = []
                keyword_results = []
                scan_results_records: list[tuple] = []

                for trimmed_keyword in trimmed_keywords:
                    per_keyword_country_results = []
                    for selected_country, market in country_pairs:
                        country_results, actual_count = search_playlists_with_pagination(
                            trimmed_keyword, market, token, target_count=RESULTS_LIMIT
                        )
                        per_keyword_country_results.append(
                            {
                                "country": selected_country,
                                "market": market,
                                "results": country_results,
                                "actual_count": actual_count,
                                "keyword": trimmed_keyword,
                            }
                        )
                        current_step += 1
                        if total_steps:
                            progress.progress(current_step / total_steps)
                        status.write(f"Processing: keyword='{trimmed_keyword}' country='{selected_country}'")
                    keyword_results.append({"keyword": trimmed_keyword, "countries": per_keyword_country_results})

                for keyword_data in keyword_results:
                    kw = keyword_data["keyword"]
                    for country_data in keyword_data["countries"]:
                        selected_country = country_data["country"]
                        market = country_data["market"]
                        results = country_data["results"]
                        actual_count = country_data["actual_count"]
                        playlist_ids_to_fetch = [
                            p.get("id")
                            for p in results
                            if p.get("id") and not p.get("placeholder", False) and p.get("id") not in playlist_meta_cache
                        ]
                        fetch_playlist_details(playlist_ids_to_fetch, token, playlist_meta_cache)

                        found_rank = None
                        for i, p in enumerate(results, start=1):
                            pid = p.get("id", "")
                            is_placeholder = p.get("placeholder", False)
                            cached_meta = playlist_meta_cache.get(pid) or {}
                            name = cached_meta.get("playlist_name", p.get("name", "-"))
                            url = cached_meta.get("playlist_url", (p.get("external_urls") or {}).get("spotify", ""))
                            description = cached_meta.get("playlist_description", p.get("description", ""))
                            followers = None if is_placeholder else cached_meta.get("playlist_followers")
                            songs_count = None if is_placeholder else cached_meta.get("songs_count")
                            playlist_last_track_added_at = (
                                None if is_placeholder else cached_meta.get("playlist_last_track_added_at")
                            )
                            playlist_owner = cached_meta.get("playlist_owner")
                            if not playlist_owner:
                                owner_info = p.get("owner") or {}
                                playlist_owner = owner_info.get("display_name") or owner_info.get("id")
                            is_your_playlist = bool(playlist_id and pid == playlist_id)
                            download_rows.append(
                                {
                                    "searched_at": now_str,
                                    "keyword": kw,
                                    "country": selected_country,
                                    "rank": i,
                                    "playlist_name": name,
                                    "playlist_owner": playlist_owner,
                                    "playlist_url": url,
                                    "playlist_description": description,
                                    "is_your_playlist": is_your_playlist,
                                    "playlist_followers": followers,
                                    "songs_count": songs_count,
                                    "playlist_last_track_added_at": playlist_last_track_added_at,
                                }
                            )
                            if playlist_id and not is_placeholder:
                                scan_results_records.append(
                                    {
                                        "searched_at": now_str,
                                        "keyword": kw,
                                        "country": selected_country,
                                        "rank": i,
                                        "playlist_id": pid or None,
                                        "playlist_name": name,
                                        "playlist_owner": playlist_owner,
                                        "playlist_followers": followers,
                                        "songs_count": songs_count,
                                        "playlist_last_track_added_at": playlist_last_track_added_at,
                                        "playlist_description": description,
                                        "playlist_url": url,
                                        "is_your_playlist": is_your_playlist,
                                    }
                                )

                            if playlist_id and pid == playlist_id and found_rank is None:
                                found_rank = i

                        if actual_count == 0:
                            summary_rows.append(
                                f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> no results found."
                            )
                        elif playlist_id:
                            if found_rank:
                                summary_rows.append(
                                    f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> ranking: #{found_rank}"
                                )
                            else:
                                summary_rows.append(
                                    f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> your playlist is not within the first {RESULTS_LIMIT} results."
                                )
                        else:
                            summary_rows.append(
                                f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> {actual_count} results listed."
                            )

                persist_scan(
                    playlist_url=payload_playlist_url,
                    keywords=trimmed_keywords,
                    countries=[name for name, _ in country_pairs],
                    results=scan_results_records,
                    searched_at=now_str,
                    tracked_playlist_id=tracked_playlist_id,
                )

                st.session_state[last_results_key] = {
                    "searched_at_str": now_str,
                    "searched_at_iso": now_iso,
                    "summary_rows": summary_rows,
                    "download_rows": download_rows,
                    "keyword_results": keyword_results,
                    "playlist_meta_cache": playlist_meta_cache,
                    "playlist_id": playlist_id,
                    "playlist_title": playlist_header.get("title") if playlist_id else None,
                    "playlist_link": playlist_header.get("url") if playlist_id else None,
                    "playlist_image_url": playlist_header.get("image") if playlist_id else "",
                }
                st.session_state[basic_results_visibility_key] = True
                request_scroll("scan_results", offset_px=-12)

                refreshed_latest_scan = fetch_latest_scan_for_tracked_playlist(tracked_playlist_id)
                if refreshed_latest_scan:
                    st.session_state[latest_scan_override_key] = refreshed_latest_scan

            st.session_state[reset_after_search_key] = True
            st.rerun()
        finally:
            st.session_state[run_search_key] = False
            st.session_state[is_searching_key] = False
            st.session_state[search_payload_key] = None

    if basic_results_data and basic_results_visible:
        render_scroll_anchor("scan_results")
        hide_controls_config = {
            "state_key": basic_results_visibility_key,
            "state_value": False,
            "button_key": _basic_state_key(tracked_playlist_id, "hide_results"),
            "label": "Hide Results",
        }
        render_results(
            basic_results_data,
            tracked_playlist_id=tracked_playlist_id,
            hide_controls_config=hide_controls_config,
        )
    elif basic_results_data:
        if st.button(
            "Show Results",
            key=_basic_state_key(tracked_playlist_id, "show_results_button"),
        ):
            st.session_state[basic_results_visibility_key] = True
            request_scroll("scan_results", offset_px=-12)
            st.rerun()
    elif latest_results_data and not latest_results_rendered and st.session_state.get(latest_results_visibility_key):
        render_scroll_anchor("scan_results")
        render_results(latest_results_data, tracked_playlist_id=tracked_playlist_id, show_hide_controls=True)
    elif not (latest_results_data or latest_results_rendered):
        st.info("Use the Basic Scan to manually check this playlist’s ranking for specific keywords and countries.")

    st.divider()
    st.subheader("Saved Scans")
    st.caption("Coming later")
    consume_pending_scroll()


st.set_page_config(page_title="Spotify Playlist Search Rank", layout="centered")
st.title("Spotify Playlist Search Rank Checker")
render_scroll_anchor("page_top")

if "is_searching" not in st.session_state:
    st.session_state["is_searching"] = False
if "run_search" not in st.session_state:
    st.session_state["run_search"] = False
if "search_payload" not in st.session_state:
    st.session_state["search_payload"] = None
if "reset_after_search" not in st.session_state:
    st.session_state["reset_after_search"] = False
if "show_add_playlist_form" not in st.session_state:
    st.session_state["show_add_playlist_form"] = False
if "add_in_progress" not in st.session_state:
    st.session_state["add_in_progress"] = False
if "tracked_playlists_refresh_token" not in st.session_state:
    st.session_state["tracked_playlists_refresh_token"] = 0
if "add_playlist_input_version" not in st.session_state:
    st.session_state["add_playlist_input_version"] = 0
if "add_playlist_error" not in st.session_state:
    st.session_state["add_playlist_error"] = None

db_connection_ok = get_db_connection_status()
tracked_playlists_db: list[dict] = fetch_tracked_playlists_from_db(st.session_state["tracked_playlists_refresh_token"])

handle_history_export_if_requested()

with st.sidebar:
    if DB_STATUS_INDICATOR_ENABLED:
        db_status = "OK" if db_connection_ok else "ERROR"
        st.caption(f"DB: {db_status}")

if "playlist_url_input" not in st.session_state:
    st.session_state["playlist_url_input"] = ""
if "last_seen" not in st.session_state:
    st.session_state["last_seen"] = {}
if "keywords" not in st.session_state:
    st.session_state["keywords"] = []

debug_ui_enabled = _is_debug_ui_enabled()
if debug_ui_enabled:
    tracked_state_snapshot = {
        "show_add_playlist_form": st.session_state.get("show_add_playlist_form"),
        "is_searching": st.session_state.get("is_searching"),
        "has_last_results": "last_results" in st.session_state,
    }
    logger.info("DEBUG_UI: session_state snapshot (tracked playlists): %s", tracked_state_snapshot)

if st.session_state.get("reset_after_search"):
    st.session_state["countries_selected"] = []
    st.session_state["keywords"] = []
    st.session_state["playlist_url_input"] = ""
    st.session_state["prev_countries"] = []
    st.session_state["reset_after_search"] = False

raw_playlist_url = st.session_state.get("playlist_url_input", "")
if isinstance(raw_playlist_url, str) and "?" in raw_playlist_url:
    st.session_state["playlist_url_input"] = raw_playlist_url.split("?", 1)[0]

inputs_disabled = st.session_state["is_searching"]

if not db_connection_ok:
    st.info("Database connection not available. Tracking playlists requires a database connection.")

query_params = _flatten_query_params(_get_query_params())
raw_tp_value = query_params.get("tp")
tp_param_value = _normalize_uuid_param(raw_tp_value)
session_tp_value = _normalize_uuid_param(st.session_state.get("selected_tracked_playlist_id"))

if raw_tp_value is not None and not tp_param_value:
    st.error("Invalid tracked playlist id. Please use a valid link from the list.")
    render_back_to_tracked_playlists_button()
    st.stop()

tp_value = resolve_tracked_playlist_id()
if not tp_value:
    if session_tp_value is None and st.session_state.get("selected_tracked_playlist_id"):
        st.session_state.pop("selected_tracked_playlist_id", None)
    if (tp_param_value or session_tp_value) and not st.session_state.get("_tp_resolve_retry"):
        st.session_state["_tp_resolve_retry"] = True
        st.rerun()
    st.session_state.pop("_tp_resolve_retry", None)
elif tp_value:
    st.session_state.pop("_tp_resolve_retry", None)
    if not db_connection_ok:
        st.error("Database connection not available. Please try again later.")
        render_back_to_tracked_playlists_button()
        st.stop()
    detail = fetch_tracked_playlist_by_id(tp_value)
    if not detail:
        st.error("Tracked playlist not found. It may have been removed.")
        render_back_to_tracked_playlists_button()
        st.stop()
    latest_scan = fetch_latest_scan_for_tracked_playlist(tp_value)
    render_tracked_playlist_detail_page(detail, latest_scan)
    st.stop()

render_tracked_playlists_section(tracked_playlists_db, debug_enabled=debug_ui_enabled, db_connection_ok=db_connection_ok)
st.stop()

country_map, country_names = get_country_selection_options()

default_country_selection = ["Brazil"] if "Brazil" in country_names else country_names[:1]
if "countries_selected" not in st.session_state:
    st.session_state["countries_selected"] = default_country_selection
if "prev_countries" not in st.session_state:
    st.session_state["prev_countries"] = st.session_state["countries_selected"]


def handle_country_change():
    if st.session_state["is_searching"]:
        st.stop()
    current = st.session_state.get("countries_selected", [])
    prev = st.session_state.get("prev_countries", [])
    if len(current) > 10:
        st.session_state["countries_selected"] = prev
        st.rerun()
    st.session_state["prev_countries"] = st.session_state["countries_selected"]


def start_search():
    if st.session_state["is_searching"]:
        return
    request_scroll("core_running", offset_px=-12)
    payload = {
        "playlist_url": (st.session_state.get("playlist_url_input") or "").strip(),
        "keywords": [kw.strip() for kw in st.session_state.get("keywords", []) if kw.strip()],
        "countries": list(st.session_state.get("countries_selected", [])),
    }
    st.session_state["search_payload"] = payload
    st.session_state["is_searching"] = True
    st.session_state["run_search"] = True


if inputs_disabled:
    st.info("Scan in progress… please wait until it completes.")
else:
    playlist_url = st.text_input(
        "Playlist link (optional)",
        key="playlist_url_input",
        disabled=inputs_disabled,
    )

    st.subheader("Keywords (max 10)")
    with st.form("keyword_form", clear_on_submit=True):
        keyword_input = st.text_input(
            "Add keyword",
            key="keyword_input",
            disabled=inputs_disabled,
        )
        add_keyword = st.form_submit_button(
            "Add",
            disabled=inputs_disabled or len(st.session_state["keywords"]) >= 10,
        )
        if add_keyword:
            if st.session_state["is_searching"]:
                st.stop()
            trimmed = (keyword_input or "").strip()
            if trimmed and trimmed not in st.session_state["keywords"] and len(st.session_state["keywords"]) < 10:
                st.session_state["keywords"].append(trimmed)

    st.markdown(
        """
        <style>
        .keyword-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            align-items: center;
        }
        .keyword-chip-row .stButton {
            margin: 0;
        }
        .keyword-chip-row .stButton > button {
            background: #ff4d4d;
            color: #fff;
            border: none;
            border-radius: 999px;
            padding: 6px 12px;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            width: auto;
            min-width: 0;
            height: auto;
            line-height: 1.2;
            font-weight: 600;
            cursor: pointer;
        }
        .keyword-chip-row .stButton > button:hover {
            background: #e04343;
        }
        .keyword-chip-row .stButton > button:focus {
            outline: none;
            box-shadow: 0 0 0 2px rgba(255, 77, 77, 0.25);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.session_state["keywords"]:
        chip_container = st.container()
        chip_container.markdown('<div class="keyword-chip-row">', unsafe_allow_html=True)
        for idx, kw in enumerate(st.session_state["keywords"]):
            if chip_container.button(
                f"{kw} ✕",
                key=f"kw_chip_{idx}",
                help=f"Remove '{kw}'",
                disabled=inputs_disabled,
            ):
                if st.session_state["is_searching"]:
                    st.stop()
                st.session_state["keywords"].pop(idx)
                st.rerun()
        chip_container.markdown("</div>", unsafe_allow_html=True)

    st.subheader("Target Countries")

    st.multiselect(
        "Countries (max 10)",
        options=country_names,
        key="countries_selected",
        on_change=handle_country_change,
        disabled=inputs_disabled,
    )

    st.button("Search", disabled=inputs_disabled, on_click=start_search)


render_scroll_anchor("core_running")
if st.session_state.get("run_search") and st.session_state.get("search_payload"):
    try:
        payload = st.session_state["search_payload"] or {}
        payload_keywords = payload.get("keywords") or []
        payload_countries = payload.get("countries") or []
        playlist_url = (payload.get("playlist_url") or "").strip()

        if not payload_keywords:
            st.error("Please add at least one keyword.")
            st.session_state["run_search"] = False
            st.session_state["is_searching"] = False
            st.session_state["search_payload"] = None
            st.stop()

        country_pairs = [(country, country_map.get(country)) for country in payload_countries if country_map.get(country)]
        if not country_pairs:
            st.error("Please select at least one country.")
            st.session_state["run_search"] = False
            st.session_state["is_searching"] = False
            st.session_state["search_payload"] = None
            st.stop()

        if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
            st.error("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are missing from .env.")
            st.session_state["run_search"] = False
            st.session_state["is_searching"] = False
            st.session_state["search_payload"] = None
            st.stop()

        playlist_id = None
        if playlist_url.strip():
            playlist_id = extract_playlist_id(playlist_url)
            if not playlist_id:
                st.error("Invalid playlist link. (This field is optional; you may leave it blank.)")
                st.session_state["run_search"] = False
                st.session_state["is_searching"] = False
                st.session_state["search_payload"] = None
                st.stop()

        try:
            token = get_access_token()
        except Exception as e:
            st.error(f"Failed to obtain token: {e}")
            st.session_state["run_search"] = False
            st.session_state["is_searching"] = False
            st.session_state["search_payload"] = None
            st.stop()
        st.session_state["access_token"] = token

        with st.spinner("Running searches..."):
            playlist_meta_cache: dict[str, dict] = {}
            playlist_header = {"title": None, "url": None, "image": ""}
            if playlist_id:
                try:
                    fetch_playlist_details([playlist_id], token, playlist_meta_cache)
                except Exception:
                    playlist_meta_cache[playlist_id] = {
                        "playlist_name": "Playlist",
                        "playlist_url": f"https://open.spotify.com/playlist/{playlist_id}",
                        "playlist_description": "",
                        "playlist_followers": None,
                        "songs_count": None,
                        "playlist_last_track_added_at": None,
                        "playlist_image": "",
                        "playlist_owner": None,
                    }
                cached_own = playlist_meta_cache.get(playlist_id, {})
                playlist_header = {
                    "title": cached_own.get("playlist_name") or "Playlist",
                    "url": cached_own.get("playlist_url") or f"https://open.spotify.com/playlist/{playlist_id}",
                    "image": cached_own.get("playlist_image") or "",
                }

            now_dt = datetime.now()
            now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
            now_iso = now_dt.isoformat(timespec="seconds")
            trimmed_keywords = payload_keywords

            progress = st.progress(0)
            status = st.empty()
            total_steps = len(trimmed_keywords) * len(country_pairs)
            current_step = 0

            summary_rows = []
            download_rows = []
            keyword_results = []
            scan_results_records: list[tuple] = []

            for trimmed_keyword in trimmed_keywords:
                per_keyword_country_results = []
                for selected_country, market in country_pairs:
                    country_results, actual_count = search_playlists_with_pagination(
                        trimmed_keyword, market, token, target_count=RESULTS_LIMIT
                    )
                    per_keyword_country_results.append(
                        {
                            "country": selected_country,
                            "market": market,
                            "results": country_results,
                            "actual_count": actual_count,
                            "keyword": trimmed_keyword,
                        }
                    )
                    current_step += 1
                    if total_steps:
                        progress.progress(current_step / total_steps)
                    status.write(f"Processing: keyword='{trimmed_keyword}' country='{selected_country}'")
                keyword_results.append({"keyword": trimmed_keyword, "countries": per_keyword_country_results})

            for keyword_data in keyword_results:
                kw = keyword_data["keyword"]
                for country_data in keyword_data["countries"]:
                    selected_country = country_data["country"]
                    market = country_data["market"]
                    results = country_data["results"]
                    actual_count = country_data["actual_count"]
                    playlist_ids_to_fetch = [
                        p.get("id")
                        for p in results
                        if p.get("id") and not p.get("placeholder", False) and p.get("id") not in playlist_meta_cache
                    ]
                    fetch_playlist_details(playlist_ids_to_fetch, token, playlist_meta_cache)

                    found_rank = None
                    for i, p in enumerate(results, start=1):
                        pid = p.get("id", "")
                        is_placeholder = p.get("placeholder", False)
                        cached_meta = playlist_meta_cache.get(pid) or {}
                        name = cached_meta.get("playlist_name", p.get("name", "-"))
                        url = cached_meta.get("playlist_url", (p.get("external_urls") or {}).get("spotify", ""))
                        description = cached_meta.get("playlist_description", p.get("description", ""))
                        followers = None if is_placeholder else cached_meta.get("playlist_followers")
                        songs_count = None if is_placeholder else cached_meta.get("songs_count")
                        playlist_last_track_added_at = None if is_placeholder else cached_meta.get("playlist_last_track_added_at")
                        playlist_owner = cached_meta.get("playlist_owner")
                        if not playlist_owner:
                            owner_info = p.get("owner") or {}
                            playlist_owner = owner_info.get("display_name") or owner_info.get("id")
                        is_your_playlist = bool(playlist_id and pid == playlist_id)
                        download_rows.append(
                            {
                                "searched_at": now_str,
                                "keyword": kw,
                                "country": selected_country,
                                "rank": i,
                                "playlist_name": name,
                                "playlist_owner": playlist_owner,
                                "playlist_url": url,
                                "playlist_description": description,
                                "is_your_playlist": is_your_playlist,
                                "playlist_followers": followers,
                                "songs_count": songs_count,
                                "playlist_last_track_added_at": playlist_last_track_added_at,
                            }
                        )
                        if playlist_id and not is_placeholder:
                            scan_results_records.append(
                                {
                                    "searched_at": now_str,
                                    "keyword": kw,
                                    "country": selected_country,
                                    "rank": i,
                                    "playlist_id": pid or None,
                                    "playlist_name": name,
                                    "playlist_owner": playlist_owner,
                                    "playlist_followers": followers,
                                    "songs_count": songs_count,
                                    "playlist_last_track_added_at": playlist_last_track_added_at,
                                    "playlist_description": description,
                                    "playlist_url": url,
                                    "is_your_playlist": is_your_playlist,
                                }
                            )

                        if playlist_id and pid == playlist_id and found_rank is None:
                            found_rank = i

                    if actual_count == 0:
                        summary_rows.append(
                            f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> no results found."
                        )
                    elif playlist_id:
                        if found_rank:
                            summary_rows.append(
                                f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> ranking: #{found_rank}"
                            )
                        else:
                            summary_rows.append(
                                f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> your playlist is not within the first {RESULTS_LIMIT} results."
                            )
                    else:
                        summary_rows.append(
                            f"{now_str} - keyword: '{kw}', country: {selected_country} ({market}) -> {actual_count} results listed."
                        )

            persist_scan(
                playlist_url=playlist_url,
                keywords=trimmed_keywords,
                countries=[name for name, _ in country_pairs],
                results=scan_results_records,
                searched_at=now_str,
            )

            st.session_state["last_results"] = {
                "searched_at_str": now_str,
                "searched_at_iso": now_iso,
                "summary_rows": summary_rows,
                "download_rows": download_rows,
                "keyword_results": keyword_results,
                "playlist_meta_cache": playlist_meta_cache,
                "playlist_id": playlist_id,
                "playlist_title": playlist_header.get("title") if playlist_id else None,
                "playlist_link": playlist_header.get("url") if playlist_id else None,
                "playlist_image_url": playlist_header.get("image") if playlist_id else "",
            }
            request_scroll("core_results", offset_px=-12)

        st.session_state["reset_after_search"] = True
        st.rerun()
    finally:
        st.session_state["run_search"] = False
        st.session_state["is_searching"] = False
        st.session_state["search_payload"] = None


if st.session_state.get("last_results"):
    render_scroll_anchor("core_results")
    render_results(st.session_state["last_results"])
consume_pending_scroll()
