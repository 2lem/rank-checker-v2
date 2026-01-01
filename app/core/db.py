import logging
import os
from contextvars import ContextVar

from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# TEMP DEBUG: store the current request path for transaction lifecycle logging.
request_path_var: ContextVar[str | None] = ContextVar("request_path", default=None)


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(_element, _compiler, **_kwargs):
    return "JSON"


@compiles(UUID, "sqlite")
def _compile_uuid_sqlite(_element, _compiler, **_kwargs):
    return "CHAR(36)"


def get_database_url() -> str | None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+psycopg2://", 1)
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return database_url


def _create_engine():
    database_url = get_database_url()
    if not database_url:
        return None
    engine_kwargs = {"pool_pre_ping": True}
    if database_url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}

    if database_url.startswith("postgresql+psycopg2://"):
        engine_kwargs["connect_args"] = {"application_name": "rank-checker-v2-fastapi"}

    return create_engine(database_url, **engine_kwargs)


def json_array_default_clause():
    database_url = get_database_url()
    is_sqlite = bool(database_url and database_url.startswith("sqlite"))
    default_expr = "'[]'" if is_sqlite else "'[]'::jsonb"
    return text(default_expr)


engine = _create_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None


def _log_transaction_event(conn, event_name: str) -> None:
    """TEMP DEBUG: Log transaction lifecycle events to trace idle transactions."""
    path = request_path_var.get()
    conn_id = None
    try:
        conn_id = id(conn.connection)
    except Exception:  # pragma: no cover - defensive logging
        conn_id = "unknown"
    logger.info(
        "TEMP DEBUG SQL TX %s conn_id=%s path=%s",
        event_name,
        conn_id,
        path,
    )


if engine:
    event.listen(engine, "begin", lambda conn: _log_transaction_event(conn, "BEGIN"))
    event.listen(engine, "commit", lambda conn: _log_transaction_event(conn, "COMMIT"))
    event.listen(engine, "rollback", lambda conn: _log_transaction_event(conn, "ROLLBACK"))


def get_db():
    if SessionLocal is None:
        raise RuntimeError("DATABASE_URL not configured")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def provide_db_session():
    """Dependency alias to avoid leaking internal helper names into route modules."""
    yield from get_db()
