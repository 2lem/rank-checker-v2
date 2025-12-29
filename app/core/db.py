import logging
import os
from contextvars import ContextVar

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# TEMP DEBUG: store the current request path for transaction lifecycle logging.
request_path_var: ContextVar[str | None] = ContextVar("request_path", default=None)


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

    if database_url.startswith("postgresql+psycopg2://"):
        engine_kwargs["connect_args"] = {"application_name": "rank-checker-v2-fastapi"}

    return create_engine(database_url, **engine_kwargs)


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
    logger.info(
        "TEMP HOTFIX get_db open path=%s session_id=%s",
        request_path_var.get(),
        id(db),
    )
    try:
        yield db
    finally:
        logger.info(
            "TEMP HOTFIX get_db close path=%s session_id=%s",
            request_path_var.get(),
            id(db),
        )
        db.close()
