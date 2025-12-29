from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from sqlalchemy.orm import Session

from app.core.db import SessionLocal

T = TypeVar("T")


def db_preflight_check(check_fn: Callable[[Session], T]) -> T:
    if SessionLocal is None:
        raise RuntimeError("DATABASE_URL not configured")

    session = SessionLocal()
    try:
        return check_fn(session)
    finally:
        session.close()
