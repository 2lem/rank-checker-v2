from __future__ import annotations

from typing import Iterable
from typing import Optional

from pydantic import BaseModel, Field, validator


def _normalize_list(values: Iterable[str] | None, *, upper: bool = False) -> list[str]:
    if not values:
        return []
    cleaned: list[str] = []
    for entry in values:
        value = (entry or "").strip()
        if not value:
            continue
        if upper:
            value = value.upper()
        if value not in cleaned:
            cleaned.append(value)
    return cleaned


class ManualScanCreate(BaseModel):
    playlist_url: Optional[str] = None
    target_keywords: list[str] = Field(default_factory=list)
    target_countries: list[str] = Field(default_factory=list)

    @validator("playlist_url")
    def _strip_playlist_url(cls, value: Optional[str]) -> Optional[str]:
        cleaned = (value or "").strip()
        return cleaned or None

    @validator("target_keywords", pre=True)
    def _normalize_keywords(cls, value: Iterable[str] | None) -> list[str]:
        return _normalize_list(value, upper=False)

    @validator("target_countries", pre=True)
    def _normalize_countries(cls, value: Iterable[str] | None) -> list[str]:
        return _normalize_list(value, upper=True)

    @validator("target_keywords")
    def _limit_keywords(cls, value: list[str]) -> list[str]:
        if len(value) > 10:
            raise ValueError("target_keywords must contain at most 10 items")
        return value

    @validator("target_countries")
    def _limit_countries(cls, value: list[str]) -> list[str]:
        if len(value) > 10:
            raise ValueError("target_countries must contain at most 10 items")
        return value
