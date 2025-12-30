#!/usr/bin/env python3
"""Guard against accidental edits to protected endpoints."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROTECTED = {
    "app/api/routes/playlists.py": ["add_playlist", "refresh_playlist_stats"],
    "app/api/routes/basic_rank_checker.py": ["start_basic_scan", "stream_scan_events"],
}


def _run_git(args: list[str]) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def _find_base_ref() -> str:
    for ref in ("origin/main", "origin/master"):
        try:
            _run_git(["rev-parse", "--verify", ref])
            return ref
        except subprocess.CalledProcessError:
            continue
    try:
        _run_git(["rev-parse", "--verify", "HEAD~1"])
        return "HEAD~1"
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Unable to determine base ref for guard check.") from exc


def _extract_block(source: str, func_name: str) -> str:
    lines = source.splitlines()
    target = f"def {func_name}"
    start = None
    for idx, line in enumerate(lines):
        if line.startswith(target):
            start = idx
            while start > 0 and lines[start - 1].lstrip().startswith("@") and not lines[
                start - 1
            ].startswith(" "):
                start -= 1
            break
    if start is None:
        raise ValueError(f"Function {func_name} not found.")

    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].startswith("def "):
            end = idx
            break
    return "\n".join(lines[start:end]).strip()


def main() -> int:
    base_ref = _find_base_ref()
    errors = []

    for path, functions in PROTECTED.items():
        try:
            base_source = _run_git(["show", f"{base_ref}:{path}"])
            head_source = _run_git(["show", f"HEAD:{path}"])
        except subprocess.CalledProcessError as exc:
            errors.append(f"Unable to read {path}: {exc}")
            continue

        for func_name in functions:
            base_block = _extract_block(base_source, func_name)
            head_block = _extract_block(head_source, func_name)
            if base_block != head_block:
                errors.append(f"Protected function changed: {path}::{func_name}")

    if errors:
        for error in errors:
            print(error)
        return 1

    print("Guard check passed: protected endpoints unchanged.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
