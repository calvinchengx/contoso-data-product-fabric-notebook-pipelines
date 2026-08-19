"""What one step learned, for the next step to use.

A file rather than environment variables, because the steps are separate
processes and a reader must be able to run one of them on its own.
"""

from __future__ import annotations

import json

from fabric import STATE


def load() -> dict:
    if not STATE.exists():
        raise SystemExit(f"{STATE.name} is missing — run `make verify` from the start")
    return json.loads(STATE.read_text(encoding="utf-8"))


def save(**kw) -> dict:
    current = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
    current.update(kw)
    STATE.write_text(json.dumps(current, indent=2), encoding="utf-8")
    return current
