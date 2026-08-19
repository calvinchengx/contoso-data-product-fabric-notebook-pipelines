"""Register the Debezium connector, before any DML is replayed.

Order matters and is not incidental. The connector must be RUNNING before
`erp_source.py` applies its history, or those changes happen behind the
connector's start point and are captured — if at all — by a snapshot rather
than as a stream. That would still produce rows, and the counts might even
match, while quietly testing something other than change data capture.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import time
from typing import LiteralString, cast

import psycopg
import requests
from fabric import log
from sources import DEBEZIUM, erp_dsn

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFIG = ROOT / "sources" / "contoso-erp" / "debezium-connector.json"
TOPIC = "contoso.erp.customer"
SCHEMA = ROOT / "sources" / "contoso-erp" / "schema.sql"


def reset_topic() -> None:
    """Drop the change topic so the run's watermark means this run.

    rpk lives in the redpanda container; deleting through docker keeps the
    dependency to `docker`, which is already a prerequisite, rather than adding
    an admin client to the project.
    """
    subprocess.run(
        [
            "docker",
            "exec",
            "fabric-platform-notebook-pipelines-redpanda-1",
            "rpk",
            "topic",
            "delete",
            TOPIC,
        ],
        capture_output=True,
        check=False,
    )


def main() -> int:
    # The table must exist first: `table.include.list` matches nothing against
    # an absent table, and the connector then starts happily and captures
    # nothing at all.
    with psycopg.connect(erp_dsn(), autocommit=True) as conn:
        conn.execute(cast("LiteralString", SCHEMA.read_text(encoding="utf-8")))

    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    name = cfg["name"]

    # Delete first, so a re-run starts from a clean stream.
    #
    # Without this, a second `make verify` replays 93,571 more events onto a
    # topic that already holds them, and the watermark gate fails against a
    # doubled count — correctly, but for a reason that reads like a Debezium
    # fault rather than a re-run. Reproducibility is the property this whole
    # repository is built on; the ingest path does not get to opt out of it.
    requests.delete(f"{DEBEZIUM}/connectors/{name}", timeout=60)
    for _ in range(15):
        if requests.get(f"{DEBEZIUM}/connectors/{name}", timeout=30).status_code == 404:
            break
        time.sleep(1)
    reset_topic()
    r = requests.put(
        f"{DEBEZIUM}/connectors/{name}/config", json=cfg["config"], timeout=60
    )
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])

    # RUNNING is not enough on its own — a connector reports RUNNING while its
    # task has already failed, so both are checked.
    for _ in range(45):
        s = requests.get(f"{DEBEZIUM}/connectors/{name}/status", timeout=30).json()
        conn_state = s.get("connector", {}).get("state")
        tasks = [t.get("state") for t in s.get("tasks", [])]
        if conn_state == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            log(f"Debezium connector {name}: {conn_state}, tasks {tasks}")
            return 0
        if "FAILED" in (conn_state, *tasks):
            raise SystemExit(f"connector failed: {json.dumps(s)[:600]}")
        time.sleep(2)
    raise SystemExit(f"connector never reached RUNNING: {json.dumps(s)[:600]}")


if __name__ == "__main__":
    raise SystemExit(main())
