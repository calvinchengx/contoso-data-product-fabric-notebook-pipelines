"""Register the Debezium connector, before any DML is replayed.

Order matters and is not incidental. The connector must be RUNNING before
`erp_source.py` applies its history, or those changes happen behind the
connector's start point and are captured — if at all — by a snapshot rather
than as a stream. That would still produce rows, and the counts might even
match, while quietly testing something other than change data capture.
"""

from __future__ import annotations

import json
import os
import pathlib
import time
from typing import LiteralString, cast

import psycopg
import requests
from fabric import log
from sources import DEBEZIUM, REDPANDA, erp_dsn

ROOT = pathlib.Path(__file__).resolve().parent.parent
# THE VENDORS LIVE IN THEIR OWN REPOSITORY. These paths read
# `ROOT / "sources" / ...`, which was correct while this product lived inside a
# platform that carried a copy of every vendor. G13 moved the vendors to
# contoso-sources and G2 moved this product out, so that path now names a
# directory in neither repository. SOURCES is exported by the platform that
# runs these steps; the fallback keeps a hand-run working.
SOURCES = pathlib.Path(os.environ.get("SOURCES", ROOT.parent / "contoso-sources"))

CONFIG = SOURCES / "contoso-erp" / "debezium-connector.json"
TOPIC = "contoso.erp.customer"
SCHEMA = SOURCES / "contoso-erp" / "schema.sql"


def reset_topic() -> None:
    """Drop the change topic so the run's watermark means this run.

    THROUGH THE BROKER, NOT THROUGH DOCKER. This used to `docker exec` into a
    container it named literally -- `...-redpanda-1` -- with `check=False` and
    the output captured. When G13 made the vendor stack generated from
    contoso-sources, the broker became `...-contoso-erp-broker-1` and this
    silently stopped doing anything: every re-run replayed onto a topic that
    still held the last one, the watermark gate failed against exactly twice
    the expected count, and the message blamed Debezium.

    Two faults, and the quieter one was worse. A container name is deployment
    knowledge this product should not hold at all; swallowing the failure is
    what let it be wrong for a day without saying so. The admin client talks to
    the same bootstrap the consumer already uses, so it works against a real
    Kafka too -- and a failure to delete is now raised rather than captured.
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": REDPANDA})
    if TOPIC not in admin.list_topics(timeout=30).topics:
        return
    for fut in admin.delete_topics([TOPIC], operation_timeout=30).values():
        fut.result()  # raises if the delete failed -- which is the point
    # Deletion is asynchronous: the broker acknowledges before the log is gone,
    # and Debezium recreating it underneath a half-deleted topic is how a
    # "clean" stream ends up holding the old one.
    for _ in range(30):
        if TOPIC not in admin.list_topics(timeout=10).topics:
            return
        time.sleep(1)
    raise SystemExit(
        f"topic {TOPIC!r} still exists after 30s -- refusing to replay onto it, "
        f"because the watermark gate would then be measuring two runs."
    )


def main() -> int:
    # The table must exist first: `table.include.list` matches nothing against
    # an absent table, and the connector then starts happily and captures
    # nothing at all.
    with psycopg.connect(erp_dsn(), autocommit=True) as conn:
        conn.execute(cast("LiteralString", SCHEMA.read_text(encoding="utf-8")))

    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))

    # OVERRIDE THE TOPOLOGY, KEEP THE CAPTURE. The vendor's connector file names
    # `erp-postgres` because that is what the database was called in the
    # platform the file came from. Where the database LISTENS is a deployment
    # fact; everything else in that file -- plugin, slot, table list, snapshot
    # mode, converters -- describes the capture and is the vendor's to state.
    # contoso-sources' own seeder draws the same line, in the same words.
    #
    # This was not needed while the platform hand-wrote its compose and happened
    # to name the service `erp-postgres` too. G13 made the vendor stack
    # GENERATED from contoso-sources, which names it `contoso-erp-db`, and the
    # two silently stopped matching: Debezium answered
    # `Connector configuration is invalid ... The connection attempt failed`,
    # which names neither the host nor the rename. G13 was verified by
    # `compose config` and the test suite, and neither can see this.
    cfg["config"]["database.hostname"] = os.environ.get("ERP_DB_HOST", "contoso-erp-db")

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
