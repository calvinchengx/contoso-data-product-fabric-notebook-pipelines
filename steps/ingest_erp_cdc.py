"""Consume the ERP change stream and land it in OneLake.

This is the boundary. Everything upstream — Postgres, Debezium, Redpanda — is
the world outside Fabric; everything downstream is the lakehouse. The consumer
is the only thing that touches both, which is exactly where a real ingestion
job sits.

WHAT IS PRESERVED, AND WHAT IS NOT. Counts survive real CDC: the same DML
produces the same events, so `EXPECTED_ERP_CHANGE_EVENTS` holds. LSNs, commit
timestamps and Kafka offsets do not — they differ every run, and nothing here
asserts on them. `effective_date` travels as DATA, which is what keeps the
fixture's deliberate disagreement between capture order and business order
intact: a pipeline that sorts by the wrong one still gets the wrong answer,
which is the lesson the ERP source exists to teach.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import connections
import state
from confluent_kafka import Consumer, TopicPartition
from fabric import FABRIC_AUD, STORAGE_AUD, log, token, upload
from sources import (
    ERP_DB,
    ERP_HOST,
    ERP_PORT,
    ERP_TOPIC,
    REDPANDA,
)

TOPIC = ERP_TOPIC

# Debezium's op codes, in the vocabulary the change log uses. `r` is a snapshot
# read: it should not appear here, because the connector is started before any
# DML — and if it does, that is a finding about the ordering, not a row to
# quietly relabel.
OPS = {"c": "I", "u": "U", "d": "D"}

COLUMNS = [
    "erp_customer_id",
    "phone",
    "legal_name",
    "account_tier",
    "segment",
    "credit_band",
    "account_status",
    "payment_terms_days",
    "country",
    "effective_date",
]


def high_watermark(consumer: Consumer) -> int:
    _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
    return high


def main() -> int:
    import erp_system as erp
    import pyarrow as pa
    import pyarrow.parquet as pq

    st = state.load()
    expected = erp.EXPECTED_ERP_CHANGE_EVENTS

    consumer = Consumer(
        {
            "bootstrap.servers": REDPANDA,
            "group.id": "contoso-erp-ingest",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(TOPIC, 0, 0)])

    # THE GATE. Never a sleep: a fixed wait is a flake generator that passes on
    # a fast machine and fails on a loaded one, and — worse — passes with a
    # partial stream, producing a smaller landed file that every count stated
    # as a minimum would still accept.
    high = high_watermark(consumer)
    assert high == expected, (
        f"the change stream holds {high:,} events, expected {expected:,} — "
        f"Debezium has not finished capturing, or captured something else"
    )

    rows = []
    while len(rows) < expected:
        msg = consumer.poll(30.0)
        assert msg is not None, f"stream stalled at {len(rows):,}/{expected:,}"
        assert not msg.error(), msg.error()
        # A null value is a TOMBSTONE, which the connector is configured not to
        # emit (`tombstones.on.delete: false`). One appearing here would mean
        # the connector config drifted, so it fails rather than being skipped —
        # skipping would silently shorten the stream by exactly the deletes.
        raw = msg.value()
        assert raw is not None, (
            f"tombstone at offset {msg.offset()} — tombstones.on.delete drifted"
        )
        env = json.loads(raw)
        op = env["op"]
        assert op in OPS, (
            f"unexpected Debezium op {op!r} at offset {msg.offset()} — 'r' means "
            f"a snapshot read, which means the connector started after the DML"
        )
        # A delete carries its row in `before`; an insert and an update in
        # `after`. REPLICA IDENTITY FULL is what makes the delete's before-image
        # complete — without it an SCD2 build cannot close the version it
        # belonged to, and the past is silently erased.
        image = env["before"] if op == "d" else env["after"]
        assert image, f"{op} at offset {msg.offset()} carried no row image"
        rows.append(
            {
                "op": OPS[op],
                "capture_offset": msg.offset(),
                **{c: image[c] for c in COLUMNS},
            }
        )
    consumer.close()

    by_op = {o: sum(1 for r in rows if r["op"] == o) for o in ("I", "U", "D")}
    assert by_op["I"] == erp.EXPECTED_ERP_INSERTS, by_op
    assert by_op["U"] == erp.EXPECTED_ERP_UPDATES, by_op
    assert by_op["D"] == erp.EXPECTED_ERP_DELETES, by_op

    # Parquet, because that is what a CDC sink lands and what a columnar read
    # downstream expects.
    table = pa.table({c: pa.array([r[c] for r in rows]) for c in rows[0]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    blob = buf.getvalue()

    day = st.get("landing_day") or dt.date.today().isoformat()
    dest = f"Files/landing/contoso_erp/{day}/changes.parquet"
    written = upload(st["workspace"], st["lakehouse"], dest, blob, token(STORAGE_AUD))
    assert written == len(blob), (written, len(blob))

    # NAME THE SOURCE. The bytes came from a Postgres change stream carried by
    # Kafka, not from a file — and a graph that starts at `changes.parquet`
    # cannot say that the ERP database is upstream of gold. The connection
    # records the system; `erp_dsn()` builds its address from the same values
    # this step connected with, password excluded.
    ftok = token(FABRIC_AUD)
    erp_conn = connections.ensure(
        ftok,
        "Contoso ERP",
        "OnPremisesGateway",
        connections.details(
            "PostgreSql",
            "PostgreSql",
            server=f"{ERP_HOST}:{ERP_PORT}",
            database=ERP_DB,
        ),
    )
    connections.announce(
        ftok,
        st["workspace"],
        "ingest_erp_cdc",
        "Contoso ERP",
        connections.from_source(erp_conn, st["lakehouse"], [dest]),
    )

    state.save(erp_landed=written, erp_change_events=len(rows), erp_connection=erp_conn)
    log(
        f"Contoso ERP: {len(rows):,} change events consumed from Kafka "
        f"({by_op['I']:,} I / {by_op['U']:,} U / {by_op['D']:,} D) "
        f"→ {dest}, {written:,} bytes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
