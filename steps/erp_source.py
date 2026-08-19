"""Contoso ERP: seed the database, then replay its history as real DML.

The fixture generates a change LOG. A change log is what CDC *produces*, not
what a source system holds — so this seeds the table the ERP would actually
have and then applies the events as INSERT / UPDATE / DELETE, in capture order.
Debezium watches the table and produces the log for us.

That inversion is the whole point of Wave 2. The emulator's own advanced example
lands a Parquet file DESCRIBING a change feed; this produces one.
"""

from __future__ import annotations

import pathlib
from typing import LiteralString, cast

import psycopg
import state
from fabric import log
from psycopg import sql
from sources import erp_dsn

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "sources" / "contoso-erp" / "schema.sql"

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
SETS = [c for c in COLUMNS if c != "erp_customer_id"]

# Composed with psycopg's `sql` module rather than str.format. The identifiers
# come from this file and nowhere near user input, but psycopg's types demand
# LiteralString precisely so that dynamic SQL has to be justified rather than
# assumed — and composing it properly is a better justification than a comment.
INSERT = sql.SQL("INSERT INTO erp.customer ({}) VALUES ({})").format(
    sql.SQL(", ").join(map(sql.Identifier, COLUMNS)),
    sql.SQL(", ").join(sql.Placeholder() * len(COLUMNS)),
)
UPDATE = sql.SQL("UPDATE erp.customer SET {} WHERE erp_customer_id = %s").format(
    sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder()) for c in SETS
    )
)
DELETE = sql.SQL("DELETE FROM erp.customer WHERE erp_customer_id = %s")


def _events_from_export(erp) -> list[dict]:
    """The change events, in capture order, from the vendor's Parquet export."""
    import io

    import pyarrow.parquet as pq

    blob = erp.export(erp.API_KEY)["changes.parquet"]
    table = pq.read_table(io.BytesIO(blob))
    rows = table.to_pylist()
    rows.sort(key=lambda r: r["capture_seq"])
    return rows


def main() -> int:
    import erp_system as erp

    # The vendor's own export is the ground truth for what happened. Read
    # through the PUBLIC api — `export()` — rather than the generator's
    # internals, because a consumer has only the published contract and a
    # private helper could change under a patch release without warning.
    events = _events_from_export(erp)
    assert len(events) == erp.EXPECTED_ERP_CHANGE_EVENTS, (
        len(events),
        erp.EXPECTED_ERP_CHANGE_EVENTS,
    )

    with psycopg.connect(erp_dsn(), autocommit=False) as conn:
        with conn.cursor() as cur:
            # The DDL is a repo-controlled file, not input — but psycopg's
            # types demand LiteralString precisely so that dynamic SQL has to
            # be justified rather than assumed. Saying so here is the
            # justification.
            cur.execute(cast("LiteralString", SCHEMA.read_text(encoding="utf-8")))
            # Start from empty so a re-run is not an append. The platform is a
            # test; a second run that silently doubles the history would still
            # be green on every count that is stated as a minimum.
            cur.execute("TRUNCATE erp.customer")
        conn.commit()

        ins = upd = dele = 0
        with conn.cursor() as cur:
            for i, e in enumerate(events, 1):
                if e["op"] == "I":
                    cur.execute(INSERT, tuple(e[c] for c in COLUMNS))
                    ins += 1
                elif e["op"] == "U":
                    cur.execute(UPDATE, (*(e[c] for c in SETS), e["erp_customer_id"]))
                    upd += 1
                else:
                    cur.execute(DELETE, (e["erp_customer_id"],))
                    dele += 1
                # Commit in batches: every row change still becomes its own CDC
                # event, but one transaction per statement would spend the run
                # in fsync.
                if i % 2000 == 0:
                    conn.commit()
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM erp.customer")
            row = cur.fetchone()
            assert row is not None, "COUNT(*) returned no row"
            live = row[0]

    assert ins == erp.EXPECTED_ERP_INSERTS, (ins, erp.EXPECTED_ERP_INSERTS)
    assert upd == erp.EXPECTED_ERP_UPDATES, (upd, erp.EXPECTED_ERP_UPDATES)
    assert dele == erp.EXPECTED_ERP_DELETES, (dele, erp.EXPECTED_ERP_DELETES)
    # What survives in the SOURCE is the current state — the deleted are gone
    # from the table and live on only in the change stream, which is precisely
    # why the stream is what a warehouse ingests.
    assert live == erp.EXPECTED_SCD2_CURRENT, (live, erp.EXPECTED_SCD2_CURRENT)

    state.save(erp_events=len(events), erp_live=live)
    log(
        f"ERP replayed: {ins:,} inserts, {upd:,} updates, {dele:,} deletes "
        f"→ {live:,} live rows, {len(events):,} change events"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
