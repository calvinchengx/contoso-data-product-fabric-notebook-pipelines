"""Landing → bronze, as a Fabric NOTEBOOK.

The transform is `definitions/bronze-ingest.Notebook/notebook-content.py` and it
is not imported here — it is published as a Notebook item and executed by a Spark
engine. This module is the operator: publish, submit, wait, grade.

WHY THIS CHANGED. Bronze always claimed to be notebook code — "Spark reads
`abfs://…` itself, which is what a Fabric notebook or a Spark Job Definition
does" — while actually running in this process over Spark Connect. **No Fabric
tenant exposes a Spark Connect endpoint**, so the step could not have run in
production at all: `FABRIC_TARGET=real` left `SPARK_REMOTE` unset and `spark.py`
refused, correctly and uselessly. Now Fabric decides where the transform runs,
and the same file is a notebook on both targets.

Bronze parses and nothing more. No dedupe, no conforming, no quarantine — those
are silver's job, and doing them here would destroy the only copy of what the
vendor actually sent.

HOW THE NUMBERS GET OUT. The notebook writes one row to `bronze_ingest_metrics`
and this module reads it. Real Fabric exposes no exit value for a REST-submitted
run, so a table is the only portable channel — and the quantities involved
(distinct counts, whether a declared column parsed at all) exist only inside the
transform. The grading stays here because the expected counts come from the
generator fixtures, which belong to the harness and not to the transform.
"""

from __future__ import annotations

import connections
import notebookjob
import state
import web_schema

# T comes from `fabric`, as it does in every other step. `target` has never
# exported it, so this import raised at module load and `make verify` has been
# broken on main since it landed — invisible because acceptance.yml runs on a
# SCHEDULE and workflow_dispatch, never on pull_request, so no PR check ever
# executes this file.
from fabric import FABRIC_AUD, STORAGE_AUD, T, log, token

NOTEBOOK = "bronze-ingest"
METRICS = "bronze_ingest_metrics"

# Which landing path fed which table. Used only for the lineage report — the
# notebook records its own reads and writes as it performs them, and this names
# the vendor→table pairing that a per-cell observation cannot express: one move
# per table, because the ERP change stream did not produce the customers table.
FEEDS = (
    ("contoso_pos/{day}/customers", "bronze_customers"),
    ("contoso_pos/{day}/orders", "bronze_orders"),
    ("contoso_web/{day}/customers", "bronze_web_customers"),
    ("contoso_web/{day}/products", "bronze_web_products"),
    ("contoso_web/{day}/orders", "bronze_web_orders"),
    ("contoso_reference/{day}/fx_rates.parquet", "bronze_fx_rates"),
    ("contoso_reference/{day}/product_hierarchy.parquet", "bronze_product_hierarchy"),
    ("contoso_erp/{day}/changes.parquet", "bronze_erp_changes"),
)


def leaf_names(fields: dict) -> str:
    """The declared leaf field names, for the notebook's parsed-anything check.

    Nested maps are skipped rather than flattened: `lines` is an array of struct,
    and `F.col("lines")` is what the notebook checks — its children are silver's
    concern.
    """
    return ",".join(fields)


def metrics_row(tok: str, workspace: str, lakehouse: str) -> dict:
    """The one row the notebook wrote, read back over OneLake.

    delta-rs rather than Spark: this process must not hold a session at all now,
    which is the point of the change. `T.delta_storage_options` is where the
    emulator-vs-real endpoint difference lives, so nothing about that decision is
    restated here.

    `az://`, NOT an https URL. The account name and the endpoint arrive through
    the storage options; the URI carries only the container — the workspace — and
    the path within it. Building `https://{onelake_url}/…` instead looks right and
    routes around the endpoint option, which is the same mistake that had the
    medallion examples landing bytes on the emulator's own control-plane prefix
    and 404ing on a tenant.
    """
    from deltalake import DeltaTable

    uri = f"az://{workspace}/{lakehouse}/Tables/{METRICS}"
    rows = (
        DeltaTable(uri, storage_options=T.delta_storage_options(tok))
        .to_pyarrow_table()
        .to_pylist()
    )
    assert len(rows) == 1, f"{METRICS} holds {len(rows)} rows, expected exactly 1"
    return rows[0]


def landed_at(
    tok: str, workspace: str, lakehouse: str, table: str
) -> tuple[int, list[str]]:
    """Open `Tables/{table}` and return its (row count, column names).

    THE COLUMNS AS WELL AS THE ROWS, because the disagreement between them is a
    diagnosis. `customer_columns` came back 102 against a vendor figure of 101
    while the table written from that same frame had 101 — and that gap localises
    the fault to the READ rather than to the data or the write. The cause turned
    out to be the emulator's `input_file_name()` shim tagging file reads with a
    hidden column that was stripped at `.write` and visible in `df.columns`, so
    every surface outside the notebook reported the right number. Comparing the
    frame to the table is what says "the read added it" in one assertion.

    WHERE, not just how many. A Copy activity fed a dataset whose empty column
    list rendered as a path segment ran, reported `Succeeded`, and wrote to
    `Tables/[]/bronze_customers` — every row count agreed and the bytes were in
    the wrong place. So each table is opened at the path this platform claims it
    wrote, and a table that is not there fails here rather than in whatever reads
    it next.
    """
    from deltalake import DeltaTable, exceptions

    uri = f"az://{workspace}/{lakehouse}/Tables/{table}"
    try:
        at_rest = DeltaTable(
            uri, storage_options=T.delta_storage_options(tok)
        ).to_pyarrow_table()
        return at_rest.num_rows, list(at_rest.column_names)
    except exceptions.TableNotFoundError as err:
        raise SystemExit(
            f"nothing at {uri} — bronze reported writing {table}, so either the "
            f"engine wrote somewhere else or the write did not happen"
        ) from err


def main() -> int:
    import erp_system as erp
    import reference_data as ref
    import source_system as src
    import web_store as web

    st = state.load()
    day = st["landing_day"]
    ws, lake = st["workspace"], st["lakehouse"]
    tok = token(FABRIC_AUD)

    body = notebookjob.content(
        NOTEBOOK,
        WORKSPACE=ws,
        LAKEHOUSE=lake,
        # The Environment that carries contoso-data-product to the engine.
        # Resolved by provision, because both notebooks bind the same one.
        ENVIRONMENT=st["environment"],
        # DAY IS STANDING IN FOR A JOB PARAMETER. Real Fabric would pass the
        # landing day through the RunNotebook job's `executionData.parameters`
        # and leave the file untouched; the emulator implements no parameter
        # override, so it is substituted instead. **When the override lands, this
        # is the call site to change** — the notebook's own cell says the same, so
        # whoever implements it finds both ends rather than grepping for `@@`.
        DAY=day,
        # The page schemas, rendered from the module a test pins to the vendor's
        # own OpenAPI. See the notebook's parameters cell for why they travel as
        # DDL rather than as an import.
        WEB_CUSTOMER=web_schema.array_of(web_schema.WEB_CUSTOMER),
        WEB_PRODUCT=web_schema.array_of(web_schema.WEB_PRODUCT),
        WEB_ORDER=web_schema.array_of(web_schema.WEB_ORDER),
        WEB_CUSTOMER_FIELDS=leaf_names(web_schema.WEB_CUSTOMER),
        WEB_PRODUCT_FIELDS=leaf_names(web_schema.WEB_PRODUCT),
        WEB_ORDER_FIELDS=leaf_names(web_schema.WEB_ORDER),
    )
    item = notebookjob.publish(tok, ws, NOTEBOOK, body)
    job = notebookjob.submit(tok, ws, item)
    notebookjob.await_job(tok, ws, item, job)

    stok = token(STORAGE_AUD)
    got = metrics_row(stok, ws, lake)

    # EVERY TABLE IS WHERE IT SAYS IT IS, and holds what the notebook counted.
    # Read from OneLake at the declared path rather than trusting the run's own
    # report: a job that says Succeeded having written to the wrong prefix is a
    # measured failure mode here, not a hypothetical.
    at_rest_cols: dict[str, list[str]] = {}
    for _, table in FEEDS:
        at_path, cols = landed_at(stok, ws, lake, table)
        at_rest_cols[table] = cols
        assert at_path == got[table], (
            f"Tables/{table} holds {at_path:,} rows and the run counted "
            f"{got[table]:,} — the notebook and the store disagree"
        )

    # --- what bronze must have preserved -----------------------------------
    # The vendor repeats a share of its rows. Bronze holding MORE rows than
    # distinct customers is the property silver's dedupe exists to fix — and if
    # bronze had already deduped, silver would pass its own assertions while
    # testing nothing.
    assert got["distinct_customers"] == src.EXPECTED_SILVER_CUSTOMERS, (
        got["distinct_customers"],
        src.EXPECTED_SILVER_CUSTOMERS,
    )
    assert got["bronze_customers"] > got["distinct_customers"], (
        f"bronze holds {got['bronze_customers']:,} rows for "
        f"{got['distinct_customers']:,} customers — the vendor's redeliveries "
        f"are missing, so silver's dedupe has nothing to do"
    )
    # WHICH COLUMNS, not how many. This asserted the count alone and the first run
    # it ever completed reported `(102, 101)` — a column had appeared and nothing
    # said which, so the two available fixes ("the reader is adding one" and "the
    # expectation is stale") were indistinguishable from the failure. They are not
    # equivalent: all three published fixture wheels declare 101, so 101 is the
    # vendor's own figure and an edit to 102 would have gone green over a column
    # nobody had identified. Silver carries the same assertion and would have been
    # edited twice.
    landed_cols = got["customer_column_names"].split(",")
    expected_cols = [name for name, _kind in src.CUSTOMER_COLUMNS]
    # THE FRAME AGAINST THE TABLE, first, because when these two disagree the
    # fault is in the READ and nowhere else — the notebook wrote what it read, so
    # a column the frame has and the table does not was added between the file and
    # the DataFrame. That is precisely how the emulator's `input_file_name()` shim
    # presented: a hidden tag on every file read, stripped at `.write`, so the
    # vendor's pages, the landed parts, a client Spark session and the written
    # table all said 101 while the notebook said 102.
    on_disk = at_rest_cols["bronze_customers"]
    frame_only = [c for c in landed_cols if c not in set(on_disk)]
    assert not frame_only, (
        f"the notebook's frame carries {frame_only} which Tables/bronze_customers "
        f"does not ({len(landed_cols)} columns in the frame, {len(on_disk)} at "
        f"rest). The write stripped it, so the READ added it — look at the engine "
        f"and its shims, not at the vendor's data."
    )
    extra = [c for c in landed_cols if c not in set(expected_cols)]
    absent = [c for c in expected_cols if c not in set(landed_cols)]
    # DUPLICATES SEPARATELY, because the two set differences above are both empty
    # when the surplus column repeats a name that belongs — and "a duplicate means
    # two part files were unioned" would then be a cause this check names and
    # cannot detect.
    seen: dict[str, int] = {}
    for c in landed_cols:
        seen[c] = seen.get(c, 0) + 1
    repeated = sorted(c for c, n in seen.items() if n > 1)
    assert not extra and not absent and not repeated, (
        f"bronze_customers has {len(landed_cols)} columns, the vendor declares "
        f"{len(expected_cols)}.\n"
        f"  appeared:  {extra}\n"
        f"  missing:   {absent}\n"
        f"  duplicated: {repeated}\n"
        f"An empty or `_c<N>` name means a trailing delimiter in the vendor's "
        f"header; a duplicate means two part files with different headers were "
        f"unioned by the reader."
    )
    # Kept as its own check: the count is what silver and gold size themselves
    # against, and a rename that swapped two names would satisfy the sets above.
    assert got["customer_columns"] == src.EXPECTED_CUSTOMER_COLUMNS, (
        got["customer_columns"],
        src.EXPECTED_CUSTOMER_COLUMNS,
    )

    # Orders arrive at-least-once, so bronze must exceed the distinct order
    # count that silver settles on.
    assert got["bronze_orders"] > got["distinct_orders"], (
        got["bronze_orders"],
        got["distinct_orders"],
    )

    # --- what the web vendor must have preserved ---------------------------
    assert got["bronze_web_customers"] == web.N_WEB_CUSTOMERS, (
        got["bronze_web_customers"],
        web.N_WEB_CUSTOMERS,
    )
    assert got["bronze_web_orders"] == web.N_WEB_ORDERS, (
        got["bronze_web_orders"],
        web.N_WEB_ORDERS,
    )
    assert got["bronze_web_products"] == len(web.PRODUCTS), (
        got["bronze_web_products"],
        len(web.PRODUCTS),
    )
    assert got["web_orders_has_lines"], (
        "bronze_web_orders lost its nested `lines` — a flattening reader lands "
        "the right row count and the loss surfaces later as an order with no items"
    )
    assert not got["blank_columns"], (
        f"{got['blank_columns']} parsed entirely NULL — the schema declared in "
        f"web_schema.py no longer matches what Contoso Web sends"
    )
    assert got["shared_emails"] > 0, (
        "no web account shares an email with a POS customer — the overlap that "
        "makes identity resolution a real problem is missing"
    )

    # --- what the reference vendor must have preserved ---------------------
    assert got["bronze_fx_rates"] == ref.EXPECTED_FX_ROWS, (
        got["bronze_fx_rates"],
        ref.EXPECTED_FX_ROWS,
    )
    assert got["bronze_product_hierarchy"] == ref.EXPECTED_PRODUCTS, (
        got["bronze_product_hierarchy"],
        ref.EXPECTED_PRODUCTS,
    )
    assert got["fx_currencies"] == ref.EXPECTED_FX_CURRENCIES, (
        got["fx_currencies"],
        ref.EXPECTED_FX_CURRENCIES,
    )
    assert got["departments"] == ref.EXPECTED_DEPARTMENTS, (
        got["departments"],
        ref.EXPECTED_DEPARTMENTS,
    )

    # THE GAPS ARE REAL AND MUST SURVIVE. FX is published on trading days only,
    # so this table is missing every weekend — and that absence is the whole
    # reason gold has to carry the last rate forward instead of joining on the
    # date. A vendor that started filling weekends, or a reader that quietly
    # interpolated, would make the carry-forward look like dead code while
    # silently changing what revenue means. Asserting the gap keeps the problem
    # in the data rather than in a comment.
    assert got["fx_published_days"] == ref.EXPECTED_FX_PUBLISHED_DAYS, (
        got["fx_published_days"],
        ref.EXPECTED_FX_PUBLISHED_DAYS,
    )
    assert got["fx_calendar_span"] > got["fx_published_days"], (
        f"FX covers {got['fx_calendar_span']} calendar days with "
        f"{got['fx_published_days']} published — the non-trading-day gaps are "
        f"gone, so the carry-forward in gold is no longer being exercised"
    )

    assert got["bronze_erp_changes"] == erp.EXPECTED_ERP_CHANGE_EVENTS, (
        got["bronze_erp_changes"],
        erp.EXPECTED_ERP_CHANGE_EVENTS,
    )

    # The landing→bronze hop, reported because nothing else can see it. The
    # engine read `abfs://…` directly, so the emulator watched bytes leave
    # OneLake and bytes arrive with nothing tying one to the other — and without
    # this the vendor nodes the ingest steps name would hang off landing paths
    # that no later edge mentions, leaving the source systems floating beside the
    # medallion rather than feeding it.
    if T.lineage_can_be_reported:
        connections.announce(
            tok,
            ws,
            "bronze",
            "landing",
            [
                {
                    "reads": [
                        {"itemId": lake, "path": f"Files/landing/{p.format(day=day)}"}
                    ],
                    "writes": [{"itemId": lake, "path": f"Tables/{table}"}],
                }
                for p, table in FEEDS
            ],
        )

    state.save(
        bronze={
            k: got[k]
            for k in (
                "bronze_customers",
                "bronze_orders",
                "bronze_web_customers",
                "bronze_web_products",
                "bronze_web_orders",
                "bronze_fx_rates",
                "bronze_product_hierarchy",
                "bronze_erp_changes",
            )
        },
        bronze_notebook=item,
        bronze_job=job,
    )
    log(
        f"bronze: {got['bronze_customers']:,} POS customer rows "
        f"({got['distinct_customers']:,} distinct, {got['customer_columns']} cols), "
        f"{got['bronze_orders']:,} POS order events "
        f"({got['distinct_orders']:,} distinct), "
        f"{got['bronze_web_customers']:,} web accounts "
        f"({got['shared_emails']:,} sharing an email with POS), "
        f"{got['bronze_web_orders']:,} web orders nested over "
        f"{got['bronze_web_products']} products, "
        f"{got['bronze_erp_changes']:,} ERP change events, "
        f"{got['bronze_product_hierarchy']} products over {got['departments']} "
        f"departments, {got['bronze_fx_rates']} FX rows on "
        f"{got['fx_published_days']} of {got['fx_calendar_span']} calendar days "
        f"— computed by a Fabric notebook"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
