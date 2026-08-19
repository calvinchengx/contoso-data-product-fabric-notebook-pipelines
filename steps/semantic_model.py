"""Publish gold as a semantic model and query it the way Power BI does.

This is the readiness check for a BI client: a SemanticModel item with a TMSL
definition and measures, queried with **DAX over the Power BI `executeQueries`
wire** — the same REST surface Power BI Desktop, the service, and SemPy use.

DIRECT LAKE, which is what a production Fabric model does: the model binds to
gold in OneLake and the engine reads it in place. It was import-mode until
fabric-emulator 0.21.0, carrying gold's rows embedded in the definition as
`data.json` because the emulator's DAX evaluator read that instead of the
warehouse. That worked on one target only, and a model with an embedded copy of
its own source is two things that can disagree.

The audience matters and is asserted: `executeQueries` takes a Power BI token,
not the control-plane one. A surface that accepted either would be teaching the
wrong thing about Fabric's auth model.
"""

from __future__ import annotations

import base64
import json
import pathlib
import time

import pbip
import state
from fabric import (
    FABRIC,
    FABRIC_AUD,
    S,
    T,
    await_operation,
    ensure_audience,
    fabric,
    log,
    token,
)
from provision import find_item

import gold
from gold import in_dbt_container

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXPORT = ROOT / "gold" / "_export.json"

PBI_AUD = "https://analysis.windows.net/powerbi/api"
MODEL = "ContosoRevenue"

# Which gold table each model table projects from. Written here rather than
# derived from the name: `Revenue` comes from `fct_daily_revenue`, and a
# convention that guessed would break the first time a model table was renamed
# for the report writer's benefit — which is the whole point of a semantic layer.
GOLD_TABLE = {
    "Customer": "dim_customer",
    "Country": "dim_country",
    "Revenue": "fct_daily_revenue",
    "Reporting": "fct_revenue_summary",
}

# One query, asked over two surfaces. xmla_probe.py runs this same DAX through
# ADOMD.NET, so if both answer they must agree — which is a stronger statement
# than either surface makes alone. THE CONSTANT IS THE QUERY: main() used to
# re-declare an identical copy for the REST call, which meant the two surfaces
# were only running "the same DAX" for as long as nobody edited one of them.
#
# GROUPED BY Country[Country], not Customer[Country]. Country is now a real
# dimension that Customer and Revenue both point at, so this asks the question
# from the shared key rather than leaning on a many-to-many between two tables
# that have no key in common.
DAX = (
    "EVALUATE SUMMARIZECOLUMNS(Country[Country], "
    '"Revenue", [Total Revenue], "PerUnit", [Revenue per Unit])'
)


def sql_endpoint(workspace: str, warehouse: str, tok: str) -> str:
    """Where a SQL client dials to reach the Warehouse, asked of Fabric.

    DISCOVERED, never configured. Real Fabric returns
    `properties.connectionString` on a Warehouse item, and it is per-workspace —
    so an endpoint written into a config file is one that cannot be right on
    two targets at once. Asking the API is the same call on both.
    """
    r = fabric("GET", f"/workspaces/{workspace}/items/{warehouse}", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    cs = (r.json().get("properties") or {}).get("connectionString")
    assert cs, (
        "the Warehouse advertises no connectionString. On the emulator that "
        "means no SQL endpoint is running (FABRIC_SQL_TDS_ADDR unset), and the "
        "gold export this step grades against reads gold over TDS."
    )
    return cs


def gold_column(model_column: str) -> str:
    """The warehouse column a model column projects from.

    Gold is snake_case because dbt wrote it; the model is PascalCase because a
    report writer reads it. `export_gold.py` already performs this mapping when
    it selects rows, and a SECOND hand-written copy here would drift — silently,
    because a partition naming a column that does not exist fails only when
    something actually refreshes, which today is nothing.

    So it is derived, and `test_the_partition_columns_match_the_gold_export`
    pins the derivation against the SELECTs export_gold.py really issues.
    """
    out = []
    for i, ch in enumerate(model_column):
        if ch.isupper() and i:
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


# The shared M expression every Direct Lake partition points at, by name.
#
# THE HOST IS LITERAL ON BOTH TARGETS, and this is the one place where writing
# the emulator's own address would be wrong. A Direct Lake expression is not
# fetched by the client: Fabric's engine resolves it, and the emulator parses the
# workspace and item ids straight out of it. So the public host belongs here
# verbatim even when nothing public is involved — resolving it per target made the
# conversion fail on the emulator, measured, because the ids no longer parsed.
ONELAKE_EXPRESSION = "GoldWarehouse"


def gold_expression(workspace: str, warehouse: str) -> dict:
    return {
        "name": ONELAKE_EXPRESSION,
        "kind": "m",
        "expression": (
            'let\n    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.'
            f'microsoft.com/{workspace}/{warehouse}")\nin\n    Source'
        ),
    }


def direct_lake_partition(table: str) -> dict:
    """Where this table's rows come from, as a Direct Lake partition.

    NO QUERY, which is the whole difference from the import partition above and
    the reason the column mapping had to move. An import partition carries
    `Value.NativeQuery(… SELECT [snake_case] AS [PascalCase] …)`, so the SELECT is
    where gold's names became the model's. Direct Lake names an ENTITY and the
    engine reads it in place — there is no query to alias in, so every
    `sourceColumn` must be the warehouse's own column name and the rename lives
    in `name` alone.

    WHY THIS REPLACES THE IMPORT MODE. The import model leaned on `data.json`, an
    emulator-native convenience: the rows were exported over TDS and embedded in
    the definition, so the model carried a COPY of gold rather than reading it.
    Power BI Desktop opens such a model to empty tables and nothing in the
    definition says why. Direct Lake is what a production model does — bind to
    gold, read it in place — and since fabric-emulator 0.21.0 the emulator
    resolves it over a Warehouse too, so the same definition answers DAX on both
    targets.
    """
    return {
        "name": table,
        "mode": "directLake",
        "source": {
            "type": "entity",
            "entityName": table,
            "schemaName": "dbo",
            "expressionSource": ONELAKE_EXPRESSION,
        },
    }


def definition(workspace: str = "", warehouse: str = "") -> dict:
    """TMSL as a single InlineBase64 definition part.

    ONE SOURCE OF ROWS, which is what changed. This used to ship `data.json`
    beside the model — gold's rows exported over TDS and embedded — because the
    emulator's DAX evaluator read that rather than the warehouse. A Direct Lake
    model reads gold in place on both targets, so a second embedded copy could
    only ever drift from the first.
    """
    model = {
        "name": MODEL,
        # 1604, not 1550: Direct Lake partitions are not expressible below it,
        # and the emulator enforces that rather than accepting a model it cannot
        # resolve — measured, after a conversion silently produced empty tables.
        "compatibilityLevel": 1604,
        "model": {
            "culture": "en-US",
            "tables": [
                {
                    "name": "Customer",
                    "columns": [
                        {"name": c, "dataType": "string", "sourceColumn": c}
                        for c in ("CustomerId", "Name", "Country")
                    ],
                },
                # THE CONFORMED GEOGRAPHY DIMENSION, and the fix for this
                # model's one bad relationship. `Country` used to be a column on
                # two unrelated tables joined to each other on it — three values
                # across 100,000 customers, which is a many-to-many that answers
                # a country total and nothing finer. Now it is a table with one
                # row per country, and everything that has a country points at
                # it. See gold/models/dim_country.sql for the full argument.
                {
                    "name": "Country",
                    "columns": [
                        {"name": c, "dataType": "string", "sourceColumn": c}
                        for c in ("Country", "CountryName")
                    ],
                },
                {
                    "name": "Revenue",
                    "columns": [
                        {
                            "name": "OrderDate",
                            "dataType": "string",
                            "sourceColumn": "OrderDate",
                        },
                        {
                            "name": "Country",
                            "dataType": "string",
                            "sourceColumn": "Country",
                        },
                        {
                            "name": "Orders",
                            "dataType": "int64",
                            "sourceColumn": "Orders",
                        },
                        {"name": "Units", "dataType": "int64", "sourceColumn": "Units"},
                        {
                            # Currency, like every other money column here.
                            "name": "Revenue",
                            "dataType": "decimal",
                            "sourceColumn": "Revenue",
                        },
                    ],
                    # Measures are the point of a semantic model: a column plus
                    # how you aggregate it, which is the thing a table cannot
                    # say and a report writer needs.
                    "measures": [
                        {
                            "name": "Total Revenue",
                            "expression": "SUM(Revenue[Revenue])",
                        },
                        {"name": "Total Units", "expression": "SUM(Revenue[Units])"},
                        {
                            "name": "Revenue per Unit",
                            "expression": "DIVIDE([Total Revenue], [Total Units])",
                        },
                    ],
                },
                # --- the management reporting table -----------------------
                # WHAT A P&L PACK IS ACTUALLY BUILT ON, and the table that
                # makes this model answer management accounting rather than
                # only "how did we trade today". Three axes the Revenue table
                # cannot offer at all: Contoso's 1 April financial year, the
                # group data office's product rollup, and customer segment.
                #
                # NO RELATIONSHIP, deliberately, and this is the contrast worth
                # reading. `Revenue` has to reach `Customer` through Country —
                # a three-value column, so the relationship is many-to-many and
                # can carry a country total and nothing finer. An aggregate
                # that carries its own attributes needs no such bridge: every
                # slice below is a column on the row it describes.
                {
                    "name": "Reporting",
                    "columns": [
                        {"name": c, "dataType": t, "sourceColumn": c}
                        for c, t in (
                            ("FiscalYearLabel", "string"),
                            ("FiscalQuarterLabel", "string"),
                            ("Department", "string"),
                            ("ProductSegment", "string"),
                            ("CustomerSegment", "string"),
                            ("Country", "string"),
                            # WHICH BUSINESS SOLD IT. "revenue is up" and
                            # "online revenue is up" are different sentences,
                            # and a pack that cannot separate them can only
                            # make the first one.
                            ("ChannelSystem", "string"),
                            ("SaleLines", "int64"),
                            ("Units", "int64"),
                            # `decimal` in TMSL is Currency — fixed at four
                            # decimal places, the same width the Warehouse
                            # stores. So money keeps one type from Spark through
                            # gold to a Power BI measure instead of becoming a
                            # float at the last hop.
                            ("RevenueUsd", "decimal"),
                            ("CancelledRevenueUsd", "decimal"),
                            ("RevenueAtCarriedRate", "decimal"),
                        )
                    ],
                    "measures": [
                        # IN USD, converted per order at that day's rate —
                        # which today equals `Revenue`'s figure exactly,
                        # because Contoso POS stamps every order `USD`. The
                        # two are denominated differently even where they
                        # agree, and only this one stays right when the
                        # storefront's currencies arrive.
                        {
                            "name": "Revenue USD",
                            "expression": "SUM(Reporting[RevenueUsd])",
                        },
                        {
                            "name": "Units Sold",
                            "expression": "SUM(Reporting[Units])",
                        },
                        # NET IS THE HEADLINE and cancellations are reported
                        # beside it, never folded in. The storefront writes off
                        # about 5% of what it takes and the shops write off
                        # nothing, so a single "revenue" measure would overstate
                        # one business and misdescribe the other.
                        {
                            "name": "Cancelled Revenue",
                            "expression": "SUM(Reporting[CancelledRevenueUsd])",
                        },
                        {
                            "name": "Gross Revenue",
                            "expression": "[Revenue USD] + [Cancelled Revenue]",
                        },
                        # HOW MUCH OF THE ABOVE RESTS ON AN ASSUMPTION. FX is
                        # published on trading days only, so weekend trading is
                        # converted at the preceding Friday's rate. Surfacing
                        # the share as a measure means a reviewer can ask the
                        # question without leaving the report.
                        {
                            "name": "Revenue at Carried Rate",
                            "expression": "SUM(Reporting[RevenueAtCarriedRate])",
                        },
                        {
                            "name": "Carried Rate Share",
                            "expression": (
                                "DIVIDE([Revenue at Carried Rate], [Revenue USD])"
                            ),
                        },
                    ],
                },
            ],
            # EVERY RELATIONSHIP IS MANY-TO-ONE, pointing at a unique key.
            #
            # What was here before was `Revenue[Country] -> Customer[Country]`:
            # a join between two tables that share no key, on a column with
            # three distinct values. Tabular models do not reject that, they
            # resolve it as many-to-many and return a number — which is why it
            # survived. It could answer "revenue by country" and could not
            # answer anything below country, because there is no path from an
            # aggregated revenue row to a customer.
            #
            # The replacement is the ordinary star shape: `Country` is the one
            # side, everything that carries a country is the many side. A single
            # country selection now filters Customer, Revenue and Reporting
            # consistently, which is the thing a conformed dimension buys and
            # the thing the old edge could not do.
            "relationships": [
                {
                    "name": f"{table}_Country",
                    "fromTable": table,
                    "fromColumn": "Country",
                    "toTable": "Country",
                    "toColumn": "Country",
                    "fromCardinality": "many",
                    "toCardinality": "one",
                }
                for table in ("Customer", "Revenue", "Reporting")
            ],
        },
    }

    def part(path: str, obj: dict) -> dict:
        return {
            "path": path,
            "payloadType": "InlineBase64",
            "payload": base64.b64encode(json.dumps(obj).encode()).decode(),
        }

    # Attach a Direct Lake partition per table when the warehouse is known.
    # Guarded rather than assumed so `definition()` stays callable without a live
    # warehouse — the governance step and the tests both do that.
    if workspace and warehouse:
        model["model"]["expressions"] = [gold_expression(workspace, warehouse)]
        tables: list[dict] = model["model"]["tables"]
        for tbl in tables:
            name: str = tbl["name"]
            gold_name = GOLD_TABLE[name]
            tbl["partitions"] = [direct_lake_partition(gold_name)]
            # EVERY sourceColumn BECOMES THE WAREHOUSE'S OWN NAME. With no query
            # to alias in, a PascalCase sourceColumn names a column gold does not
            # have — and a Direct Lake partition pointing at a missing column
            # fails only when something refreshes, which is exactly the silent
            # shape `gold_column`'s docstring warns about.
            for col in tbl["columns"]:
                col["sourceColumn"] = gold_column(col["name"])

    # `data.json` IS GONE, and that is the point of the change rather than a side
    # effect. It carried a copy of gold's rows for the emulator's bounded DAX
    # evaluator to read; a Direct Lake model reads gold itself, on both targets,
    # so embedding rows would now be a second source that can disagree with the
    # first. Requires fabric-emulator >= 0.21.0 (see versions.env).
    return {"parts": [part("model.bim", model)]}


def publish(workspace: str, defn: dict, tok: str) -> str:
    h = {"Authorization": f"Bearer {tok}"}

    # Resolve-or-update, not create. Display names are unique per workspace on
    # both targets, so a second create returns 409 ItemDisplayNameAlreadyInUse —
    # and a platform that only runs against a fresh workspace is not one anybody
    # can operate. Updating also refreshes the embedded rows, which is what a
    # rebuild of gold should do to the model that carries it.
    existing = find_item(tok, workspace, MODEL, "SemanticModel")
    if existing:
        r = S.post(
            f"{FABRIC}/v1/workspaces/{workspace}/items/{existing['id']}"
            f"/updateDefinition",
            headers=h,
            json={"definition": defn},
            timeout=120,
        )
        assert r.status_code in (200, 202), (r.status_code, r.text[:300])
        # AWAIT IT. A 202 means the definition has NOT been applied yet, and the
        # next thing this platform does is query the model — so accepting the 202
        # and returning made the rebuilt rows a race. It presented as "the model
        # still has the old numbers" rather than as an error, which is the worst
        # way for it to present.
        #
        # Invisible locally until the emulator learned to answer 202 for a
        # definition write (fabric-emulator #173/#174); silver.py has always
        # awaited its own updateDefinition, so this was an inconsistency inside
        # one repo rather than a missing idea.
        await_operation(r, tok, "updateDefinition")
        return existing["id"]

    r = S.post(
        f"{FABRIC}/v1/workspaces/{workspace}/items",
        headers=h,
        json={"displayName": MODEL, "type": "SemanticModel", "definition": defn},
        timeout=120,
    )
    assert r.status_code in (201, 202), (r.status_code, r.text[:300])
    if r.status_code == 201:
        return r.json()["id"]

    # 202 is the long-running-operation path, which real Fabric uses for most
    # mutations. Poll it rather than assuming the sync shape.
    op = r.headers["x-ms-operation-id"]
    for _ in range(60):
        status = S.get(f"{FABRIC}/v1/operations/{op}", headers=h, timeout=30).json()[
            "status"
        ]
        if status in ("Succeeded", "Failed"):
            break
        time.sleep(1)
    assert status == "Succeeded", f"publish operation {status}"
    return S.get(f"{FABRIC}/v1/operations/{op}/result", headers=h, timeout=30).json()[
        "id"
    ]


def main() -> int:
    import source_system as src

    st = state.load()

    log("exporting gold from the warehouse over TDS")
    rc = in_dbt_container("--entrypoint", "python", "dbt", "/tools/export_gold.py")
    assert rc == 0, f"gold export failed: exit {rc}"
    rows = json.loads(EXPORT.read_text(encoding="utf-8"))
    assert (
        rows["Revenue"] and rows["Customer"] and rows["Reporting"] and rows["Country"]
    ), "the export is empty"

    tok = token(FABRIC_AUD)
    # A PRECONDITION NOW, not an input to the model. A Direct Lake partition names
    # no server — the engine resolves OneLake itself — so this no longer feeds the
    # definition. It is still worth asking: the export above grades this step, and
    # it reads gold over TDS, so an endpoint that is not listening fails here with
    # a cause rather than three calls later with an empty comparison. The database
    # name is the one thing the Warehouse differs about across targets, so
    # target.py resolves it and this does not decide it.
    server = sql_endpoint(st["workspace"], st["warehouse"], tok)
    database = T.warehouse_database(st["warehouse"], gold.WAREHOUSE)
    log(f"gold readable over TDS at {server}, database {database}")

    defn = definition(st["workspace"], st["warehouse"])
    dataset = publish(st["workspace"], defn, tok)

    # The same definition, on disk, as a Power BI Project. Written from `defn`
    # rather than rebuilt so the folder describes what was actually published —
    # a second construction could drift by a partition and nobody would see it.
    model_bim = json.loads(base64.b64decode(defn["parts"][0]["payload"]))
    folder = pbip.write(ROOT / "artifacts" / "pbip", model_bim)
    log(f"PBIP written to {folder.relative_to(ROOT)}")
    state.save(dataset=dataset)

    # Queried exactly as a Power BI REST client would.
    ensure_audience(PBI_AUD, "Power BI Service")
    # THE CONSTANT, not a copy of it. xmla_probe.py imports `DAX` and runs it
    # through ADOMD.NET; this used to re-declare the same string locally, so the
    # claim that both surfaces answer the same question held only until someone
    # edited one of them — and the two would then have disagreed silently while
    # both still returning plausible totals.
    dax = DAX
    url = (
        f"{FABRIC}/v1.0/myorg/groups/{st['workspace']}"
        f"/datasets/{dataset}/executeQueries"
    )
    r = S.post(
        url,
        headers={"Authorization": f"Bearer {token(PBI_AUD)}"},
        json={"queries": [{"query": dax}]},
        timeout=120,
    )
    assert r.status_code == 200, (r.status_code, r.text[:300])
    result = r.json()["results"][0]["tables"][0]["rows"]
    assert result, r.text[:300]

    # The measure has to agree with the fixture, not merely return something.
    total = sum(row["[Revenue]"] for row in result)
    assert abs(total - src.EXPECTED_REVENUE) < 0.01, (total, src.EXPECTED_REVENUE)
    countries = {row["Country[Country]"] for row in result}
    assert countries == src.EXPECTED_COUNTRIES, (
        sorted(countries),
        src.EXPECTED_COUNTRIES,
    )

    # --- the management reporting pack, over the same wire -----------------
    # A PUBLISHED SURFACE NOBODY QUERIES is a surface nobody finds out has
    # broken. The tables above are asserted, so this one is too — and it is
    # asked the way a Power BI report asks it, by fiscal period and product
    # segment, which is the whole reason this table exists.
    pack_dax = (
        "EVALUATE SUMMARIZECOLUMNS(Reporting[FiscalQuarterLabel], "
        'Reporting[ChannelSystem], "Revenue", [Revenue USD], '
        '"Carried", [Revenue at Carried Rate])'
    )
    r = S.post(
        url,
        headers={"Authorization": f"Bearer {token(PBI_AUD)}"},
        json={"queries": [{"query": pack_dax}]},
        timeout=120,
    )
    assert r.status_code == 200, (r.status_code, r.text[:300])
    pack = r.json()["results"][0]["tables"][0]["rows"]
    assert pack, r.text[:300]

    # Graded against the EXPORT, which came from the warehouse — so the model
    # agreeing with itself is not what is being checked here.
    usd = sum(row["[Revenue]"] for row in pack)
    expected_usd = sum(x["RevenueUsd"] for x in rows["Reporting"])
    assert abs(usd - expected_usd) < 0.01, (usd, expected_usd)

    # THE FISCAL YEAR REALLY APPLIED, and it spans a QUARTER BOUNDARY. Contoso's
    # year starts 1 April, so July trading reports as Q2 — a model that quietly
    # fell back to the calendar would say Q3 and every total would still be
    # right. Q1 exists only because the storefront's UTC offsets pull some sales
    # back to 30 June; a reader that took the shopper's local date instead would
    # report a single quarter and lose the boundary entirely.
    quarters = {row["Reporting[FiscalQuarterLabel]"] for row in pack}
    assert quarters == {"FY27 Q1", "FY27 Q2"}, (
        f"expected FY27 Q1 and Q2 on a 1 April financial year — Q1 from the "
        f"storefront sales that fall on 30 June once their UTC offsets are "
        f"applied — got {sorted(quarters)}"
    )

    # BOTH SELLING SYSTEMS reach the pack. The storefront arrives through the
    # resolved party key, and a join that silently dropped it would leave a
    # model describing a business with no online channel.
    systems = {row["Reporting[ChannelSystem]"] for row in pack}
    assert systems == {"POS", "WEB"}, sorted(systems)
    carried = sum(row["[Carried]"] for row in pack)
    assert carried > 0, (
        "no revenue is flagged as converted at a carried-forward rate — the "
        "weekend FX gaps stopped being filled, or stopped existing"
    )

    # A control-plane token must be refused. Fabric's audiences are not
    # interchangeable, and a surface that accepted either would be teaching the
    # wrong thing about its auth model.
    r = S.post(
        url,
        headers={"Authorization": f"Bearer {token(FABRIC_AUD)}"},
        json={"queries": [{"query": dax}]},
        timeout=60,
    )
    assert r.status_code == 401, (
        f"a non-Power BI audience was accepted: {r.status_code}"
    )

    log(
        f"semantic model {dataset}: DAX over executeQueries — "
        f"{total:,.2f} revenue across {sorted(countries)}; reporting pack "
        f"{usd:,.2f} USD net over {sorted(quarters)} from {sorted(systems)}, "
        f"{carried:,.2f} ({100 * carried / usd:.1f}%) at a carried-forward rate"
    )
    log("executeQueries rejects a non-Power BI audience token (401)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
