"""Catalog the platform in OpenMetadata — including the SOURCE SYSTEMS.

THIS IS THE POINT OF THE WHOLE PLATFORM. A catalog that starts at bronze can
answer "where did this number come from?" with a filename in `Files/landing/`
and stop. Contoso POS is not a CSV; it is a REST API with a published contract.
Contoso ERP is not a Parquet file; it is a Postgres table whose changes reach us
through a Kafka topic. Until those are entities, lineage is a half-truth.

Three source archetypes, three OpenMetadata service types, each native:

    Contoso POS   REST + OpenAPI   ->  API service        (collection, endpoints)
    Contoso ERP   Postgres         ->  Database service   (schema, table)
    ERP changes   Redpanda topic   ->  Messaging service  (topic)

DERIVED, NEVER AUTHORED TWICE. The API endpoints come from the OpenAPI spec the
vendor publishes and mokapi serves; the ERP columns come from the DDL that
created the table. A catalog whose semantics are retyped drifts from the
pipeline by the end of the first sprint.
"""

from __future__ import annotations

import base64
import json
import os
import pathlib
import re

import requests
import state
import yaml
from contoso_product.contracts import DOMAIN
from fabric import log
from sources import ERP_DB, ERP_TOPIC, POS_API

ROOT = pathlib.Path(__file__).resolve().parent.parent
# 18587, not 8585: compose/governance.yml moved the published port to stop
# colliding with the two sibling platforms, and this default stayed behind.
# It works under `make`, which exports OM_URL, so the wrong number is only
# reachable when a step is run by hand, which is when it is hardest to read.
OM = os.environ.get("OM_URL", "http://localhost:18587/api/v1").rstrip("/")
# OpenMetadata's seeded dev admin. Basic auth is not enough — the API wants a
# JWT, obtained by exchanging these at /users/login.
OM_USER = "admin@open-metadata.org"
OM_PASSWORD = "admin"

# THE VENDORS LIVE IN THEIR OWN REPOSITORY. These paths read
# `ROOT / "sources" / ...`, which was correct while this product lived inside a
# platform that carried a copy of every vendor. G13 moved the vendors to
# contoso-sources and G2 moved this product out, so that path now names a
# directory in neither repository. SOURCES is exported by the platform that
# runs these steps; the fallback keeps a hand-run working.
SOURCES = pathlib.Path(os.environ.get("SOURCES", ROOT.parent / "contoso-sources"))

POS_SPEC = SOURCES / "contoso-pos" / "openapi.yaml"
ERP_DDL = SOURCES / "contoso-erp" / "schema.sql"

S = requests.Session()


def login() -> None:
    """Exchange the seeded admin for a JWT.

    Basic auth returns `401 Token not present` — a message that says what is
    missing rather than what to do, so it is worth stating here: OpenMetadata
    authenticates every API call with a bearer, and the password goes over
    /users/login base64-encoded.
    """
    r = S.post(
        f"{OM}/users/login",
        json={
            "email": OM_USER,
            "password": base64.b64encode(OM_PASSWORD.encode()).decode(),
        },
        timeout=60,
    )
    assert r.status_code == 200, (r.status_code, r.text[:300])
    S.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"


def put(path: str, body: dict) -> dict:
    """OpenMetadata's PUT is create-or-update, so this is idempotent.

    Some endpoints answer 200 with an EMPTY body — lineage among them. Returning
    {} rather than raising keeps that legible, and every caller that needs a
    field from the response asserts on it, so an unexpectedly empty answer fails
    where it matters instead of here.
    """
    r = S.put(f"{OM}/{path}", json=body, timeout=60)
    assert r.status_code in (200, 201), (path, r.status_code, r.text[:300])
    return r.json() if r.content else {}


def register_pos() -> tuple[str, list[str]]:
    """Contoso POS as an API service, from the spec the vendor publishes."""
    spec = yaml.safe_load(POS_SPEC.read_text(encoding="utf-8"))
    svc = put(
        "services/apiServices",
        {
            "name": "contoso-pos",
            "serviceType": "Rest",
            "description": spec["info"]["description"].strip(),
            # `docURL`, not `openAPISchemaURL`: the field name differs by
            # OpenMetadata version and a wrong one is rejected at encrypt time
            # with a message that names the field, which is how this was found.
            "connection": {"config": {"type": "Rest", "docURL": POS_API}},
        },
    )
    assert svc.get("fullyQualifiedName"), ("api service", svc)
    collection = put(
        "apiCollections",
        {
            "name": "export",
            "displayName": spec["info"]["title"],
            "service": svc["fullyQualifiedName"],
            "endpointURL": f"{POS_API}/api/v1/export",
        },
    )

    assert collection.get("fullyQualifiedName"), ("api collection", collection)
    endpoints = []
    for route, methods in spec["paths"].items():
        for method, op in methods.items():
            ep = put(
                "apiEndpoints",
                {
                    "name": op["operationId"],
                    "displayName": op.get("summary", op["operationId"]),
                    "apiCollection": collection["fullyQualifiedName"],
                    "endpointURL": f"{POS_API}{route}",
                    "requestMethod": method.upper(),
                },
            )
            endpoints.append(ep["fullyQualifiedName"])
    return svc["fullyQualifiedName"], endpoints


def register_erp() -> str:
    """Contoso ERP as a Database service, with columns from its own DDL."""
    put(
        "services/databaseServices",
        {
            "name": "contoso-erp",
            "serviceType": "Postgres",
            "description": "Contoso ERP — the finance master. A database that "
            "CHANGES, which is why it reaches us by CDC rather than as a file.",
            "connection": {
                "config": {
                    "type": "Postgres",
                    "username": "contoso",
                    "authType": {"password": "***"},
                    "hostPort": "erp-postgres:5432",
                    "database": ERP_DB,
                }
            },
        },
    )
    db = put(
        "databases",
        {"name": ERP_DB, "service": "contoso-erp"},
    )
    schema = put(
        "databaseSchemas",
        {"name": "erp", "database": db["fullyQualifiedName"]},
    )

    # Columns from the DDL that created the table — one definition, not two.
    ddl = ERP_DDL.read_text(encoding="utf-8")
    marker = "CREATE TABLE IF NOT EXISTS erp.customer ("
    body = ddl.split(marker, 1)[1].split(");", 1)[0]
    columns = []
    for line in body.splitlines():
        parts = line.strip().rstrip(",").split()
        if len(parts) >= 2 and not parts[0].startswith("--"):
            pg = parts[1].lower()
            columns.append(
                {
                    "name": parts[0],
                    "dataType": {
                        "text": "STRING",
                        "integer": "INT",
                        "date": "DATE",
                    }.get(pg, "STRING"),
                }
            )
    assert columns, "no columns parsed from the ERP DDL"

    table = put(
        "tables",
        {
            "name": "customer",
            "databaseSchema": schema["fullyQualifiedName"],
            "columns": columns,
        },
    )
    return table["fullyQualifiedName"]


def register_topic() -> str:
    """The change stream as a Messaging service — the CDC hop made visible."""
    put(
        "services/messagingServices",
        {
            "name": "contoso-redpanda",
            "serviceType": "Kafka",
            "description": "Debezium publishes Contoso ERP's row changes here.",
            "connection": {
                "config": {"type": "Kafka", "bootstrapServers": "redpanda:9092"}
            },
        },
    )
    topic = put(
        "topics",
        {
            "name": ERP_TOPIC,
            "service": "contoso-redpanda",
            "partitions": 1,
            "description": "One message per row change: op c/u/d with before and "
            "after images. REPLICA IDENTITY FULL, so a delete carries the row it "
            "closed rather than only its key.",
        },
    )
    return topic["fullyQualifiedName"]


# --- the platform's own tables ------------------------------------------------
# A Fabric workspace maps to an OM database; the lakehouse and the warehouse are
# its schemas — which is what they are on the SQL surface too, so a catalog user
# and a SQL user see the same names.
FABRIC_SERVICE = "contoso-fabric"

# The medallion, in order. Each entry is (schema, table, upstreams) and the
# upstreams are what actually built it — derived from the pipeline, not from a
# diagram someone drew once.
# --- the semantic layer -----------------------------------------------------
#
# Entities and lineage say a number EXISTS and where it came from. They cannot
# say what it means, who it is for, or what it promises — which is the half a
# consumer actually needs. These four blocks add that half, and every one of
# them is derived from something the platform already has.
#
# WHAT IS DELIBERATELY ABSENT: prose. A business glossary describing what
# "revenue" means to Contoso would have to be typed here, and this file's own
# rule forbids that — it would drift from the pipeline by the end of the first
# sprint and nothing would notice. The terms below are defined by their SQL,
# which is derived and exact. Real prose belongs in a data contract next to the
# transform, and there is not one in this repository yet.
# DOMAIN IS CORE'S, IMPORTED RATHER THAN RESTATED. This file used to declare
# `DOMAIN = "contoso-sales"` while contoso-data-product already named the same
# domain `contoso-commerce`, and databricks-platform-jobs imported that one. So
# the two runtimes published ONE product into TWO domains, described two ways —
# the disagreement a shared catalog exists to remove, reproduced inside it.
#
# Nothing detected it, because each platform was self-consistent and neither
# read the other. RULES.md §1 already said a platform consumes what core names
# instead of restating it; this was simply in breach, in the same shape as a
# platform carrying its own copy of the vendors.
#
# GLOSSARY stays local because core does not name it and the databricks runtime
# publishes no glossary at all — but its VALUE follows the domain, or the
# catalog reads as two things again.
GLOSSARY = "Contoso Commerce"

# The measures gold publishes, taken from gold/models/fct_daily_revenue.sql.
# A metric is a column PLUS how it aggregates, which is exactly what a column
# entity cannot express and what a report writer needs.
METRICS = [
    (
        "revenue",
        "SUM",
        "DOLLARS",
        "sum(o.amount)",
        "Money taken, summed over the order-line grain and grouped by trading day "
        "and country. The measure the semantic model serves to Power BI.",
    ),
    (
        "orders",
        "COUNT",
        "TRANSACTIONS",
        "count(*)",
        "Orders on a trading day, counted at order grain.",
    ),
    (
        "units",
        "SUM",
        "COUNT",
        "sum(o.quantity)",
        "Items sold, which moves independently of revenue when the mix changes.",
    ),
]

# dbt's generic tests, in OpenMetadata's ODCS vocabulary. The mapping is exact
# — `unique` IS duplicateValues == 0 — so this is a rename, not an
# interpretation. The tests that gate the build become the contract in the
# catalog, which is the same fact stated once.
DBT_TO_ODCS = {
    "unique": {"metric": "duplicateValues", "dimension": "uniqueness"},
    "not_null": {"metric": "nullValues", "dimension": "completeness"},
    "accepted_values": {"metric": "invalidValues", "dimension": "conformity"},
}

MEDALLION = [
    # Bronze: one entry per landed table. No upstreams here — what feeds bronze
    # is a VENDOR, and the vendor edges are registered from the connections the
    # ingest steps announced, not restated in this list.
    ("lakehouse", "bronze_customers", []),
    ("lakehouse", "bronze_orders", []),
    ("lakehouse", "bronze_web_customers", []),
    ("lakehouse", "bronze_web_products", []),
    ("lakehouse", "bronze_web_orders", []),
    ("lakehouse", "bronze_fx_rates", []),
    ("lakehouse", "bronze_product_hierarchy", []),
    ("lakehouse", "bronze_erp_changes", []),
    # Silver: upstreams are what the notebook actually reads for each table.
    ("lakehouse", "silver_customers", ["lakehouse.bronze_customers"]),
    ("lakehouse", "silver_orders", ["lakehouse.bronze_orders"]),
    ("lakehouse", "silver_quarantine_orders", ["lakehouse.bronze_orders"]),
    ("lakehouse", "silver_web_customers", ["lakehouse.bronze_web_customers"]),
    ("lakehouse", "silver_web_order_lines", ["lakehouse.bronze_web_orders"]),
    # The resolution: one row per PERSON, joined across both selling systems.
    (
        "lakehouse",
        "silver_party",
        ["lakehouse.silver_customers", "lakehouse.silver_web_customers"],
    ),
    ("lakehouse", "silver_fx_daily", ["lakehouse.bronze_fx_rates"]),
    (
        "lakehouse",
        "silver_product_hierarchy",
        ["lakehouse.bronze_product_hierarchy"],
    ),
    # Warehouse: upstreams mirror each dbt model's ref()/source() list — the
    # FROM clauses, not a diagram. `dim_product` really does read `fct_sales`
    # (its Unallocated members are derived from what actually sold), and
    # `dim_date` really is built from the fact's dates; stating anything
    # tidier would be the drawing, not the pipeline.
    ("warehouse", "dim_customer", ["lakehouse.silver_customers"]),
    ("warehouse", "dim_party", ["lakehouse.silver_party"]),
    ("warehouse", "dim_country", ["warehouse.dim_party"]),
    (
        "warehouse",
        "fct_orders",
        ["lakehouse.silver_orders", "lakehouse.silver_fx_daily"],
    ),
    (
        "warehouse",
        "fct_sales",
        [
            "warehouse.fct_orders",
            "lakehouse.silver_web_order_lines",
            "warehouse.dim_party",
            "lakehouse.silver_fx_daily",
        ],
    ),
    (
        "warehouse",
        "dim_product",
        ["lakehouse.silver_product_hierarchy", "warehouse.fct_sales"],
    ),
    ("warehouse", "dim_date", ["warehouse.fct_sales"]),
    (
        "warehouse",
        "fct_daily_revenue",
        ["warehouse.dim_customer", "warehouse.fct_orders"],
    ),
    # THE REPORTING STAR — the table the whole platform exists to serve, and
    # until this entry the catalog's one empty page: the demo toured a
    # `fct_revenue_summary` the catalog had never heard of.
    (
        "warehouse",
        "fct_revenue_summary",
        [
            "warehouse.fct_sales",
            "warehouse.dim_date",
            "warehouse.dim_product",
            "warehouse.dim_party",
        ],
    ),
]


def register_fabric(st: dict, lake_cols: dict, wh_cols: dict) -> dict[str, str]:
    """The lakehouse and warehouse tables, with columns read from what was built."""
    put(
        "services/databaseServices",
        {
            "name": FABRIC_SERVICE,
            "serviceType": "Mssql",
            "description": "The Fabric workspace: a Lakehouse reached through its "
            "SQL analytics endpoint, and a Warehouse. Both are T-SQL surfaces.",
            "connection": {
                "config": {
                    "type": "Mssql",
                    "scheme": "mssql+pyodbc",
                    "username": "entra",
                    "hostPort": "fabric-emulator:1433",
                    "database": st["workspace_name"],
                }
            },
        },
    )
    db = put("databases", {"name": st["workspace_name"], "service": FABRIC_SERVICE})
    fqns = {}
    for schema in ("lakehouse", "warehouse"):
        sch = put(
            "databaseSchemas",
            {"name": schema, "database": db["fullyQualifiedName"]},
        )
        for s_name, table, _ in MEDALLION:
            if s_name != schema:
                continue
            cols = (lake_cols if schema == "lakehouse" else wh_cols).get(table)
            assert cols, f"no columns discovered for {schema}.{table}"
            t = put(
                "tables",
                {
                    "name": table,
                    "databaseSchema": sch["fullyQualifiedName"],
                    "columns": cols,
                },
            )
            fqns[f"{schema}.{table}"] = t["fullyQualifiedName"]
    return fqns


def add_lineage(
    from_fqn: str, from_type: str, to_fqn: str, to_type: str, how: str = ""
) -> None:
    """`how` records HOW the movement is known, not merely that it happened.

    The emulator distinguishes an edge it WATCHED (its TDS front saw the engine
    accept the statement) from one a step REPORTED, and a catalog that flattens
    the two has discarded the only thing telling a consumer how far to trust the
    graph. Where the emulator recorded a producer, it is carried through
    verbatim; where this platform knows the mechanism itself — Debezium's change
    stream, an HTTP pull — it says so in its own words rather than borrowing a
    label that would imply the emulator observed it.
    """
    edge: dict = {
        "fromEntity": {"id": entity_id(from_type, from_fqn), "type": from_type},
        "toEntity": {"id": entity_id(to_type, to_fqn), "type": to_type},
    }
    if how:
        edge["lineageDetails"] = {"description": how}
    put("lineage", {"edge": edge})


def emulator_producers(st: dict) -> dict[tuple[str, str], str]:
    """What the emulator recorded, keyed by (source table, target table).

    The MEDALLION constant below declares the shape this platform intends. This
    reads the shape the emulator actually OBSERVED while the run happened — so a
    producer here is evidence, and a declared edge with no match is a hop
    nothing witnessed. Absent or unreachable, the catalog is still built; it
    just carries no provenance claim, which is the honest degradation.
    """
    from fabric import FABRIC_AUD, fabric, token

    # NO `/v1` HERE. `fabric()` prefixes it, and this call carried its own — so
    # every request went to /v1/v1/workspaces/... and 404'd. A 404 is not an
    # exception, so the guard below never fired; `.get("value", [])` turned it
    # into an empty dict, and every edge was labelled "the emulator recorded no
    # edge". The headline provenance number read 0 for as long as this existed,
    # and 0 is exactly what a working lookup would report for a platform that
    # only ever declares — which is why nobody questioned it.
    try:
        path = f"/workspaces/{st['workspace']}/lineage"
        r = fabric("GET", path, token(FABRIC_AUD))
    except Exception as e:  # provenance is a bonus, not a gate
        log(f"  ! emulator lineage unavailable ({type(e).__name__}) — unlabelled")
        return {}
    # STATUS FIRST. Reading `value` off a failed response is what made a broken
    # URL indistinguishable from an honest empty graph.
    if r.status_code != 200:
        log(
            f"  ! emulator lineage GET {path} -> {r.status_code} — provenance "
            f"unlabelled (this is a fault, not an empty graph)"
        )
        return {}
    edges = r.json().get("value", [])
    out = {}
    for e in edges:
        src, dst = e.get("sourcePath", ""), e.get("targetPath", "")
        if src.startswith("Tables/") and dst.startswith("Tables/"):
            key = (src.split("/", 1)[1], dst.split("/", 1)[1])
            out[key] = e.get("producer") or "Copy"
    return out


def _ref_target(to: str) -> str:
    """The model name inside dbt's `ref('...')`, or "" if it is not one.

    Gold's models all land in one schema, so the bare name is what a query in
    the contract can join to — the same assumption `{object}` already makes.
    """
    m = re.search(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""", to or "")
    return m.group(1) if m else ""


def _relationship_rule(column: str, arg: dict) -> dict:
    """dbt `relationships` → a rule that actually checks referential integrity.

    WHAT THIS USED TO EMIT, and why it was worse than emitting nothing:

        query: select count(*) from {object} where <column> is null

    That is a NOT-NULL check. It passes with every foreign key dangling, so
    long as none of them is NULL — and it was published as `<column>_resolves`,
    dimension `consistency`, described as "every <column> matches <to>.<field>".
    The name, the dimension and the description all claimed referential
    integrity; the query checked something else entirely.

    A contract is read by people deciding whether they need their own check.
    One that says a key resolves, when it has only confirmed the key is
    present, removes the reason to look without supplying the guarantee.

    THE ANTI-JOIN IS THE CHECK: rows whose key finds no match. `is not null` on
    the left keeps this about REFERENCES rather than presence — a NULL key is
    the not_null rule's business, and this rule failing for that reason would
    be the old confusion running the other way.
    """
    target, field = _ref_target(arg.get("to", "")), arg.get("field", "")
    if not (target and field):
        # HONEST FALLBACK. If the target cannot be resolved there is no join to
        # publish, so the rule says what it actually does and drops the
        # referential-integrity claim rather than keeping a name that implies
        # one. See the note above about what that costs a reader.
        return {
            "type": "sql",
            "name": f"{column}_present",
            "column": column,
            "dimension": "completeness",
            "description": (
                f"dbt `relationships` on {column}, reduced to a presence check: "
                f"its target ({arg.get('to')!r}) could not be resolved to a "
                f"table, so referential integrity is NOT asserted here."
            ),
            "query": f"select count(*) from {{object}} where {column} is null",
            "mustBe": 0,
        }
    return {
        "type": "sql",
        "name": f"{column}_resolves",
        "column": column,
        "dimension": "consistency",
        "description": (
            f"dbt `relationships` — every {column} matches {target}.{field}."
        ),
        "query": (
            f"select count(*) from {{object}} t "
            f"left join {target} r on t.{column} = r.{field} "
            f"where t.{column} is not null and r.{field} is null"
        ),
        "mustBe": 0,
    }


def dbt_quality_rules() -> dict[str, list[dict]]:
    """gold/models/schema.yml → ODCS quality rules, per model.

    These tests already gate the build: `dbt build` fails when one breaks.
    Publishing them as a contract states the same guarantee where a consumer
    can read it, without a second place to keep in step.
    """
    path = ROOT / "gold" / "models" / "schema.yml"
    if not path.is_file():
        return {}
    spec = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    out: dict[str, list[dict]] = {}
    for model in spec.get("models", []) or []:
        rules = []
        for col in model.get("columns", []) or []:
            for test in col.get("tests", []) or []:
                name = test if isinstance(test, str) else next(iter(test))
                mapped = DBT_TO_ODCS.get(name)
                if not mapped:
                    # `relationships` is referential integrity, which ODCS has
                    # no library metric for — it becomes a sql rule naming the
                    # join, rather than being dropped or forced onto a metric
                    # that means something else.
                    if name == "relationships":
                        arg = test[name]
                        rules.append(_relationship_rule(col["name"], arg))
                    continue
                desc = f"dbt `{name}` on {model['name']}.{col['name']}."
                rule = {
                    "type": "library",
                    "column": col["name"],
                    "mustBe": 0,
                    "unit": "rows",
                    "description": desc,
                    **mapped,
                }
                if name == "accepted_values":
                    rule["validValues"] = test[name].get("values", [])
                rules.append(rule)
        if rules:
            out[model["name"]] = rules
    return out


# OpenMetadata's routes are not uniformly `{type}s`. Guessing works for most
# and silently 404s for the rest, so the exceptions are written down.
ROUTES = {
    "table": "tables",
    "topic": "topics",
    "apiEndpoint": "apiEndpoints",
    "dashboardDataModel": "dashboard/datamodels",
}


def entity_id(kind: str, fqn: str) -> str:
    route = ROUTES.get(kind)
    assert route, f"no OpenMetadata route known for {kind!r} — add it to ROUTES"
    r = S.get(f"{OM}/{route}/name/{fqn}", timeout=60)
    assert r.status_code == 200, (kind, fqn, r.status_code, r.text[:200])
    return r.json()["id"]


def discover_columns(st: dict) -> tuple[dict, dict]:
    """Column lists read from what was actually built, on both sides.

    Lakehouse: the Delta schema, through the same engine that wrote it.
    Warehouse: INFORMATION_SCHEMA, through the container that has the driver.
    Neither is typed out here — a catalog that restates a schema is a second
    definition, and the two drift.
    """
    import spark as sparkmod

    from gold import in_dbt_container

    spark = sparkmod.session()
    base = sparkmod.lakehouse_uri(st["workspace"], st["lakehouse"])
    lake = {}
    for schema, table, _ in MEDALLION:
        if schema != "lakehouse":
            continue
        df = spark.read.format("delta").load(f"{base}/Tables/{table}")
        lake[table] = [
            {"name": f.name, "dataType": _om_type(f.dataType.simpleString())}
            for f in df.schema.fields
        ]

    rc = in_dbt_container("--entrypoint", "python", "dbt", "/tools/columns.py")
    assert rc == 0, f"warehouse column discovery failed: exit {rc}"
    raw = json.loads((ROOT / "gold" / "_columns.json").read_text(encoding="utf-8"))
    wh = {
        t: [{"name": c["name"], "dataType": _om_type(c["type"])} for c in cols]
        for t, cols in raw.items()
    }
    return lake, wh


def _om_type(native: str) -> str:
    """Map an engine type name onto OpenMetadata's vocabulary."""
    n = native.lower()
    if n.startswith(("int", "bigint", "smallint", "long")):
        return "INT"
    # DECIMAL BEFORE DOUBLE, and the order is the whole point. Money is stored
    # as decimal(19,4) precisely so it is not a binary float; a catalog that
    # published it as DOUBLE would tell every consumer the opposite of what the
    # warehouse actually guarantees. This branch used to be folded into the one
    # below, which was harmless while money was a float and became wrong the
    # moment it stopped being one.
    if n.startswith(("decimal", "numeric")):
        return "DECIMAL"
    if n.startswith(("double", "float", "real")):
        return "DOUBLE"
    # TIMESTAMP BEFORE DATE, for the same reason DECIMAL comes before DOUBLE:
    # `datetime2` starts with "date", so the date branch swallowed it and the
    # timestamp branch below was unreachable. Every datetime2 column was
    # catalogued as a DATE — a real type, plausibly wrong, and silent. Found by
    # the test that pins this mapping rather than by anything failing.
    if n.startswith(("timestamp", "datetime")):
        return "TIMESTAMP"
    if n.startswith(("date",)):
        return "DATE"
    # BOTH SPELLINGS. Spark says `boolean` and T-SQL says `bit`, and this
    # function is fed by both — Delta schemas for the lakehouse, and
    # INFORMATION_SCHEMA for the warehouse. Matching only "bool" meant every
    # warehouse boolean fell through to the STRING default: `is_cancelled`,
    # `rate_is_carried`, `in_pos` and `in_web` were all catalogued as text.
    if n.startswith(("bool", "bit")):
        return "BOOLEAN"
    # LAST RESORT, and worth knowing it is a guess. Anything unrecognised is
    # published as STRING, which is why a missing branch above shows up as a
    # plausible catalog entry rather than as an error.
    return "STRING"


def _pbi_service() -> str:
    """A dashboard service to hang the semantic model on.

    The model is a Power BI artifact, so it belongs to a dashboard service
    rather than a database one — which is also how a real Fabric workspace
    presents it.
    """
    put(
        "services/dashboardServices",
        {
            "name": "contoso-powerbi",
            "serviceType": "PowerBI",
            "connection": {
                "config": {
                    "type": "PowerBI",
                    "clientId": "contoso",
                    "clientSecret": "***",
                    "tenantId": "contoso",
                }
            },
        },
    )
    return "contoso-powerbi"


def main() -> int:
    login()
    r = S.get(f"{OM}/system/version", timeout=30)
    assert r.status_code == 200, (
        f"OpenMetadata is not reachable — `make govern` starts it ({r.status_code})"
    )

    st = state.load()
    pos_svc, endpoints = register_pos()
    erp_table = register_erp()
    topic = register_topic()

    lake_cols, wh_cols = discover_columns(st)
    tables = register_fabric(st, lake_cols, wh_cols)

    # --- the semantic layer, before the lineage that will reference it -------
    put(
        "domains",
        {
            "name": DOMAIN,
            "displayName": "Contoso Commerce",
            "domainType": "Consumer-aligned",
            "description": "Sales across Contoso's point-of-sale and ERP systems, "
            "conformed to one customer and one order grain.",
        },
    )
    put(
        "dataProducts",
        {
            "name": "contoso-sales-star",
            "displayName": "Contoso Sales star",
            "domains": [DOMAIN],
            "description": "The star served from the Warehouse and the semantic model "
            "over it — the layer downstream consumers may depend on.",
        },
    )
    put(
        "glossaries",
        {
            "name": GLOSSARY,
            "description": "Measures gold publishes, defined by the SQL that computes "
            "them. Prose definitions belong in a data contract beside "
            "the transform; this repository does not have one yet, and "
            "inventing the meaning here would put it in a second place.",
        },
    )
    for name, mtype, unit, expr, desc in METRICS:
        put(
            "glossaryTerms",
            {
                "name": name,
                "glossary": GLOSSARY,
                "description": f"{desc}\n\nComputed as `{expr}` in "
                f"gold/models/fct_daily_revenue.sql.",
            },
        )
        put(
            "metrics",
            {
                "name": name,
                "description": desc,
                "metricType": mtype,
                "unitOfMeasurement": unit,
                "domains": [DOMAIN],
                "granularity": "DAY",
                "metricExpression": {"language": "SQL", "code": expr},
            },
        )
    log(
        f"semantics: domain {DOMAIN!r}, 1 data product, {len(METRICS)} measures "
        f"as both glossary terms and metrics"
    )

    # --- contracts: the dbt tests, where a consumer can read them ------------
    contracts = dbt_quality_rules()
    n_rules = 0
    for model, rules in sorted(contracts.items()):
        fqn = tables.get(f"warehouse.{model}")
        if not fqn:
            continue
        put(
            "dataContracts",
            {
                "name": f"{model}-contract",
                "domains": [DOMAIN],
                "entity": {"id": entity_id("table", fqn), "type": "table"},
                "description": f"The guarantees `dbt build` enforces on {model}. "
                f"Derived from gold/models/schema.yml — the tests that "
                f"gate the build ARE the contract.",
                "odcsQualityRules": rules,
            },
        )
        n_rules += len(rules)
    log(
        f"contracts: {len(contracts)} DataContract(s) carrying {n_rules} rule(s) "
        f"from dbt's own tests"
    )

    observed = emulator_producers(st)

    # The hop that did not exist before: the vendor's change stream feeds the
    # ERP table's changes into the platform. Upstream of bronze, and therefore
    # upstream of every number the platform reports.
    add_lineage(
        erp_table,
        "table",
        topic,
        "topic",
        how="Debezium change stream — the platform's own connector, not "
        "an emulator observation",
    )

    # And the rest of the chain, so a number in the semantic model traces all
    # the way back to a vendor rather than to a file that appeared in landing.
    edges = 1
    add_lineage(
        topic,
        "topic",
        tables["lakehouse.bronze_erp_changes"],
        "table",
        how="Consumed from the change stream by ingest_erp_cdc",
    )
    edges += 1
    for ep in endpoints:
        for bronze in ("lakehouse.bronze_customers", "lakehouse.bronze_orders"):
            add_lineage(
                ep,
                "apiEndpoint",
                tables[bronze],
                "table",
                how="Pulled over HTTP by ingest_pos and landed verbatim",
            )
            edges += 1
    labelled = 0
    for schema, table, upstreams in MEDALLION:
        for up in upstreams:
            producer = observed.get((up.split(".", 1)[1], table))
            how = (
                f"Fabric {producer} — observed by the emulator"
                if producer
                else "Declared by the platform; the emulator recorded no edge"
            )
            labelled += 1 if producer else 0
            add_lineage(
                tables[up], "table", tables[f"{schema}.{table}"], "table", how=how
            )
            edges += 1
    dataset = st.get("dataset")
    if dataset:
        model = put(
            "dashboard/datamodels",
            {
                "name": "ContosoRevenue",
                "service": _pbi_service(),
                "dataModelType": "PowerBIDataModel",
                "columns": [
                    {"name": c, "dataType": "STRING"}
                    for c in ("OrderDate", "Country", "Orders", "Units", "Revenue")
                ],
            },
        )
        if model.get("fullyQualifiedName"):
            add_lineage(
                tables["warehouse.fct_daily_revenue"],
                "table",
                model["fullyQualifiedName"],
                "dashboardDataModel",
            )
            edges += 1

    catalogued = {
        "lineage_edges": edges,
        "fabric_tables": sorted(tables),
        "api_service": pos_svc,
        "api_endpoints": endpoints,
        "erp_table": erp_table,
        "topic": topic,
    }
    (ROOT / "catalog.json").write_text(
        json.dumps(catalogued, indent=2), encoding="utf-8"
    )
    state.save(catalog=sorted(catalogued))

    log(
        f"catalogued source systems: {pos_svc} ({len(endpoints)} endpoints), "
        f"{erp_table}, {topic}"
    )
    log(
        f"catalogued the medallion: {len(tables)} tables, {edges} lineage edges "
        f"({labelled} carrying a producer the emulator observed)"
    )
    # Direction matters and is easy to state backwards: Debezium READS the ERP
    # table and PUBLISHES to the topic, so data flows table -> topic. An edge
    # drawn the other way would claim the platform writes to the vendor — and
    # the first version of this log line said exactly that, while the edge
    # itself was correct.
    log(
        f"lineage: {erp_table} -> {topic} — the vendor is a node now, "
        f"not a filename in Files/landing"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
