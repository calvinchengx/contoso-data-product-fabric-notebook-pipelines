# Fabric notebook source
#
# THIS FILE IS A FABRIC NOTEBOOK. Its bytes are uploaded verbatim as the
# `notebook-content.py` part of a Notebook item, and Fabric's own notebook
# format is exactly this: a `# Fabric notebook source` header followed by
# sections delimited by `# CELL ****`. Nothing converts it and nothing
# generates it — what runs on Spark is the file you are reading.
#
# WHY BRONZE IS A NOTEBOOK NOW. It always claimed to be one: "Spark reads
# `abfs://…/Files/landing/…` itself, which is what a Fabric notebook or a Spark
# Job Definition does". But it ran in the platform's own process over Spark
# Connect, and NO FABRIC TENANT EXPOSES A SPARK CONNECT ENDPOINT — so the step
# could not have run in production at all. The transform is unchanged; what
# changed is that Fabric now decides where it runs.
#
# Inside a notebook `spark` is ambient: Fabric's pool binds it to a session that
# already carries the workspace identity and the attached lakehouse. So this file
# never builds a session, and building a second one inside a notebook is the
# thing that rule exists to prevent.
#
# BRONZE PARSES AND NOTHING MORE. No dedupe, no conforming, no quarantine —
# those are silver's job, and doing them here would destroy the only copy of what
# the vendor actually sent.

# CELL ********************

# The parameters cell. Real Fabric would override these per run through the
# job's `executionData.parameters`; the emulator implements no parameter
# override, so the platform substitutes them into this cell before publishing
# (see bronze.py). The placeholders are never valid ids, so a notebook published
# without substitution fails loudly on its first read instead of quietly
# resolving somewhere else.
WORKSPACE = "@@WORKSPACE@@"
LAKEHOUSE = "@@LAKEHOUSE@@"
DAY = "@@DAY@@"

# The real Fabric scheme, on both targets: the ENGINE resolves this, not the
# client, and Sail is configured against the emulator's storage endpoint.
BASE = f"abfs://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}"
LANDING = f"{BASE}/Files/landing"
TABLES = f"{BASE}/Tables"

# The Contoso Web page schemas, rendered from `web_schema.py` and substituted in.
#
# WHY SUBSTITUTED RATHER THAN IMPORTED. A notebook cannot import a sibling module
# out of `platform/`, and shipping `web_schema.py` as an extra definition part
# would not put it on the engine's path either. Inlining a COPY of the field
# names here would be worse than either: a test already pins those names to
# `sources/contoso-web/openapi.yaml`, and a second copy the test cannot see is
# exactly how a renamed vendor field turns into a column of nulls that every
# downstream count agrees with. So the module stays the single source and the
# rendered DDL travels through the same mechanism as the ids.
WEB_CUSTOMER_DDL = "@@WEB_CUSTOMER@@"
WEB_PRODUCT_DDL = "@@WEB_PRODUCT@@"
WEB_ORDER_DDL = "@@WEB_ORDER@@"


# The declared leaf names, for the "did anything parse at all" check below.
# Substituted from the same module, so this cannot disagree with the DDL above.
#
# `.split(",")` on what LOOKS like a literal, and ruff's SIM905 is right about the
# syntax and wrong about this file: the literal is a placeholder, replaced with a
# comma-joined list before publishing, so a list literal cannot be written here.
# Split through a function rather than inline: splitting a literal yields
# `list[LiteralString]`, and `list` is invariant, so it will not pass as the
# `list[str]` the product declares. The parameter re-types it.
def _fields(spec: str) -> list[str]:
    return spec.split(",")


WEB_CUSTOMER_FIELDS = _fields("@@WEB_CUSTOMER_FIELDS@@")
WEB_PRODUCT_FIELDS = _fields("@@WEB_PRODUCT_FIELDS@@")
WEB_ORDER_FIELDS = _fields("@@WEB_ORDER_FIELDS@@")

# CELL ********************

from contoso_product import run_bronze

# THE TRANSFORM IS THE PRODUCT'S. `contoso_product` reaches this Spark session
# through the Environment named in the META block below, not from the client
# that published this notebook: the engine is a different machine.
#
# WHAT USED TO BE HERE. Two hundred and fifty lines that were a copy of
# `contoso_product/bronze.py`, including the `bronze_ingest_metrics` table and
# its typed schema, which the product writes itself.
#
# `spark` is ambient: Fabric's pool binds it to a session that already carries
# the workspace identity and the attached lakehouse, so this file never builds
# one.
metrics = run_bronze(
    spark,
    landing=LANDING,
    tables=TABLES,
    day=DAY,
    web_customer_ddl=WEB_CUSTOMER_DDL,
    web_product_ddl=WEB_PRODUCT_DDL,
    web_order_ddl=WEB_ORDER_DDL,
    web_customer_fields=WEB_CUSTOMER_FIELDS,
    web_product_fields=WEB_PRODUCT_FIELDS,
    web_order_fields=WEB_ORDER_FIELDS,
)

print(
    f"bronze: {metrics['bronze_customers']} POS customer rows "
    f"({metrics['distinct_customers']} distinct), "
    f"{metrics['bronze_orders']} POS order events, "
    f"{metrics['bronze_web_customers']} web accounts, "
    f"{metrics['bronze_web_orders']} web orders, "
    f"{metrics['bronze_erp_changes']} ERP change events"
)

# METADATA ********************

# META {
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "@@ENVIRONMENT@@",
# META       "workspaceId": "@@WORKSPACE@@"
# META     }
# META   }
# META }
