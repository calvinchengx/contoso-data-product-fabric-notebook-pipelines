# Fabric notebook source
#
# THIS FILE IS A FABRIC NOTEBOOK. Its bytes are uploaded verbatim as the
# `notebook-content.py` part of a Notebook item, and Fabric's own notebook
# format is exactly this: a `# Fabric notebook source` header followed by
# sections delimited by `# CELL ****`. Nothing converts it and nothing
# generates it — what runs on Spark is the file you are reading.
#
# WHY A FILE AND NOT A STRING. The obvious way to publish a notebook is an
# f-string in the step that submits it. Then the transform is not Python as far
# as any tool is concerned: ruff does not lint it, ty does not check it, and a
# syntax error surfaces as a failed cell on a Spark engine rather than at
# `make lint`. Keeping it a real module costs one substitution (below) and buys
# the whole toolchain.
#
# WHY IT LOOKS DIFFERENT FROM THE OTHER STEPS. Inside a notebook `spark` is
# ambient — Fabric's Spark pool binds it to a session that already carries the
# workspace identity and the attached lakehouse. So this file never calls
# `spark.session()`; using the ambient session IS the rule, and building a
# second one inside a notebook is the thing the rule exists to prevent.
#
# The transform itself is unchanged from when it ran as a plain Spark Connect
# script, which is the claim being tested: the platform's transforms are
# notebook code, not scripts that resemble notebook code.

# CELL ********************

# The parameters cell. Real Fabric would override these per run through the
# job's `executionData.parameters`; the emulator does not implement parameter
# overrides, so the platform substitutes the ids into this cell before
# publishing (see silver.py). The placeholders are never valid ids, so a
# notebook published without substitution fails loudly on its first read
# instead of quietly resolving somewhere else.
WORKSPACE = "@@WORKSPACE@@"
LAKEHOUSE = "@@LAKEHOUSE@@"

# The real Fabric scheme, on both targets: the ENGINE resolves this, not the
# client, and Sail is configured against the emulator's storage endpoint.
TABLES = f"abfs://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}/Tables"

# CELL ********************

import json

from contoso_product import run_silver

# THE TRANSFORM IS THE PRODUCT'S, and this notebook is the Fabric wrapper round
# it. `contoso_product` reaches this Spark session through the Environment named
# in the META block below; it is not installed in the client that published this
# notebook, and it could not be, because the client is a different machine.
#
# WHAT USED TO BE HERE. Four hundred lines that were a copy of
# `contoso_product/silver.py`: the same MONEY and RATE widths, the same COUNTRY
# map, the same dedupe, conform, quarantine and identity resolution. Its sibling
# in databricks-platform-jobs called the package; this one had a fork, and the
# fork had already drifted from the package it was copied from.
#
# `spark` is ambient. Fabric's pool binds it to a session that already carries
# the workspace identity and the attached lakehouse, so this file never builds
# one, and the product takes the session it is handed.
metrics = run_silver(spark, tables=TABLES)

# `lineage` is the product's record of what it read and wrote, in the precise
# `moves` form. It rides along in the exit value for whoever wants it; silver.py
# grades the counts.
notebookutils.notebook.exit(json.dumps(metrics))

# METADATA ********************

# META {
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "@@ENVIRONMENT@@",
# META       "workspaceId": "@@WORKSPACE@@"
# META     }
# META   }
# META }
