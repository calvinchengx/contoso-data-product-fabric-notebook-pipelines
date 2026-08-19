"""Ask the report's question twice, and refuse to ship if the answers differ.

THE GATE THIS PLATFORM WAS MISSING. Everything upstream proves SHAPE: the model
parses, the catalog has the table, the dashboard opens. None of it proves the
number on the tile is the number in the warehouse. A report that renders
beautifully and totals wrong passes every other check in this repository.

So: the same question is asked of two surfaces that share nothing.

  DAX   -> the semantic model, over `executeQueries`, the way Power BI asks
  SQL   -> `fct_revenue_summary` directly, over TDS, with no model involved

The DAX side goes through measures, relationships and the fiscal grain; the SQL
side is a GROUP BY. If a measure gains a filter, a relationship flips
direction, or a column is remapped, these two stop agreeing — which is the
whole point. Two checks that share a code path would only prove the code path
agrees with itself.

MONEY IS COMPARED AS DECIMAL. The warehouse stores it as decimal(19,4)
deliberately; comparing with float tolerance here would reintroduce exactly the
error the schema was chosen to avoid. The tolerance is one cent, expressed as a
Decimal, and it is a tolerance for JSON round-tripping — not for arithmetic
that has gone wrong.
"""

from __future__ import annotations

import decimal
import json
import pathlib
import sys

from say import log

# `state`, `fabric` and `gold` resolve a target, which needs the `fabric-target`
# wheel that `make fixtures` installs from a pinned release. Imported inside the
# functions that talk to a running stack rather than at module scope, so that
# `compare` — a pure comparison over two dicts, and the part of this gate that
# decides pass or fail — stays importable on a clean checkout and stays tested
# in CI. It was not: this module's import chain turned `make test` red on all
# three platforms.

ROOT = pathlib.Path(__file__).resolve().parent.parent
SQL_OUT = ROOT / "gold" / "_reconcile_sql.json"

PBI_AUD = "https://analysis.windows.net/powerbi/api"

# The report's headline, in the model's own vocabulary. Deliberately the query
# the tour types on camera: what the demo shows is what this gate checks.
DAX = (
    "EVALUATE SUMMARIZECOLUMNS(Reporting[FiscalQuarterLabel], "
    '"Revenue USD", [Revenue USD], '
    '"Cancelled", [Cancelled Revenue])'
)

# One cent. Both sides read decimal(19,4) columns; this absorbs the JSON
# round-trip, not a disagreement about the arithmetic.
TOLERANCE = decimal.Decimal("0.01")
CENTS = decimal.Decimal("0.01")


def dax_side(dataset: str, workspace: str) -> dict[str, dict[str, decimal.Decimal]]:
    """The model's answer, through the Power BI wire."""
    from fabric import FABRIC, S, ensure_audience, token

    ensure_audience(PBI_AUD, "Power BI Service")
    url = f"{FABRIC}/v1.0/myorg/groups/{workspace}/datasets/{dataset}/executeQueries"
    r = S.post(
        url,
        headers={"Authorization": f"Bearer {token(PBI_AUD)}"},
        json={"queries": [{"query": DAX}]},
        timeout=120,
    )
    assert r.status_code == 200, (r.status_code, r.text[:300])
    rows = r.json()["results"][0]["tables"][0]["rows"]
    assert rows, f"the model returned no rows: {r.text[:300]}"
    # str() before Decimal: the JSON number is already a float by the time it
    # arrives, and Decimal(float) would preserve the binary artefact instead of
    # the value anyone means.
    return {
        row["Reporting[FiscalQuarterLabel]"]: {
            "revenue_usd": decimal.Decimal(str(row["[Revenue USD]"])),
            "cancelled_revenue_usd": decimal.Decimal(str(row["[Cancelled]"])),
        }
        for row in rows
    }


def sql_side() -> dict[str, dict[str, decimal.Decimal]]:
    """The warehouse's answer, with no semantic layer in the way."""
    from gold import in_dbt_container

    SQL_OUT.unlink(missing_ok=True)
    rc = in_dbt_container("--entrypoint", "python", "dbt", "/tools/reconcile_sql.py")
    assert rc == 0, f"warehouse reconciliation query failed: exit {rc}"
    rows = json.loads(SQL_OUT.read_text(encoding="utf-8"))
    return {
        row["quarter"]: {
            "revenue_usd": decimal.Decimal(row["revenue_usd"]),
            "cancelled_revenue_usd": decimal.Decimal(row["cancelled_revenue_usd"]),
        }
        for row in rows
    }


def compare(
    model: dict[str, dict[str, decimal.Decimal]],
    warehouse: dict[str, dict[str, decimal.Decimal]],
) -> list[str]:
    """Every difference, named. Returns the failures; empty means agreement."""
    problems = []

    # A quarter present on one side only is the most dangerous disagreement
    # there is: the totals of the quarters they share can match perfectly while
    # the report silently omits a period.
    only_model = sorted(set(model) - set(warehouse))
    only_wh = sorted(set(warehouse) - set(model))
    if only_model:
        problems.append(
            f"the model reports quarters the warehouse does not: {only_model}"
        )
    if only_wh:
        problems.append(f"the warehouse has quarters the model does not: {only_wh}")

    for quarter in sorted(set(model) & set(warehouse)):
        for field in ("revenue_usd", "cancelled_revenue_usd"):
            m, w = model[quarter][field], warehouse[quarter][field]
            delta = abs(m - w)
            status = "ok" if delta <= TOLERANCE else "MISMATCH"
            # Quantised for the LOG only, never for the comparison: the DAX
            # side arrives over JSON as a float, so it prints as
            # 2,798,339.2800000003 — which reads like a discrepancy when it is
            # the wire format. The comparison above still sees the full value.
            log(
                f"  {quarter:<8} {field:<24} "
                f"model={m.quantize(CENTS):>18,} "
                f"warehouse={w.quantize(CENTS):>18,}  {status}"
            )
            if delta > TOLERANCE:
                problems.append(
                    f"{quarter} {field}: model {m} vs warehouse {w} (off by {delta})"
                )
    return problems


def main() -> int:
    import state

    st = state.load()
    dataset = st.get("dataset")
    assert dataset, "no semantic model published yet — run `make verify` first"

    log("reconciling the semantic model against the warehouse")
    model = dax_side(dataset, st["workspace"])
    warehouse = sql_side()
    problems = compare(model, warehouse)

    if problems:
        log(f"RECONCILE FAILED — {len(problems)} disagreement(s):")
        for p in problems:
            log(f"  - {p}")
        return 1

    total = sum(v["revenue_usd"] for v in model.values())
    log(
        f"RECONCILED {len(model)} fiscal quarter(s), two surfaces agree — "
        f"total revenue {total:,}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
