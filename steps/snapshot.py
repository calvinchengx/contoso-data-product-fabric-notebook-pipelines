"""Publish this runtime's gold numbers, for the family comparison.

`contoso-data-product/scripts/compare_products.py` holds every platform to the
same aggregates and the same contract names. It needs one snapshot per runtime,
and until now this platform wrote none -- so `--fabric`, which the tool
REQUIRES, had no producer in the repo it names, and the comparison could not be
run at all.

The reading happens in the dbt image (ODBC lives there); this step moves the
result to the path the family looks for.
"""

from __future__ import annotations

import json
import pathlib
import shutil
import sys

from say import log

ROOT = pathlib.Path(__file__).resolve().parent.parent
FROM_CONTAINER = ROOT / "gold" / "_snapshot.json"
OUT = ROOT / "product_snapshot.json"


def main() -> int:
    # IMPORTED HERE, NOT AT MODULE SCOPE. `state` reaches `fabric`, which needs
    # the `fabric-target` wheel from a pinned release -- deliberately absent
    # from uv.lock, because which release it came from is itself under test. A
    # module-level import would make this file unimportable on a clean checkout
    # and take tests/ down with it (test_the_repo_tests_need_no_fixture_wheels).
    import state

    from gold import in_dbt_container

    st = state.load()
    if not st.get("warehouse"):
        log("no warehouse in state -- run `make verify` first")
        return 1

    rc = in_dbt_container("--entrypoint", "python", "dbt", "/tools/snapshot.py")
    if rc != 0:
        log(f"could not read the gold snapshot: exit {rc}")
        return rc

    snapshot = json.loads(FROM_CONTAINER.read_text(encoding="utf-8"))

    # A SNAPSHOT OF NOTHING IS NOT A SNAPSHOT. compare_products refuses an
    # all-zero one, and it is better to fail where the numbers were read than
    # to publish a file that makes the comparison fail somewhere else.
    aggregates = ("revenue_usd", "cancelled_revenue_usd", "sale_lines")
    if not any(float(snapshot[k]) for k in aggregates):
        log("gold is empty -- refusing to publish a snapshot that says nothing")
        return 1
    if not snapshot["contracts"]:
        log("no contract tests found in gold/tests -- refusing to publish")
        return 1

    shutil.copyfile(FROM_CONTAINER, OUT)
    log(
        f"snapshot: revenue_usd={snapshot['revenue_usd']} "
        f"sale_lines={snapshot['sale_lines']} -> {OUT.name}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
