"""The snapshot's refusals, tested without a running stack.

`make snapshot` needs the whole platform up; this does not. What it pins is the
part that decides whether a file is published at all, because the family
comparison is only as good as the snapshots feeding it — and a snapshot of
nothing is what makes two runtimes that built nothing compare equal.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "steps"))

import snapshot as snap

FULL = {
    "revenue_usd": "129303176.0100",
    "cancelled_revenue_usd": "2800504.4000",
    "sale_lines": "474044",
    "contracts": ["money_is_never_stored_as_float"],
    "runtime": "fabric",
    "catalog": "wh-1",
}


def _run(monkeypatch, tmp_path, body: dict, container_rc: int = 0, state=None):
    """Drive main() with the container step stubbed out."""
    state = {"warehouse": "wh-1"} if state is None else state
    src, out = tmp_path / "_snapshot.json", tmp_path / "product_snapshot.json"
    src.write_text(json.dumps(body), encoding="utf-8")
    monkeypatch.setattr(snap, "FROM_CONTAINER", src)
    monkeypatch.setattr(snap, "OUT", out)
    fake_gold = type("m", (), {"in_dbt_container": lambda *a: container_rc})
    monkeypatch.setitem(sys.modules, "gold", fake_gold)
    monkeypatch.setitem(
        sys.modules, "state", type("m", (), {"load": staticmethod(lambda: state)})
    )
    return snap.main(), out


def test_a_real_snapshot_is_published(monkeypatch, tmp_path):
    rc, out = _run(monkeypatch, tmp_path, FULL)
    assert rc == 0
    assert json.loads(out.read_text(encoding="utf-8"))["sale_lines"] == "474044"


def test_an_all_zero_snapshot_is_refused(monkeypatch, tmp_path):
    """Two runtimes that built nothing must not compare equal.

    compare_products refuses this too, but failing where the numbers were READ
    names the runtime that is empty instead of reporting a disagreement between
    two files.
    """
    rc, out = _run(
        monkeypatch,
        tmp_path,
        {**FULL, "revenue_usd": "0", "cancelled_revenue_usd": "0", "sale_lines": "0"},
    )
    assert rc == 1
    assert not out.exists()


def test_one_nonzero_aggregate_is_enough(monkeypatch, tmp_path):
    # A single zero is a legitimate value -- a period with no cancellations is
    # not an empty warehouse.
    rc, _ = _run(monkeypatch, tmp_path, {**FULL, "cancelled_revenue_usd": "0"})
    assert rc == 0


def test_a_snapshot_naming_no_contracts_is_refused(monkeypatch, tmp_path):
    rc, out = _run(monkeypatch, tmp_path, {**FULL, "contracts": []})
    assert rc == 1
    assert not out.exists()


def test_nothing_is_published_when_the_warehouse_read_fails(monkeypatch, tmp_path):
    # A stale file from a previous run would otherwise be republished as if it
    # described this one.
    rc, out = _run(monkeypatch, tmp_path, FULL, container_rc=3)
    assert rc == 3
    assert not out.exists()


def test_it_refuses_before_the_platform_has_a_warehouse(monkeypatch, tmp_path):
    # `make snapshot` before `make verify` should say so, not fail inside ODBC.
    rc, out = _run(monkeypatch, tmp_path, FULL, state={})
    assert rc == 1
    assert not out.exists()
