"""The reconciliation gate's comparison, tested without a running stack.

`make reconcile` needs the whole platform up; this does not. What it pins is
the part that decides pass or fail, because a gate whose comparison quietly
stops comparing is worse than no gate — every downstream run goes green.
"""

import decimal
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "steps"))

import reconcile

D = decimal.Decimal


def figures(revenue: str, cancelled: str = "1.00") -> dict:
    return {"revenue_usd": D(revenue), "cancelled_revenue_usd": D(cancelled)}


def test_agreement_reports_no_problems():
    both = {"FY27 Q2": figures("129303176.01")}
    assert reconcile.compare(both, both) == []


def test_a_disagreement_beyond_a_cent_is_reported():
    model = {"FY27 Q2": figures("129303176.01")}
    warehouse = {"FY27 Q2": figures("129303177.01")}
    problems = reconcile.compare(model, warehouse)
    assert len(problems) == 1
    assert "FY27 Q2 revenue_usd" in problems[0]


def test_a_cent_of_wire_noise_is_tolerated():
    # The DAX side crosses JSON as a float; a hundredth is the round trip, not
    # a disagreement. A gate that failed here would be unusable and would be
    # switched off, which is the worst outcome of all.
    model = {"FY27 Q2": figures("129303176.02")}
    warehouse = {"FY27 Q2": figures("129303176.01")}
    assert reconcile.compare(model, warehouse) == []


def test_a_quarter_missing_from_one_side_is_reported():
    # THE DANGEROUS CASE. Every shared quarter can agree perfectly while the
    # report silently omits a period — totals that match on what is shown say
    # nothing about what is not.
    model = {"FY27 Q2": figures("1.00")}
    warehouse = {"FY27 Q2": figures("1.00"), "FY27 Q1": figures("2.00")}
    problems = reconcile.compare(model, warehouse)
    assert len(problems) == 1
    assert "FY27 Q1" in problems[0]
    assert "the warehouse has quarters the model does not" in problems[0]
