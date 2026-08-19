"""The platform runs when nobody is watching.

Every other step in this repository runs because a person typed `make verify`.
That proves the platform WORKS; it does not prove it OPERATES. In production
nothing types anything — a schedule fires at 02:00 and the medallion refreshes,
and if that mechanism is broken the failure is silent in the worst way: the
data is simply yesterday's, and every table, row count and dashboard still
looks entirely correct.

So this step schedules the silver notebook the way an operator would, and then
asks the only question that matters about a schedule: did it actually produce a
run?

THE HARD PART IS TIME, and it is why nothing in either repository exercised
this before. A schedule's whole behaviour is "wait, then act", and a test that
waits an hour is not a test. Real Fabric's clock is the world's. The emulator's
is controllable — advance it and every occurrence due in the window fires
through the same code path a manual run uses — so the firing becomes an
assertion instead of a hope.

Both targets therefore create the schedule, read it back and check that Fabric
kept what was sent. Only the "and then it fired" half is emulator-only, gated
by `T.clock_is_controllable`. On real Fabric the schedule is left in place,
which is the honest end state: it will fire at the hour it says, and nothing
here pretends to have watched it.
"""

from __future__ import annotations

import datetime
import json
import time

import state
from fabric import FABRIC_AUD, S, T, fabric, log, token

JOB_TYPE = "RunNotebook"

# Quarter-hourly, and the number is CONSTRAINED rather than chosen.
#
# The obvious cadence is hourly, and it does not work. Only the Fabric
# emulator's clock moves when this step advances it — the Entra emulator that
# mints the tokens keeps its own — so after a jump the two disagree, and Fabric
# measures every token against a clock the issuer knows nothing about. Tokens
# live 3600s. Advance further than that and every subsequent call 401s with
# `invalid token: expired`, including freshly minted ones, because the new
# token is already born expired from Fabric's point of view. It reads as an
# authentication fault and is really the consequence of the lever this step
# exists to pull.
#
# So the whole advance has to fit inside one token lifetime. Fifteen minutes is
# a plausible refresh cadence and leaves a wide margin.
INTERVAL_MINUTES = 15

# One interval plus a margin, sized off the interval rather than written as a
# number, so changing the cadence cannot silently produce an advance that no
# longer crosses an occurrence.
ADVANCE_SECONDS = INTERVAL_MINUTES * 60 + 300

# The token lifetime the identity provider issues. Asserted, not trusted: if
# the cadence above is ever raised past it, this fails at import with the
# reason, instead of 401ing three calls later.
TOKEN_LIFETIME_SECONDS = 3600
assert ADVANCE_SECONDS < TOKEN_LIFETIME_SECONDS, (
    f"advancing {ADVANCE_SECONDS}s outruns the {TOKEN_LIFETIME_SECONDS}s token "
    f"lifetime — Fabric's clock would move past the expiry of every token "
    f"Entra can mint, and the step would fail as an auth error"
)


def iso(epoch: float) -> str:
    """RFC 3339 in UTC, which is what the Job Scheduler parses."""
    return (
        datetime.datetime.fromtimestamp(epoch, datetime.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def now() -> float:
    """The clock the SCHEDULE will be judged against, not this machine's.

    On the emulator those differ: its clock carries an offset and can be
    frozen, so a window computed from local wall time can sit entirely in the
    emulator's past or future — and a schedule that never fires looks exactly
    like a scheduler that does not work.
    """
    if not T.clock_is_controllable:
        return datetime.datetime.now(datetime.UTC).timestamp()
    r = S.get(f"{T.api_root}/_emulator/clock", timeout=30)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    return float(r.json()["now"])


def ensure(tok: str, workspace: str, item: str, when: float) -> dict:
    """Resolve-or-create the schedule, like every other object this platform owns.

    Not merely idempotence for its own sake: Fabric caps an item at 20
    schedules per job type, so a step that created one per run would work
    twenty times and then start failing with a quota error that names nothing
    about this code.
    """
    base = f"/workspaces/{workspace}/items/{item}/jobs/{JOB_TYPE}/schedules"
    config = {
        "type": "Cron",
        "interval": INTERVAL_MINUTES,
        # Starting just BEFORE now, so the first occurrence is due almost
        # immediately. A window opening in the future would need the clock
        # advanced twice and would say nothing more.
        "startDateTime": iso(when - 60),
        "endDateTime": iso(when + 86400),
        # A Windows time-zone id in real Fabric; UTC is spelled the same in
        # both worlds, which is the whole reason to use it here.
        "localTimeZoneId": "UTC",
    }
    body = {"enabled": True, "configuration": config}

    existing = fabric("GET", base, tok)
    assert existing.status_code == 200, (existing.status_code, existing.text[:200])
    found = existing.json().get("value", [])
    if found:
        sid = found[0]["id"]
        r = fabric("PATCH", f"{base}/{sid}", tok, json=body)
        assert r.status_code == 200, (r.status_code, r.text[:300])
        log(f"reusing schedule {sid} on the silver notebook")
        return r.json()

    r = fabric("POST", base, tok, json=body)
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])
    created = r.json()
    log(f"scheduled the silver notebook every {INTERVAL_MINUTES}m ({created['id']})")
    return created


def scheduled_runs(tok: str, workspace: str, item: str) -> list[dict]:
    """Job instances this item ran BECAUSE OF a schedule.

    `invokeType` is the whole assertion. A scheduled run and a manual one are
    the same job doing the same work, and this field is the only thing that
    distinguishes them — so filtering on it is what makes the claim "the
    scheduler produced this" rather than "a run exists, and one was expected".
    """
    r = fabric("GET", f"/workspaces/{workspace}/items/{item}/jobs/instances", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    return [j for j in r.json().get("value", []) if j.get("invokeType") == "Scheduled"]


TERMINAL = ("Completed", "Failed", "Cancelled", "Deduped")


def in_flight(tok: str, workspace: str, item: str) -> list[dict]:
    """Job instances of this item that have not reached a terminal state."""
    r = fabric("GET", f"/workspaces/{workspace}/items/{item}/jobs/instances", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    return [j for j in r.json().get("value", []) if j.get("status") not in TERMINAL]


def await_quiet(tok: str, workspace: str, item: str, timeout: int = 180) -> None:
    """Wait until nothing is running this notebook, BEFORE creating a schedule.

    The emulator evaluates due schedules the moment one is created, so the
    first occurrence fires immediately — and the previous step leaves an
    event-triggered run in flight. Both write the same silver Delta tables, and
    Delta's optimistic concurrency does exactly what it should: one commits and
    the other dies with `Failed to commit transaction: 0`.

    That is not a bug to engineer around in the emulator, and NOT one to hide.
    Real Fabric would collide identically — a scheduled and a triggered run of
    one notebook are not mutually excluded there either. It is this pipeline
    racing itself, in a step whose claim is "a schedule fires unattended".
    Proving that does not require two writers, so it no longer has two.
    """
    for _ in range(timeout):
        running = in_flight(tok, workspace, item)
        if not running:
            return
        time.sleep(1)
    raise AssertionError(
        f"{item} still has {len(in_flight(tok, workspace, item))} run(s) in "
        f"flight after {timeout}s; scheduling now would race them"
    )


def await_terminal(
    tok: str, workspace: str, item: str, job: str, timeout: int = 180
) -> dict:
    """Poll one job instance to a terminal state and return it."""
    base = f"/workspaces/{workspace}/items/{item}/jobs/instances/{job}"
    for _ in range(timeout):
        r = fabric("GET", base, tok)
        assert r.status_code == 200, (r.status_code, r.text[:200])
        detail = r.json()
        if detail.get("status") in TERMINAL:
            return detail
        time.sleep(1)
    raise AssertionError(f"scheduled job {job} never reached a terminal state")


def advance(seconds: int) -> None:
    """Move the emulator's clock. Emulator-only, by construction — real Fabric
    has no such lever and this is never called against it."""
    assert T.clock_is_controllable, "the clock is not controllable on this target"
    r = S.post(f"{T.api_root}/_emulator/clock", json={"advance": seconds}, timeout=30)
    assert r.status_code == 200, (r.status_code, r.text[:200])


def reset_clock() -> None:
    """Put the clock back where it was found.

    An offset clock is not a local detail: only Fabric's moves, while the Entra
    emulator that mints tokens keeps its own, so a stack left advanced hands
    out tokens Fabric reads as already expired. Leaving that behind would make
    the next unrelated command fail with an authentication error.
    """
    assert T.clock_is_controllable, "the clock is not controllable on this target"
    r = S.post(f"{T.api_root}/_emulator/clock", json={"offset": 0}, timeout=30)
    assert r.status_code == 200, (r.status_code, r.text[:200])


def main() -> int:
    st = state.load()
    tok = token(FABRIC_AUD)
    ws, notebook = st["workspace"], st["silver_notebook"]

    # Nothing else may be writing silver when the schedule is created: the
    # first occurrence fires immediately, and step 13's trigger run is still
    # going. See await_quiet.
    await_quiet(tok, ws, notebook)

    when = now()
    created = ensure(tok, ws, notebook, when)

    # Read it back through a separate GET rather than trusting the create
    # response. What Fabric STORED is the thing that will fire; what it echoed
    # is only what it received.
    base = f"/workspaces/{ws}/items/{notebook}/jobs/{JOB_TYPE}/schedules"
    r = fabric("GET", f"{base}/{created['id']}", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    stored = r.json()
    assert stored["enabled"] is True, stored
    assert stored["configuration"]["type"] == "Cron", stored
    assert stored["configuration"]["interval"] == INTERVAL_MINUTES, stored
    log(f"schedule stored and read back: {json.dumps(stored['configuration'])}")

    if not T.clock_is_controllable:
        # The honest end state on real Fabric. Saying so out loud matters: a
        # step that silently skipped its only real assertion would report
        # success for a scheduler nobody exercised.
        log(
            "real target: the schedule is created and verified; its firing "
            "happens at the hour it says and is not asserted here"
        )
        state.save(schedule={"id": created["id"], "fired": None})
        return 0

    # Already one, and that is not a bug to route around: the emulator
    # evaluates due schedules when one is created, and the window opens a
    # minute before now, so the first occurrence fires immediately. Counting
    # before and after is what makes the assertion about THIS advance rather
    # than about whatever the create happened to do.
    before = {j["id"] for j in scheduled_runs(tok, ws, notebook)}

    # AND AGAIN, because creating the schedule fired one. `await_quiet` above
    # cleared step 13's trigger run; this clears the occurrence the create
    # itself produced. Without it the advance starts a second run seconds later
    # in REAL time — the two are 15 virtual minutes apart and simultaneous in
    # practice — and they race for the same silver tables. That is exactly how
    # this step recorded `Failed to commit transaction: 0` while reporting
    # success. Compressing time compresses the executions too, which is the one
    # property of a controllable clock that has no real-Fabric counterpart.
    await_quiet(tok, ws, notebook)

    advance(ADVANCE_SECONDS)

    # A fresh token. The advance is bounded to stay inside one token lifetime,
    # but the one above was already minted some seconds ago and the jump eats
    # most of the remaining margin — re-minting costs one call and removes the
    # whole question.
    tok = token(FABRIC_AUD)

    # No polling loop. The emulator evaluates due schedules when the clock
    # moves and when job instances are listed, so by the time this read returns
    # the occurrence has either fired or never will — waiting would only make a
    # broken scheduler take longer to report.
    after = scheduled_runs(tok, ws, notebook)
    # BY IDENTITY, never by position. The API returns job instances
    # NEWEST-FIRST, so `after[-1]` is the OLDEST scheduled run — the occurrence
    # the create fired, not the one this advance produced. The outcome
    # assertion below was therefore reading a different job's status, and a
    # scheduled run that died mid-notebook passed as green. A set difference
    # cannot pick the wrong one whichever way the list is ordered.
    fresh = [j for j in after if j["id"] not in before]

    # WAIT FOR THE RUN INSIDE THE ADVANCED FRAME, then always put time back.
    #
    # The clock must be reset even when this step fails, or the stack is left
    # an interval ahead of the identity provider that mints its tokens and the
    # next unrelated command dies with `invalid token: expired`, pointing at
    # nothing. That is what `finally` is for.
    #
    # But the reset cannot come FIRST. The fired job was created while the
    # clock was advanced, so its startTimeUtc is in that frame; resetting
    # before it finishes stamps its endTimeUtc in the old one and the instance
    # comes back with an end BEFORE its start. That is not an emulator defect —
    # it is what moving a clock backwards under a running job means, and the
    # emulator is reporting both timestamps exactly as they were taken.
    try:
        assert fresh, (
            f"the clock advanced {ADVANCE_SECONDS}s past an occurrence and no "
            f"NEW scheduled run appeared ({len(before)} before, {len(after)} "
            f"after) — the schedule exists but does not fire, which is the "
            f"failure this step was written to catch"
        )
        fired = fresh[0]
        assert fired["jobType"] == JOB_TYPE, fired

        # THE OUTCOME, not just the existence. This step used to assert that a
        # job with invokeType=Scheduled appeared and stop there — so it logged
        # "the platform runs unattended" over a run that had died mid-notebook,
        # and the pipeline reported 14/14. A schedule that reliably starts
        # something that reliably fails is not unattended operation; it is an
        # alarm nobody wired up. `make verify` was green across two such
        # failures before this line existed.
        detail = await_terminal(tok, ws, notebook, fired["id"])
    finally:
        reset_clock()

    assert detail.get("status") == "Completed", (
        f"the schedule fired job {fired['id']} and it ended "
        f"{detail.get('status')!r}: {detail.get('failureReason')}"
    )
    assert detail["endTimeUtc"] >= detail["startTimeUtc"], (
        f"job {fired['id']} ended before it started "
        f"({detail['startTimeUtc']} -> {detail['endTimeUtc']}) — the clock was "
        f"moved backwards while it was still running"
    )

    state.save(schedule={"id": created["id"], "fired": fired["id"]})
    log(
        f"the schedule FIRED and the run COMPLETED: job {fired['id']} on the "
        f"silver notebook, invokeType=Scheduled — the platform runs unattended"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
