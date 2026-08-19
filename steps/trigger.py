"""The platform reacts when a vendor delivers.

A schedule answers "run at 02:00". It cannot answer "run when the data lands",
and for an external feed that is the question that matters: the vendor's export
finishes when it finishes, and a fixed hour either processes yesterday's file
or processes nothing. Event-driven ingestion is the other half of operating a
platform, and until this step existed nothing here exercised it.

WHAT FABRIC DOES. A Reflex (Data Activator) rule watches OneLake file events
and starts a job when one matches. The job reads the event through
`@pipeline()?.TriggerEvent?.FileName`, so the same definition works whether a
person, a schedule or a file started it.

THE ONE PLACE THIS PLATFORM CANNOT BE TARGET-NEUTRAL. Real Fabric has no public
REST for the BINDING — the Eventstream/Reflex rule is assembled in the portal,
by hand — so a deployment cannot declare it the way it declares a lakehouse or
a schedule. The emulator exposes an emulator-native surface for it and labels
it as such in its own parity table.

So this step is honest about a split it cannot close: against the emulator it
creates the binding and proves a dropped file really starts a job; against real
Fabric it says the binding is a portal task and asserts nothing, rather than
inventing a REST call that does not exist. Everything DOWNSTREAM of the binding
— the filter, the job, the `invokeType`, the TriggerEvent fields — is ordinary
Fabric on both targets.

THE MARKER FILE, and why the trigger does not watch the whole landing zone.
The POS export lands as 21 parts; a prefix covering them would fire 21 times
and start 21 refreshes of the same data. So the trigger watches one path that
means "the delivery is complete", and the step writes exactly that. It is the
`_SUCCESS`-marker convention every batch system converges on, for the same
reason: file-arrived and delivery-finished are different events, and only the
second one is worth acting on.
"""

from __future__ import annotations

import json

import provision
import state
from fabric import FABRIC_AUD, STORAGE_AUD, T, fabric, log, token, upload

REFLEX = "contoso-arrivals"
TRIGGER_NAME = "pos-delivery-complete"

# One path, not a prefix over the parts. See the module docstring.
MARKER = "Files/landing/contoso_pos/_delivery/complete.json"
WATCHED_PREFIX = "Files/landing/contoso_pos/_delivery"

EVENT_TYPE = "Microsoft.Fabric.OneLake.FileCreated"
JOB_TYPE = "RunNotebook"


def ensure_reflex(tok: str, workspace: str) -> str:
    """Resolve-or-create the Reflex the trigger hangs off, by NAME like every
    other item this platform owns."""
    found = provision.find_item(tok, workspace, REFLEX, "Reflex")
    if found:
        log(f"reusing reflex {REFLEX}")
        return found["id"]
    r = fabric(
        "POST",
        f"/workspaces/{workspace}/items",
        tok,
        json={"displayName": REFLEX, "type": "Reflex"},
    )
    assert r.status_code in (201, 202), (r.status_code, r.text[:300])
    created = r.json()
    log(f"created reflex {REFLEX}")
    return created["id"]


def ensure_trigger(tok: str, workspace: str, reflex: str, lake: str, notebook: str):
    """Bind file arrivals under the watched prefix to a run of the notebook."""
    base = f"/workspaces/{workspace}/reflexes/{reflex}/triggers"
    body = {
        "displayName": TRIGGER_NAME,
        "enabled": True,
        "eventType": EVENT_TYPE,
        "source": {"itemId": lake, "pathPrefix": WATCHED_PREFIX},
        "action": {"workspaceId": workspace, "itemId": notebook, "jobType": JOB_TYPE},
    }

    listed = fabric("GET", base, tok)
    assert listed.status_code == 200, (listed.status_code, listed.text[:200])
    for t in listed.json().get("value", []):
        if t.get("displayName") == TRIGGER_NAME:
            r = fabric("PATCH", f"{base}/{t['id']}", tok, json=body)
            assert r.status_code == 200, (r.status_code, r.text[:300])
            log(f"reusing trigger {TRIGGER_NAME}")
            return r.json()

    r = fabric("POST", base, tok, json=body)
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])
    created = r.json()
    log(f"trigger {TRIGGER_NAME}: {WATCHED_PREFIX} -> {JOB_TYPE} on the notebook")
    return created


def triggered_runs(tok: str, workspace: str, item: str) -> list[dict]:
    """Job instances an EVENT started.

    `invokeType` again, and for the same reason it mattered for schedules: a
    triggered run and a manual one are the same job doing the same work, so
    only this field can support the claim that the trigger is what started it.
    """
    r = fabric("GET", f"/workspaces/{workspace}/items/{item}/jobs/instances", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    return [
        j for j in r.json().get("value", []) if j.get("invokeType") == "EventTriggered"
    ]


def main() -> int:
    st = state.load()
    tok = token(FABRIC_AUD)
    ws, lake, notebook = st["workspace"], st["lakehouse"], st["silver_notebook"]

    if not T.event_triggers_have_rest_api:
        # Said out loud, not skipped quietly. A step that returned 0 without
        # explanation would report success for a mechanism nobody wired.
        log(
            "real target: the Reflex binding has no public REST — it is "
            "assembled in the portal, so this step creates nothing and "
            "asserts nothing. Everything downstream of the binding is "
            "ordinary Fabric and is exercised by the job it starts."
        )
        state.save(trigger=None)
        return 0

    reflex = ensure_reflex(tok, ws)
    created = ensure_trigger(tok, ws, reflex, lake, notebook)

    before = len(triggered_runs(tok, ws, notebook))

    # The delivery marker. Written through the same ADLS path the vendor feed
    # uses, because a trigger that only fires for a specially-crafted write
    # would prove nothing about the real one.
    payload = json.dumps(
        {
            "feed": "contoso_pos",
            "parts": st.get("landed", {}).get("parts"),
            "state": "complete",
        }
    ).encode()
    upload(ws, lake, MARKER, payload, token(STORAGE_AUD))
    log(f"dropped the delivery marker at {MARKER}")

    # No polling. The emulator dispatches file events synchronously through its
    # own storage layer, so the trigger has already fired or never will by the
    # time the upload returns — waiting would only slow down a failure.
    after = triggered_runs(tok, ws, notebook)

    assert len(after) > before, (
        f"a file landed under {WATCHED_PREFIX} and no EventTriggered run "
        f"appeared ({before} before, {len(after)} after) — the trigger exists "
        f"but does not fire, which is the failure this step was written to "
        f"catch"
    )
    fired = after[-1]
    assert fired["jobType"] == JOB_TYPE, fired

    state.save(trigger={"id": created["id"], "reflex": reflex, "fired": fired["id"]})
    log(
        f"the trigger FIRED: job {fired['id']} on the silver notebook, "
        f"invokeType=EventTriggered — the platform reacts to a delivery"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
