"""Resolve the workspace and lakehouse by NAME, creating them if absent.

BY NAME, NOT BY ID, and that is the documented contract rather than a
convenience: ids can never match across targets, so user code holds display
names and the platform resolves them to a GUID per target
(docs/21-real-fabric-toggle).

Resolve-or-create also makes the run idempotent. Display names are unique per
tenant in real Fabric, so a second `POST /workspaces` returns 409
`WorkspaceNameAlreadyExists` — on both targets. A platform that can only be run
against a fresh tenant is not one anybody can operate.
"""

from __future__ import annotations

import capacity
import environment
import state
from fabric import FABRIC_AUD, fabric, log, token

# From the target, not restated here: real mode is workspace-scoped and the
# resolver needs the name before this module is even imported. One string, one
# place — a second copy would be a second answer the day it drifts.
from target import WORKSPACE

LAKEHOUSE = "contoso_lake"


def find_workspace(tok: str, name: str) -> dict | None:
    r = fabric("GET", "/workspaces", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    for ws in r.json().get("value", []):
        if ws.get("displayName") == name:
            return ws
    return None


def find_item(tok: str, workspace: str, name: str, kind: str) -> dict | None:
    r = fabric("GET", f"/workspaces/{workspace}/items", tok)
    # A stale state.json is the common cause, not a broken workspace: the
    # emulator keeps state in memory, so any restart invalidates the ids a
    # previous run recorded. Say that, rather than leaving `WorkspaceNotFound`
    # to be interpreted.
    assert r.status_code != 404, (
        f"workspace {workspace} is gone — state.json is from an earlier stack. "
        f"Run `make verify` from the start; provision resolves by name."
    )
    assert r.status_code == 200, (r.status_code, r.text[:200])
    for it in r.json().get("value", []):
        if it.get("displayName") == name and it.get("type") == kind:
            return it
    return None


def main() -> int:
    tok = token(FABRIC_AUD)

    ws = find_workspace(tok, WORKSPACE)
    if ws is None:
        # A workspace is created ON a capacity. `capacityId` is optional in the
        # contract, and omitting it is the trap: the emulator auto-assigns its
        # seeded capacity, real Fabric leaves the workspace with none and the
        # Lakehouse below then fails. So it is always supplied.
        cap = capacity.for_new_workspace(tok)
        r = fabric(
            "POST",
            "/workspaces",
            tok,
            json={"displayName": WORKSPACE, "capacityId": cap},
        )
        # Create is one of the synchronous paths (quickstart §3). A 202 would
        # mean the contract changed, which is worth failing on rather than
        # silently polling.
        assert r.status_code == 201, (r.status_code, r.text[:300])
        ws = r.json()
        log(f"created workspace {WORKSPACE} on capacity {cap}")
    else:
        # ADOPTED, NOT REPLACED. An existing workspace already carries the
        # capacity someone put it on, and on real Fabric that someone is an
        # operator whose decision this run must not overrule. Moving it would
        # change what it bills to and disturb whatever is running on it.
        log(f"reusing workspace {WORKSPACE}")
        if not ws.get("capacityId"):
            # Nothing to adopt. A workspace can exist with no capacity, and
            # then no Lakehouse can be created in it. Assigning one here would
            # be harmless, but the rule that this step never changes a
            # workspace's capacity is worth more than the convenience: a rule
            # with an exception is one somebody extends to the case that hurts.
            raise SystemExit(
                f"workspace {WORKSPACE} exists but is on no capacity. Assign it "
                f"to one and re-run. This step never changes which capacity a "
                f"workspace is on, because doing that to a live workspace moves "
                f"its billing and disturbs what is running on it."
            )
    assert ws["id"], ws

    # True on BOTH targets, and the reason the old capacity_is_auto_assigned
    # flag is gone: a workspace without a capacity is not usable on either.
    r = fabric("GET", f"/workspaces/{ws['id']}", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    on = r.json().get("capacityId")
    assert on, f"workspace {WORKSPACE} is on no capacity: {r.json()}"
    log(f"workspace {WORKSPACE} runs on capacity {on}")

    lake = find_item(tok, ws["id"], LAKEHOUSE, "Lakehouse")
    if lake is None:
        r = fabric(
            "POST",
            f"/workspaces/{ws['id']}/items",
            tok,
            json={"displayName": LAKEHOUSE, "type": "Lakehouse"},
        )
        assert r.status_code in (201, 202), (r.status_code, r.text[:300])
        lake = r.json()
        log(f"created lakehouse {LAKEHOUSE}")
    else:
        log(f"reusing lakehouse {LAKEHOUSE}")
    assert lake["id"], lake

    # The data product, delivered to the Spark engine. bronze and silver run
    # as notebooks on the pool, so installing the package in this process is
    # not enough: the Environment is what carries it across.
    env = environment.ensure(tok, ws["id"])

    state.save(
        environment=env,
        workspace=ws["id"],
        lakehouse=lake["id"],
        workspace_name=WORKSPACE,
        lakehouse_name=LAKEHOUSE,
    )
    log(f"workspace {ws['id']}, lakehouse {lake['id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
