"""Resolve the capacity the workspace runs on, by NAME.

A Fabric capacity is an **ARM resource**, not a Fabric one. It is created
through `management.azure.com` as `Microsoft.Fabric/capacities`, and only then
does the Fabric control plane list it on `GET /v1/capacities` under a Fabric
GUID of its own. Nothing in the Fabric REST API creates one.

That is why this module exists rather than an `if` in `provision.py`: the
SEQUENCE is identical on both targets, and only the source of the capacity
differs. Both targets resolve a capacity by display name and assign the
workspace to it. The emulator is additionally allowed to create the resource
first, because doing so costs nothing; the real target is not, because a
capacity is billable infrastructure and provisioning one is an operator's
decision rather than a side effect of a pipeline run.

WHAT THIS REPLACED. The platform used to assert `ws.get("capacityId")` behind a
`capacity_is_auto_assigned` flag, because the emulator seeds a capacity and
attaches it to every new workspace while real Fabric does not. That flag was a
difference the platform *tolerated*. Creating the resource properly removes it:
the assertion "this workspace runs on a capacity" is now true, and checked, on
both targets.

THE CAPACITY IS SUPPLIED AT CREATE, NEVER REASSIGNED. An earlier version of
this resolved a capacity first and then moved the workspace onto it whenever
the two disagreed. Against the emulator that is harmless. Against real Fabric
it means a `make verify` with a mismatched `FABRIC_CAPACITY` silently moves a
live workspace between capacities, changing what it bills to and disturbing
whatever is running on it. A pipeline run has no business doing that, so the
capacity is now passed to `POST /v1/workspaces` and an existing workspace's
capacity is adopted as it stands.
"""

from __future__ import annotations

import time

from fabric import MANAGEMENT_AUD, S, T, fabric, log, token

# How long to wait for a created capacity to reach the Fabric control plane.
# In Azure the ARM-to-Fabric sync is internal and not instant; the emulator
# polls ARM on an interval, so the same wait covers both. Observed locally at
# one poll, and a generous ceiling costs nothing on the path that succeeds.
APPEAR_TIMEOUT = 90
APPEAR_INTERVAL = 3

# Azure's GA versions for the two resource types this touches.
RG_API = "2021-04-01"
CAPACITY_API = "2023-11-01"


def _arm(method: str, url: str, tok: str, **kw):
    return S.request(
        method,
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=60,
        **kw,
    )


def create(name: str) -> None:
    """Create the capacity in ARM, if the target allows it. Idempotent.

    PUT is the whole story: ARM answers 201 the first time and 200 on a repeat,
    so a second run of the platform is not an error. Both are accepted for that
    reason, and nothing else is.
    """
    arm = T.capacity_arm
    assert arm is not None, "create() called on a target that may not create one"
    tok = token(MANAGEMENT_AUD)
    base = f"{arm.url}/subscriptions/{arm.subscription}"

    # The resource group first: ARM refuses a capacity whose group does not
    # exist, with a 404 that names the group rather than the capacity.
    r = _arm(
        "PUT",
        f"{base}/resourceGroups/{arm.resource_group}?api-version={RG_API}",
        tok,
        json={"location": arm.location},
    )
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])

    r = _arm(
        "PUT",
        f"{base}/resourceGroups/{arm.resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{name}"
        f"?api-version={CAPACITY_API}",
        tok,
        json={
            "location": arm.location,
            "sku": {"name": arm.sku, "tier": "Fabric"},
            # ARM requires at least one administrator and rejects an empty
            # list. A capacity nobody administers is not a thing Azure makes.
            "properties": {"administration": {"members": [arm.admin]}},
        },
    )
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])
    made = "created" if r.status_code == 201 else "already current"
    log(f"capacity {name} ({arm.sku}) {made}")


def find(tok: str, name: str) -> str | None:
    """The Fabric GUID for the capacity called `name`, or None.

    Resolved by DISPLAY NAME, which is the cross-target address. The GUID is
    Fabric's own and has no relationship to the ARM resource id, so it cannot
    be computed and must be looked up.
    """
    r = fabric("GET", "/capacities", tok)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    for c in r.json().get("value", []):
        if c.get("displayName") == name:
            return c.get("id")
    return None


def for_new_workspace(tok: str) -> str:
    """The capacity id to CREATE a workspace on.

    Called only when there is no workspace yet, or when the one that exists
    carries no capacity. An existing workspace's capacity is adopted, never
    replaced — see provision.py.

    Real Fabric will not guess: `capacityId` is optional on
    `POST /v1/workspaces`, and omitting it leaves a workspace no Lakehouse can
    be created in. So something has to name one, and on the real target that
    is configuration rather than anything this platform can invent.
    """
    name = T.capacity_name

    if T.capacity_arm is None:
        if not name:
            raise SystemExit(
                "creating a workspace on real Fabric needs FABRIC_CAPACITY, the "
                "display name of a capacity that already exists in your tenant. "
                "It is needed ONLY to create a workspace: an existing one "
                "already carries its capacity and this platform adopts it. This "
                "platform never creates a capacity, because that is billable "
                "Azure infrastructure and an operator's decision."
            )
        found = find(tok, name)
        if found is None:
            raise SystemExit(
                f"no capacity named {name!r} is visible to this identity. "
                f"FABRIC_CAPACITY must name a capacity that already exists and "
                f"that the running identity can see."
            )
        log(f"capacity {name} resolved to {found}")
        return found

    create(name)

    # Created, but not yet Fabric's. The control plane learns about it from
    # ARM asynchronously, so the id is not available the instant PUT returns.
    deadline = time.monotonic() + APPEAR_TIMEOUT
    while True:
        found = find(tok, name)
        if found is not None:
            log(f"capacity {name} resolved to {found}")
            return found
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"capacity {name} was created in ARM but never appeared on "
                f"GET /v1/capacities within {APPEAR_TIMEOUT}s. The Fabric "
                f"service reads capacities from ARM: check that it is pointed "
                f"at the same ARM this platform wrote to."
            )
        time.sleep(APPEAR_INTERVAL)
