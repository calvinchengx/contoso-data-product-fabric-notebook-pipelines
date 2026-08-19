"""The Environment item that puts the data product on the Spark engine.

WHY THIS EXISTS. `bronze` and `silver` run as Fabric Notebooks, which execute on
the Spark pool, not in this process. Installing `contoso-data-product` here puts
it in the *client's* environment, where the notebook cannot see it: the engine is
another machine with another interpreter. So `from contoso_product import
run_silver` inside a notebook needs the package delivered to the engine, and in
Fabric the thing that does that is an **Environment** item carrying a
`Libraries/requirements.txt`.

This is the real mechanism, not an emulator affordance. A notebook names its
Environment in its own `# META` dependencies block, Fabric resolves it when the
session starts, and the emulator implements the same contract.

THE VERSION IS NOT WRITTEN HERE. It is read from the package this platform
itself installed, so the engine and the client cannot end up on different
releases of the product. Pinning the URL separately would be a second source of
truth, and the one thing worse than a stale copy of the transforms is two
runtimes quietly running different ones.
"""

from __future__ import annotations

import base64
import time
from importlib.metadata import version

from fabric import fabric, log

ENVIRONMENT = "contoso-product-env"


def wheel_url() -> str:
    """The release wheel for the product version this platform has installed."""
    v = version("contoso-data-product")
    return (
        "https://github.com/calvinchengx/contoso-data-product/releases/download/"
        f"v{v}/contoso_data_product-{v}-py3-none-any.whl"
    )


def ensure(tok: str, workspace: str) -> str:
    """Resolve-or-create the Environment, and return its id.

    Resolve-or-create by NAME like every other item here: ids cannot match
    across targets, and a step that only works on a fresh workspace is not one
    anybody can operate.
    """
    # Imported HERE, not at module scope. provision imports this module, so a
    # top-level `from provision import find_item` is a cycle: it resolved when
    # provision ran as __main__ and broke the moment another step imported
    # both, which is exactly how it surfaced — at step 9, not step 1.
    from provision import find_item

    url = wheel_url()
    payload = base64.b64encode(f"{url}\n".encode()).decode()
    definition = {
        "parts": [
            {
                "path": "Libraries/requirements.txt",
                "payloadType": "InlineBase64",
                "payload": payload,
            }
        ]
    }

    found = find_item(tok, workspace, ENVIRONMENT, "Environment")
    if found:
        r = fabric(
            "POST",
            f"/workspaces/{workspace}/items/{found['id']}/updateDefinition",
            tok,
            json={"definition": definition},
        )
        assert r.status_code in (200, 202), (r.status_code, r.text[:300])
        log(f"reusing environment {ENVIRONMENT} at {version('contoso-data-product')}")
        return found["id"]

    r = fabric(
        "POST",
        f"/workspaces/{workspace}/items",
        tok,
        json={
            "displayName": ENVIRONMENT,
            "type": "Environment",
            "definition": definition,
        },
    )
    assert r.status_code in (201, 202), (r.status_code, r.text[:300])

    # The create is an LRO, so the id arrives by resolving the name rather than
    # from the response body. Polling the item list needs no header plumbing and
    # works whichever way the service answers.
    for _ in range(60):
        found = find_item(tok, workspace, ENVIRONMENT, "Environment")
        if found:
            log(f"created environment {ENVIRONMENT} carrying {url}")
            return found["id"]
        time.sleep(0.5)
    raise SystemExit(f"the Environment item {ENVIRONMENT} never appeared")
