"""A Fabric client. Not an emulator client that happens to reach Fabric.

Every call below is the real Fabric contract — an Entra bearer token, the `/v1`
control plane, OneLake's ADLS Gen2 create/append/flush. What differs between
the local family and production is resolved in target.py and appears here only
as configuration: an endpoint, a credential, a TLS flag, one Host header.
Nothing in this module is shaped around the emulator — including how it
authenticates, which is a credential object rather than a grant type this file
picked.

DELIBERATELY NOT `common.py`. That module ships inside the contoso-fixtures
wheel and would give this repository the emulator's own client plumbing — which
would quietly void the single claim this repository exists to make: that a
consumer can build against a PUBLISHED image without the source. A test in
tests/test_repo.py enforces the absence.
"""

from __future__ import annotations

import pathlib
import ssl
import time
import urllib.parse

import apipath
import requests
import target
import urllib3

# Re-exported: `log` now lives in a module with no dependencies, so callers that
# only want to print a line need not resolve a target. See say.py.
from say import log as log

T = target.resolve()

# Real Fabric audiences, on both targets — the emulator validates the same ones.
FABRIC_AUD = "https://api.fabric.microsoft.com"
STORAGE_AUD = "https://storage.azure.com"
# The management plane, where a capacity is an ARM resource. A first-party
# Entra audience like the two above, so it needs no registration on either
# target.
MANAGEMENT_AUD = "https://management.azure.com"

FABRIC = T.api_root

ROOT = pathlib.Path(__file__).resolve().parent.parent
STATE = ROOT / "state.json"

S = requests.Session()
# TLS verification follows the TARGET, never a constant. Against real Fabric it
# is on and cannot be turned off from configuration; the local family serves
# self-signed certificates a consumer has no CA for.
S.verify = T.verify_tls
if not T.verify_tls:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def token(audience: str) -> str:
    """A bearer token for `audience`, from the target's credential.

    THE GRANT TYPE IS NOT DECIDED HERE, and that is the point. This used to POST
    `grant_type=client_credentials` with a client id and secret, which quietly
    made a service principal the only identity the platform could ever use:
    `az login` could not drive it, and inside a Fabric notebook — where the
    session runs as the invoking user or a managed identity and there is no
    secret to hand over — it could not run at all. The target resolves a
    credential instead, so who is authenticating is the deployment's business
    and not this module's.

    The audiences are the real Fabric ones on both targets; the emulator
    validates the same strings.
    """
    tok = T.token(audience)
    assert tok, f"the {T.name} credential returned an empty access token"
    return tok


def ensure_audience(audience: str, name: str) -> None:
    """Make sure a token for `audience` can be minted.

    A no-op against real Fabric: Azure SQL and Power BI are first-party Entra
    resources and every tenant can already request them. The local family's
    entra mints only for audiences it knows, so a non-default one is registered
    first — a setup difference, resolved by the target like every other.
    """
    if not T.entra_admin_api:
        return
    r = S.post(
        T.entra_admin_api,
        json={"displayName": name, "appIdUri": audience, "isConfidential": False},
        timeout=30,
    )
    # 409 means it is already there, which is the normal case on a re-run.
    assert r.status_code in (200, 201, 409), (audience, r.status_code, r.text[:200])


def fabric(method: str, path: str, tok: str, **kw):
    """One Fabric REST call. `path` is relative to `/v1`, which this adds.

    THE ARGUMENT IS CHECKED, because getting it wrong is silent. A caller that
    passes `/v1/workspaces/...` gets `/v1/v1/workspaces/...`, and the emulator
    answers 404 `UnknownEndpoint` — a perfectly successful HTTP response that
    `requests` does not raise on. `govern.py` did exactly this, and every
    provenance lookup came back empty for as long as the code existed. The
    platform reported "0 lineage edges the emulator observed", which is a
    plausible number for a platform that only declares its lineage, so nothing
    about the output looked wrong. The real answer was four.
    """
    r = S.request(
        method,
        f"{FABRIC}/v1{apipath.check(path)}",
        headers={"Authorization": f"Bearer {tok}", **kw.pop("headers", {})},
        timeout=120,
        **kw,
    )
    return r


def await_operation(resp, tok: str, what: str) -> dict:
    """Poll a long-running operation to its result.

    Fabric splits item writes by whether a DEFINITION is involved: creating an
    empty item is synchronous (201), while creating or updating one that
    carries source — a notebook, a report — is a 202 with an operation to poll.
    Both are the real contract, so a caller that only handles 201 works right up
    until it publishes something with a body.

    Returns the operation result, or `{}` for an operation that succeeds
    without one (updateDefinition).
    """
    if resp.status_code == 201:
        return resp.json()
    assert resp.status_code == 202, (what, resp.status_code, resp.text[:300])
    op = resp.headers.get("x-ms-operation-id")
    assert op, f"{what}: 202 with no x-ms-operation-id: {dict(resp.headers)}"

    for _ in range(120):
        r = fabric("GET", f"/operations/{op}", tok)
        assert r.status_code == 200, (what, r.status_code, r.text[:200])
        status = r.json().get("status")
        if status == "Succeeded":
            got = fabric("GET", f"/operations/{op}/result", tok)
            # An operation with nothing to return answers 400
            # OperationNotComplete-style rather than a body; that is a success
            # with no result, not a failure.
            return got.json() if got.status_code == 200 else {}
        assert status != "Failed", (what, r.text[:300])
        time.sleep(1)
    raise SystemExit(f"{what}: operation {op} never completed")


def onelake(method: str, path: str, tok: str, **kw):
    """ADLS Gen2 against OneLake.

    Real Fabric has its own hostname and is addressed directly. The emulator
    serves OneLake on the Fabric port and routes by Host header — the same
    thing `curl --resolve` does — so that override is applied only when the
    target asks for it.
    """
    headers = {"Authorization": f"Bearer {tok}", **kw.pop("headers", {})}
    if T.onelake_host_header:
        headers["Host"] = T.onelake_host_header
    return S.request(
        method,
        f"{T.onelake_url}/{path.lstrip('/')}",
        headers=headers,
        timeout=300,
        **kw,
    )


def upload(workspace: str, item: str, rel_path: str, blob: bytes, tok: str) -> int:
    """create → append → flush, the three-step ADLS Gen2 write.

    Chunked because that is how a real ADLS client writes a 170 MB export, and
    because a single append would say nothing about whether `position` is
    handled correctly across calls.
    """
    quoted = urllib.parse.quote(rel_path)
    base = f"/{workspace}/{item}/{quoted}"

    r = onelake("PUT", f"{base}?resource=file", tok)
    assert r.status_code in (201, 202), (r.status_code, r.text[:200])

    chunk = 8 * 1024 * 1024
    pos = 0
    while pos < len(blob):
        part = blob[pos : pos + chunk]
        r = onelake("PATCH", f"{base}?action=append&position={pos}", tok, data=part)
        assert r.status_code in (200, 202), (pos, r.status_code, r.text[:200])
        pos += len(part)

    r = onelake("PATCH", f"{base}?action=flush&position={pos}", tok)
    assert r.status_code in (200, 202), (r.status_code, r.text[:200])
    return pos


def server_cert_pem(url: str) -> str:
    """The certificate the stack is actually presenting.

    Tools that own their own HTTP client (dbt, ODBC) verify the chain and have
    no CA for a self-signed cert; handing them exactly what is being served
    keeps TLS exercised rather than disabled globally.
    """
    host, _, port = url.split("://", 1)[1].partition(":")
    return ssl.get_server_certificate((host, int(port or 443)))
