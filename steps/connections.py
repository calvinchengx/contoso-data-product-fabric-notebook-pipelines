"""The source systems, as first-class objects — and as lineage nodes.

A medallion does not begin in Fabric. It begins at a vendor's REST API, a
production database, a change stream. But every lineage edge used to require a
(workspace, item, path) triple at both ends, so the first hop could only ever
be drawn from a file already sitting in `Files/landing/` — and the system that
PUT it there could not be said at all. `ingest_pos.py` has claimed since it was
written that the vendor's spec "is what makes Contoso POS a node in the lineage
graph rather than a filename in Files/landing"; until this module existed that
was an aspiration, and the graph started at the landed file.

WHY A CONNECTION AND NOT A URI. The connection already exists in Fabric's model:
it holds the vendor's credential, it carries a display name and a connectivity
type, and it is what the ingesting client actually authenticated through.
Naming it keeps the rule an edge lives by — record what happened, never a guess
about it. A free-form URI would be a string this platform made up.

CREDENTIALS STILL COME FROM KEY VAULT. The connection is created with the
secret read from the vault at call time, exactly as the ingest steps read it to
authenticate. Nothing here puts a credential in the source tree, and the value
is never read back out of Fabric — the read shape is metadata only, as in real
Fabric.

ONE ASYMMETRY. Creating a connection is real Fabric's own API. REPORTING
lineage is not: `POST …/lineage` is an emulator-native extension, because real
Fabric derives lineage from the artifacts it manages rather than accepting a
claim. So the connection is created on both targets and the report is gated —
see `T.lineage_can_be_reported`.
"""

from __future__ import annotations

import json

from fabric import T, fabric, log


def ensure(tok: str, display_name: str, connectivity: str, details: dict) -> str:
    """Resolve-or-create a connection by DISPLAY NAME.

    By name for the same reason every other object here is: ids cannot match
    across targets, and a step that only works against a fresh tenant is not
    one anybody can operate.
    """
    listed = fabric("GET", "/connections", tok)
    assert listed.status_code == 200, (listed.status_code, listed.text[:200])
    for c in listed.json().get("value", []):
        if c.get("displayName") == display_name:
            return c["id"]

    r = fabric(
        "POST",
        "/connections",
        tok,
        json={
            "displayName": display_name,
            "connectivityType": connectivity,
            "connectionDetails": details,
            # REQUIRED, and not by the emulator. Posting this body to a real
            # tenant without it answers `The CredentialDetails field is
            # required.` alongside the Type and CreationMethod complaints —
            # measured 2026-08-11. The emulator treats credentialDetails as
            # optional (a git-provider connection legitimately has none), so
            # omitting it round-tripped here and could never have worked there.
            #
            # Anonymous because these are fixture HTTP endpoints and `Web`
            # accepts it. A vault-backed credential would be
            # `credentialType: "Key"` with `keyReference: {connectionId,
            # secretName}` — but connectionId must name an AzureKeyVault
            # connection, and that connector takes only OAuth2 or
            # ServicePrincipal, so it cannot be created non-interactively.
            # Conformant is not the same as runnable-on-real; this is the
            # former.
            "credentialDetails": {"credentials": {"credentialType": "Anonymous"}},
        },
    )
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])
    created = r.json()
    log(f"connection {display_name} ({connectivity})")
    return created["id"]


def report(tok: str, workspace: str, step: str, moves: list[dict]) -> bool:
    """Report what a step moved. Returns whether anything was recorded.

    `moves` is the PRECISE form — a list of `{reads, writes}` derivations —
    rather than one flat read set and one flat write set. The flat form pairs
    every read with every write, which overstates the moment a step has more
    than one of either: a step reading two feeds and writing two paths would
    claim four movements where two happened. That exact mistake produced a
    graph with three phantom edges in this repository once already.
    """
    if not T.lineage_can_be_reported:
        return False
    r = fabric(
        "POST",
        f"/workspaces/{workspace}/lineage",
        tok,
        json={"step": step, "moves": moves},
    )
    assert r.status_code in (200, 201), (r.status_code, r.text[:300])
    return True


def from_source(connection: str, item: str, paths: list[str]) -> list[dict]:
    """Movements from ONE source system into the paths it landed.

    One move per path, never one move listing them all: the customers feed did
    not produce the orders file. The read side carries only `connectionId` —
    a source system has no workspace and no path inside it, and the emulator
    rejects a ref that tries to be both.
    """
    return [
        {
            "reads": [{"connectionId": connection}],
            "writes": [{"itemId": item, "path": p}],
        }
        for p in paths
    ]


def announce(tok: str, workspace: str, step: str, name: str, moves: list[dict]) -> None:
    """Report, and say what was recorded — or why nothing was.

    A step that silently skipped its lineage would leave a graph that begins at
    a landed file and looks perfectly correct, which is the failure this whole
    module exists to remove.
    """
    if report(tok, workspace, step, moves):
        landed = [w["path"] for m in moves for w in m["writes"]]
        log(f"lineage: {name} -> {', '.join(landed)}")
    else:
        log(
            f"lineage: not reported — {T.name} derives lineage from the "
            f"artifacts it manages and accepts no claim ({name})"
        )


def details(conn_type: str, creation_method: str, **parameters) -> dict:
    """`connectionDetails` in the shape Fabric's Create Connection API defines.

    THIS USED TO BE AN INVENTED SHAPE — `{kind, endpoint, secretName}` — and the
    emulator stored it verbatim, so it round-tripped and nothing complained. No
    tenant would have accepted it: the request shape is
    `{type, creationMethod, parameters[]}`, and `path` (the field a reader
    reaches for) belongs to the RESPONSE. fabric-emulator 0.22.0 began enforcing
    that and answered `InvalidConnectionDetails: The Type field is required.`

    The type and creation-method names are not guesses. They come from a real
    tenant's `GET /v1/connections/supportedConnectionTypes` — `Web` takes `url`,
    `PostgreSql` takes `server` and `database`.

    `secretName` is gone rather than relocated: which vault secret backs a
    connection is a CREDENTIAL fact, and Fabric carries it as a
    `KeyVaultSecretReference` inside `credentialDetails`, not as connection
    metadata. Recording it here described nothing the service would honour.
    """
    return json.loads(
        json.dumps(
            {
                "type": conn_type,
                "creationMethod": creation_method,
                "parameters": [
                    {"dataType": "Text", "name": name, "value": value}
                    for name, value in parameters.items()
                ],
            }
        )
    )
