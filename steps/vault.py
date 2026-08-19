"""Secrets come from Key Vault. Never from the source tree.

Named `vault`, not `secrets`: the latter is a Python standard-library module,
and shadowing it on sys.path is a collision waiting for whichever dependency
imports the real one.

The azure-keyvault-emulator locally, the customer's real Azure Key Vault in
production — the same REST contract and the same vault-audience token on both,
so this module has no target-specific code at all beyond the URL.

WHY THIS MATTERS MORE THAN IT LOOKS. A vendor API key or a database password in
a repository is a credential that has already leaked: it is in every clone, in
the reflog, and in whatever CI cached the checkout. It also skips the part of
the pipeline a real deployment has to get right — an identity permitted to read
a vault, and a rotation that does not require a code change.

THE ONE EXCEPTION, and it is structural: the Entra credential used to obtain a
token cannot itself live in the vault, because reading the vault requires it.
That bootstrap identity is a service principal from the environment (or a
managed identity in production) and lives in target.py.
"""

from __future__ import annotations

import requests
from fabric import S, T, token

VAULT_AUD = "https://vault.azure.net"
API_VERSION = "7.4"


def _url(name: str) -> str:
    return f"{T.vault_url}/secrets/{name}?api-version={API_VERSION}"


def put(name: str, value: str) -> str:
    """Store a secret. Used to SEED the vault for a self-contained run.

    In a real deployment the secrets are already there, placed by whoever owns
    them — which is the point: the platform reads, it does not author.
    """
    r = S.put(
        _url(name),
        json={"value": value},
        timeout=30,
        headers={"Authorization": f"Bearer {token(VAULT_AUD)}"},
    )
    assert r.status_code in (200, 201), (name, r.status_code, r.text[:200])
    return r.json()["id"]


def get(name: str) -> str:
    r: requests.Response = S.get(
        _url(name),
        timeout=30,
        headers={"Authorization": f"Bearer {token(VAULT_AUD)}"},
    )
    assert r.status_code == 200, (
        f"secret {name!r} not readable from {T.vault_url}: "
        f"{r.status_code} {r.text[:200]}"
    )
    value = r.json()["value"]
    assert value, f"secret {name!r} is empty"
    return value
