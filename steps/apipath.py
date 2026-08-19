"""The one rule about a Fabric REST path, kept where a test can reach it.

`fabric()` adds the `/v1` prefix, so a caller passes what comes after it. Get
that wrong and the request goes to `/v1/v1/…`, which the emulator answers 404
`UnknownEndpoint` — a perfectly successful HTTP response that `requests` does
not raise on. `govern.py` did exactly this, and every provenance lookup came
back empty for as long as the code existed.

WHY THIS IS ITS OWN MODULE, and it is not tidiness. Importing `fabric` resolves
a target, and resolving a target needs the published `fabric-target` wheel —
installed by `make fixtures` from the pinned release, deliberately outside
`uv.lock`. `tests/test_repo.py` promises in its first paragraph that none of its
tests need the emulator, Docker or the fixture wheels, because they are the part
of CI that is green from day one and runs identically on all three platforms.

A guard that lives in `fabric.py` cannot be tested from there without breaking
that promise. It broke it: the test was added, `make test` imported `fabric`,
and CI went red on ubuntu, macOS and Windows at once with
`ModuleNotFoundError: No module named 'fabric_target'`.

The rule has no dependencies — it is a string check on an argument. So it does
not have to live behind any, and this module is the place that has none.
"""

from __future__ import annotations


def check(path: str) -> str:
    """Raise unless `path` is a valid /v1-relative Fabric path. Returns it.

    Returns the path so a caller can write `check(path)` inline at the point of
    use rather than validating and then separately trusting the same variable.
    """
    if not path.startswith("/"):
        raise ValueError(f"fabric() path must start with '/': {path!r}")
    if path == "/v1" or path.startswith("/v1/"):
        raise ValueError(
            f"fabric() adds the /v1 prefix; pass {path[3:] or '/'!r} rather than "
            f"{path!r}, or the request goes to /v1/v1/... and 404s as "
            f"UnknownEndpoint without raising"
        )
    return path
