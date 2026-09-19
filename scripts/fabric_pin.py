#!/usr/bin/env python3
"""The fabric-emulator release this leaf pins, and the bump to the newest one.

Stdlib only, so the bump workflow needs nothing installed to decide whether
there is anything to do.

WHY A SCRIPT RATHER THAN sed IN THE WORKFLOW. A regex typed into YAML is
invisible to review and untestable, and this one has to be exactly right: the
pin it rewrites is what the platform's acceptance run compares against the
release it was asked to verify. This is importable, and its self-test asserts
the ways it can be wrong.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
REPO = "calvinchengx/fabric-emulator"

# The four wheels a fabric-emulator release publishes and this leaf installs.
# Named, not discovered: a release that stops publishing one of them must fail
# here rather than open a pull request that silently drops a dependency.
WHEELS = (
    "fabric_target",
    "contoso_fixtures",
    "contoso_fixtures_advanced",
    "fabric_emulator_notebookutils",
)

PIN = re.compile(
    r"(?P<head>fabric-emulator/releases/download/)v(?P<tag>[0-9][^/]*)/"
    r"(?P<wheel>[a-z_]+)-(?P<ver>[0-9][^-]*)-"
)


def pinned(text: str) -> set[str]:
    """Every fabric release this file pins. More than one is a bug, not a state."""
    return {m.group("tag") for m in PIN.finditer(text)}


def latest() -> str:
    out = subprocess.run(
        ["gh", "api", f"repos/{REPO}/releases/latest", "-q", ".tag_name"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise SystemExit(f"cannot read the latest release: {out.stderr.strip()[:200]}")
    return out.stdout.strip().lstrip("v")


def assets(tag: str) -> set[str]:
    out = subprocess.run(
        ["gh", "api", f"repos/{REPO}/releases/tags/v{tag}", "-q", "[.assets[].name]"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise SystemExit(f"cannot read v{tag}'s assets: {out.stderr.strip()[:200]}")
    return set(json.loads(out.stdout))


def rewrite(text: str, version: str) -> str:
    return PIN.sub(
        lambda m: f"{m.group('head')}v{version}/{m.group('wheel')}-{version}-", text
    )


def plan(text: str, version: str, names: set[str]) -> list[str]:
    """What the workflow should do, as GITHUB_OUTPUT lines."""
    here = pinned(text)
    if len(here) != 1:
        raise SystemExit(
            f"this file pins {sorted(here) or 'no'} fabric releases; "
            f"expected exactly one"
        )
    if here == {version}:
        return ["bump=no", f"version={version}"]
    # EVERY wheel, checked before the pull request rather than after `uv lock`
    # 404s inside it.
    missing = [
        w for w in WHEELS if not any(n.startswith(f"{w}-{version}-") for n in names)
    ]
    if missing:
        raise SystemExit(
            f"v{version} does not publish {', '.join(missing)}; "
            f"not bumping to a release this leaf cannot install"
        )
    return ["bump=yes", f"version={version}"]


def self_test():
    """The ways this can be wrong, each asserted."""
    sample = (
        'a = { url = ".../fabric-emulator/releases/download/v0.38.0/'
        'fabric_target-0.38.0-py3-none-any.whl" }\n'
        'b = { url = ".../fabric-emulator/releases/download/v0.38.0/'
        'contoso_fixtures-0.38.0-py3-none-any.whl" }\n'
    )
    full = {f"{w}-0.39.0-py3-none-any.whl" for w in WHEELS}
    problems = []
    if pinned(sample) != {"0.38.0"}:
        problems.append(f"pinned() read {pinned(sample)}")
    moved = rewrite(sample, "0.39.0")
    if pinned(moved) != {"0.39.0"} or "0.38.0" in moved:
        problems.append("rewrite left the old release behind")
    if plan(sample, "0.38.0", full) != ["bump=no", "version=0.38.0"]:
        problems.append("a release already pinned still asked for a bump")
    if plan(sample, "0.39.0", full) != ["bump=yes", "version=0.39.0"]:
        problems.append("a new release did not ask for a bump")
    try:
        plan(sample, "0.39.0", full - {"fabric_target-0.39.0-py3-none-any.whl"})
        problems.append("a release missing a wheel was accepted")
    except SystemExit:
        pass
    try:
        plan(sample.replace("v0.38.0/contoso", "v0.37.0/contoso"), "0.39.0", full)
        problems.append("two different pins read as one state")
    except SystemExit:
        pass
    # The real file, so the regex cannot drift away from what it must match.
    if len(pinned(PYPROJECT.read_text(encoding="utf-8"))) != 1:
        problems.append("pyproject.toml does not read as exactly one pinned release")
    for line in problems:
        print(f"self-test: {line}", file=sys.stderr)
    if problems:
        return 1
    print("self-test: pins read, rewritten, and a release missing a wheel refused")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--plan",
        action="store_true",
        help="print GITHUB_OUTPUT lines for the newest release",
    )
    ap.add_argument("--write", metavar="VERSION", help="rewrite the pins to VERSION")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args(argv)
    if a.self_test:
        return self_test()
    text = PYPROJECT.read_text(encoding="utf-8")
    if a.write:
        want = a.write.lstrip("v")
        PYPROJECT.write_text(rewrite(text, want), encoding="utf-8")
        left = pinned(PYPROJECT.read_text(encoding="utf-8"))
        if left != {want}:
            raise SystemExit(f"after the rewrite this file pins {sorted(left)}")
        print(f"pins now v{want}")
        return 0
    if a.plan:
        version = latest()
        for line in plan(text, version, assets(version)):
            print(line)
        return 0
    print(", ".join(sorted(pinned(text))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
