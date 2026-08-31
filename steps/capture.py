"""Record the platform: the flow view while it runs, the catalog after.

TWO MODES, BECAUSE THE TWO SURFACES DIFFER IN WHEN THEY MEAN ANYTHING.

    Data flow     during   nodes light up as writes land, over SSE
    OpenMetadata  after    the catalog is a result

So `flow_watch` starts BEFORE the pipeline and records through it, and
`om_verify` runs once there is something to photograph. That ordering is the
reason capture has two entry points rather than one post-run script.

ASSERT, THEN CAPTURE. A screenshot suite that only captures will happily
produce a beautiful picture of an empty catalog: green run, worthless artifact.
Every shot is preceded by a check on the page's own text, and both scripts exit
non-zero if their checks fail.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess

from fabric import T, log

# THE IMAGE IS BUILT FROM THE PLATFORM'S Dockerfile. `docker/` belongs to the
# platform -- it is how that repository packages the tools this step drives --
# so the build has to run there. Building from the product's directory looked
# right while the two were one repository and fails with a bare
# `returned non-zero exit status 1` now that they are not.
PLATFORM_DIR = pathlib.Path(
    os.environ.get("PLATFORM", pathlib.Path(__file__).resolve().parent.parent)
)


# `OM_URL` NAMES TWO DIFFERENT THINGS ON THE TWO SIDES OF THIS CALL, WHICH IS
# HOW A FIXED PORT OUTLIVED THE MAKEFILE THAT STOPPED HARDCODING ONE. On the
# Python side it is the API base the Makefile exports, ending `/api/v1`; the
# browser scripts want the ORIGIN, because they navigate the UI rather than
# call it. This step used to bridge that by passing a literal
# `http://localhost:8585`, which overrode the export and pointed Playwright at
# the port the catalog no longer publishes: compose moved it to 18587 to stop
# colliding with the two sibling platforms.
#
# Derived, so the port lives in exactly one place: compose/governance.yml.
OM_API = os.environ.get("OM_URL", "http://localhost:18587/api/v1").rstrip("/")
OM_ORIGIN = re.sub(r"/api/v\d+$", "", OM_API)


ROOT = pathlib.Path(__file__).resolve().parent.parent
# THE CAPTURE SCRIPTS AND THEIR OUTPUT ARE THE PLATFORM'S. `capture/` holds
# om_verify.js and the recorder that photographs the catalog -- tooling about how
# a run is demonstrated, not about the data product. Mounting the PRODUCT's
# `capture/` handed the container an empty directory and it exited 1 with
# "the catalog did not verify", which reads like a broken catalog rather than a
# missing mount.
SHOTS = PLATFORM_DIR / "capture" / "shots"
STOP = SHOTS / ".stop"
IMAGE = "contoso-capture"


def build() -> None:
    subprocess.run(
        ["docker", "build", "-q", "-f", "docker/capture/Dockerfile", "-t", IMAGE, "."],
        cwd=PLATFORM_DIR,
        check=True,
        capture_output=True,
    )


def run(script: str, detach: bool = False, **env: str):
    passthrough = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
    # NOT --rm when detached: the container is removed the instant it exits,
    # and `docker logs` afterwards finds nothing — so the recorder's own report
    # (did the graph render? was a video written?) would vanish exactly when it
    # matters. Removed explicitly in stop_watch instead.
    cmd = [
        "docker",
        "run",
        *([] if detach else ["--rm"]),
        "--network",
        "host",
        *(["-d"] if detach else []),
        "-v",
        f"{PLATFORM_DIR / 'capture'}:/capture",
        *passthrough,
        IMAGE,
        f"/capture/{script}",
    ]
    if detach:
        out = subprocess.run(
            cmd, cwd=PLATFORM_DIR, check=True, capture_output=True, text=True
        )
        return out.stdout.strip()
    return subprocess.run(cmd, cwd=PLATFORM_DIR)


def start_watch() -> str:
    """Begin recording the flow view. Returns the container id."""
    build()
    SHOTS.mkdir(parents=True, exist_ok=True)
    STOP.unlink(missing_ok=True)
    cid = run("flow_watch.js", detach=True, PORTAL_URL=T.api_root)
    log(f"recording the data flow view (container {cid[:12]})")
    return cid


def stop_watch(cid: str) -> None:
    """Signal the recorder and wait for it to flush the video.

    A file, not a `docker kill`: killing the container discards the video,
    because Playwright writes it when the context closes.
    """
    STOP.write_text("stop", encoding="utf-8")
    subprocess.run(["docker", "wait", cid], capture_output=True, timeout=180)
    out = subprocess.run(["docker", "logs", cid], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith(("RENDERED", "VIDEO", "WATCHED")):
            log(f"  {line}")
    subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


def main() -> int:
    """Verify and photograph the catalog. The flow video is recorded by
    `make verify`, which is the only place that knows when the run starts."""
    build()
    r = run("om_verify.js", OM_URL=OM_ORIGIN)
    assert r.returncode == 0, (
        f"the catalog did not verify (exit {r.returncode}) — the screenshots "
        f"would be pictures of something that is not there"
    )
    shots = sorted(p.name for p in SHOTS.glob("*.png"))
    log(f"captured {len(shots)} screenshot(s): {', '.join(shots)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
