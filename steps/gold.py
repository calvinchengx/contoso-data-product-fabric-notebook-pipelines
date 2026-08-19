"""Silver → gold: the star, built in the WAREHOUSE by dbt-fabric over TDS.

Gold is a Warehouse because that is what Fabric gives you: a T-SQL MPP engine
reached over TDS on 1433, authenticated with Entra. Spark cannot write one —
Spark writes Delta to a Lakehouse and the Warehouse reads across the boundary
by three-part name, which is exactly what gold/models/sources.yml does.

WHY dbt RUNS IN A CONTAINER. `dbt-fabric` requires Microsoft's ODBC Driver 18;
Microsoft's own documentation says there is no driver-free option. A native
driver install is the one thing `git clone && make` cannot do on three
platforms, so the driver lives in an image. That is also how dbt usually runs
in production — Fabric does not host dbt, so it executes on a laptop, a CI
agent, or a container, and a container is what any repeatable pipeline picks.

This step itself needs no driver: it creates the Warehouse item over REST and
hands dbt a token.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess

import state
from contoso_product import gold_dir
from fabric import FABRIC_AUD, ensure_audience, fabric, log, token
from provision import find_item

ROOT = pathlib.Path(__file__).resolve().parent.parent
WAREHOUSE = "contoso_warehouse"

# The Warehouse audience is Azure SQL's, not Fabric's — the token goes to a TDS
# endpoint, and the same audience is correct against real Fabric.
SQL_AUD = "https://database.windows.net"


def in_dbt_container(*args: str) -> int:
    """Run something in the dbt image, which has the ODBC driver.

    Shared with the semantic model, which also has to read the Warehouse over
    TDS. One image with the driver, used by everything that needs it, rather
    than a driver on the contributor's machine.

    The connection reaches dbt through the ENVIRONMENT, so nothing about the
    target is written into the project — the same profiles.yml points at a real
    Fabric Warehouse when these values do.
    """
    st = state.load()
    env = {
        **os.environ,
        "WAREHOUSE_ID": st["warehouse"],
        "WAREHOUSE_TOKEN": token(SQL_AUD),
        "LAKEHOUSE_ID": st["lakehouse"],
    }
    overlay = []
    if os.environ.get("TERMINAL") == "1":
        overlay = ["-f", "compose/terminal.yml"]
    return subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "versions.env",
            "-f",
            "compose/docker-compose.yml",
            "-f",
            "compose/sources.yml",
            # The SAME file set every other caller uses. Compose decides whether
            # a running container matches by hashing the config it is given, so
            # a `run` with a shorter -f list recreates fabric-emulator to match
            # — which reverted the image and dropped the terminal overlay in the
            # middle of a recording, and read as the pipeline failing.
            *overlay,
            "--profile",
            "gold",
            "run",
            "--rm",
            "-e",
            f"LAKEHOUSE_ID={st['lakehouse']}",
            *args,
        ],
        cwd=ROOT,
        env=env,
    ).returncode


def stage_the_product() -> None:
    """Copy the product's dbt project into `gold/`, where the container mounts it.

    THE MODEL GRAPH IS THE PRODUCT'S, THE MATERIALIZATION IS OURS. `gold/` keeps
    this platform's own `dbt_project.yml` and `profiles.yml`, because dbt-fabric's
    CTAS and an Entra-token connection are Fabric's business and not the
    product's. `models/`, `macros/` and `tests/` arrive from the installed
    package on every run. They are gitignored, so a local edit has nowhere to
    hide.

    This replaces a checked-in copy of all 18 files, which had already drifted:
    the product gained `cast(x as int) = 1` so its boolean comparisons run on
    Spark SQL as well as T-SQL, and the copy here never got it.
    """
    product = gold_dir()
    for name in ("models", "macros", "tests"):
        dest = ROOT / "gold" / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(product / name, dest)
    models = sorted(p.name for p in (ROOT / "gold" / "models").glob("*.sql"))
    log(f"staged the product's gold project: {len(models)} models from {product}")


def main() -> int:
    st = state.load()
    tok = token(FABRIC_AUD)
    ensure_audience(SQL_AUD, "Azure SQL")
    stage_the_product()

    wh = find_item(tok, st["workspace"], WAREHOUSE, "Warehouse")
    if wh is None:
        r = fabric(
            "POST",
            f"/workspaces/{st['workspace']}/items",
            tok,
            json={"displayName": WAREHOUSE, "type": "Warehouse"},
        )
        assert r.status_code in (201, 202), (r.status_code, r.text[:300])
        wh = r.json()
        log(f"created warehouse {WAREHOUSE}")
    else:
        log(f"reusing warehouse {WAREHOUSE}")
    assert wh["id"], wh

    state.save(warehouse=wh["id"], warehouse_name=WAREHOUSE)

    # Silver has to be visible through the lakehouse SQL analytics endpoint
    # before gold can read it by three-part name. On real Fabric it already is;
    # here the connect is what makes the emulator reflect it.
    log("verifying silver through the SQL analytics endpoint")
    rc = in_dbt_container("--entrypoint", "python", "dbt", "/tools/reflect.py")
    assert rc == 0, f"silver is not queryable from the warehouse: exit {rc}"

    # `--no-partial-parse`, and it is not a performance preference.
    #
    # dbt caches a parsed manifest under target/ and reuses it. When that cache
    # went stale here it did not fail and did not warn — it silently ran 19 of
    # the 46 data tests and reported `PASS=27 ERROR=0`, a completely green build
    # that had never executed the assertions on five of the eight models.
    # Measured: the same tree, after `rm -rf target/`, found all 46 and failed
    # one of them. A test suite that quietly shrinks is worse than no suite,
    # because the green is what people act on.
    #
    # Reparsing costs a second or two against a build that already takes longer
    # than that over TDS.
    log("dbt build (containerised dbt-fabric over TDS)")
    rc = in_dbt_container(
        "dbt", "build", "--no-partial-parse", "--profiles-dir", "/gold"
    )
    assert rc == 0, f"dbt build failed: exit {rc}"

    log(f"gold: star built in warehouse {wh['id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
