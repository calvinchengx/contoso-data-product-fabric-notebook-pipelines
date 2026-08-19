"""Invariants this repository must hold on Windows, macOS and Linux.

None of these need the emulator, Docker, or the fixture wheels — they are about
the repository itself, so they are the part of CI that is green from day one and
runs identically on all three platforms.
"""

import ast
import pathlib
import re
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
# MAKEFILE lived here when these tests were in the platform repo. The
# Makefile is the platform's; none of the tests that moved read it.
# Every top-level module the fixture wheels provide, kept out of uv.lock on
# purpose: which release they came from is the thing under test. Read off the
# wheels' own RECORD files (contoso_fixtures, contoso_fixtures_advanced,
# fabric-target) rather than guessed, and listed here because a clean checkout
# — the case this guard exists for — has no wheel to ask.
WHEELS = frozenset(
    {
        "fabric_target",
        "common",
        "source_system",
        "erp_system",
        "reference_data",
        "web_store",
    }
)


def _pins():
    out = {}
    for line in (ROOT / "versions.env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def gold_root():
    """The product's dbt project, as installed.

    `gold/models` is no longer in this repository: it belongs to
    `contoso-data-product` and `platform/gold.py` stages it on every run. These
    assertions are about the SQL this platform executes, so they read it where
    it actually comes from rather than from a copy that could disagree.
    """
    from contoso_product import gold_dir

    return gold_dir()


def builds_a_session(src: str) -> bool:
    """Does this source actually CONSTRUCT a Spark session?

    Parsed, not grepped. A substring scan cannot tell code from prose, and the
    first version of this check failed on silver's notebook for the comment
    explaining why it must never call `spark.session()` — a check that fires on
    the documentation of a rule while the rule is being obeyed. Both files here
    argue about sessions at length, so the question has to be asked of the syntax.
    """
    import ast

    def roots_at_spark(name: str | None) -> bool:
        return (name or "").split(".")[0] == "spark"

    tree = ast.parse(src)
    for node in ast.walk(tree):
        # `import spark` / `from spark import session`
        if isinstance(node, ast.Import) and any(
            roots_at_spark(a.name) for a in node.names
        ):
            return True
        if isinstance(node, ast.ImportFrom) and roots_at_spark(node.module):
            return True
        # `spark.session(...)`, `sparkmod.session(...)`, `SparkSession.builder…`
        if isinstance(node, ast.Attribute) and node.attr in (
            "session",
            "getOrCreate",
            "getActiveSession",
        ):
            return True
    return False


def real_branch() -> str:
    """The real target's arm of the resolver, as source.

    Read rather than executed: constructing the real target needs a live
    credential source, and the point of these assertions is that the value is a
    LITERAL no configuration can reach — which is a property of the text.
    """
    src = (ROOT / "steps" / "target.py").read_text(encoding="utf-8")
    return src[
        src.index("if ft.is_real:") : src.index("return Target(\n        name=EMULATOR")
    ]


def test_the_toggle_contract_is_installed_not_restated():
    """`fabric-target` is the FABRIC_TARGET contract, published by the emulator
    and installed by `make fixtures`. This repo consumes it.

    It used to restate it, and the restatement drifted: the real target
    resolved an Entra client-credentials flow and required AZURE_CLIENT_SECRET,
    so `az login` could not drive the platform, a managed identity could not,
    and it could not have run inside a Fabric notebook at all — there is no
    client secret to give there. A copied contract is a contract that gets one
    branch wrong and stays green, because the emulator does not care which
    identity showed up.
    """
    src = (ROOT / "steps" / "target.py").read_text(encoding="utf-8")
    assert "import fabric_target" in src, (
        "target.py must consume the published contract, not restate it"
    )
    assert "grant_type" not in src, "the grant type is the package's business"

    # And nowhere else may mint a token by hand. Matched on the CODE shape — a
    # quoted `grant_type` key, or the token endpoint — rather than on the bare
    # words, which appear in the prose explaining why this rule exists.
    hand_rolled = re.compile(r"""["']grant_type["']|oauth2/v2\.0/token""")
    offenders = []
    for p in sorted((ROOT / "steps").glob("*.py")):
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if hand_rolled.search(line):
                offenders.append(f"{p.name}:{i}: {line.strip()}")
    assert not offenders, (
        "tokens come from the target's credential, so that az login, a service "
        f"principal and a notebook's managed identity all work: {offenders}"
    )


def test_the_emulator_appears_only_in_the_target_resolver():
    """This platform must run unmodified against real Fabric.

    Every difference between the local family and production lives in
    platform/target.py, selected by FABRIC_TARGET. A seeded credential, a
    localhost URL or a TLS bypass anywhere else is a workaround that would ship
    to production — and would be invisible, because the emulator would go on
    passing.
    """
    emulator_only = re.compile(
        r"localhost:9443|localhost:8443|daemon-app-secret|00d88624|"
        r"6f89cf12|allow_invalid_certificates|verify\s*=\s*False"
    )
    offenders = []
    for p in sorted((ROOT / "steps").glob("*.py")):
        if p.name == "target.py":
            continue
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if emulator_only.search(line):
                offenders.append(f"{p.name}:{i}: {line.strip()}")
    assert not offenders, "these belong in target.py, behind FABRIC_TARGET: " + str(
        offenders
    )


def test_the_platform_never_changes_a_workspace_capacity():
    """No step may move a workspace between capacities.

    Against the emulator that call is harmless. Against real Fabric it takes a
    live workspace off the capacity an operator chose, which changes what it
    bills to and disturbs whatever is running on it, as a side effect of a
    pipeline run. A capacity is supplied to `POST /workspaces` at create and an
    existing workspace's capacity is adopted as it stands.

    Checked as text against the whole platform, not one file, because the
    hazard is the API call existing anywhere in a step.
    """
    offenders = []
    for p in sorted((ROOT / "steps").glob("*.py")):
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if "assignToCapacity" in line or "unassignFromCapacity" in line:
                offenders.append(f"{p.name}:{i}: {line.strip()}")
    assert not offenders, (
        "a step moves a workspace between capacities, which must never happen "
        "against real Fabric: " + str(offenders)
    )


def test_the_capacity_name_is_one_arm_accepts():
    """ARM's name rule for a Fabric capacity, checked against our constant.

    `^[a-z][a-z0-9]{2,62}$` — no hyphens, no capitals. Every other name in this
    platform is hyphenated, so this one looks like a typo and will eventually
    be "fixed" by someone tidying up. It is not a style choice: ARM answers 400
    InvalidResourceName, and only when the stack is actually running.

    Parsed rather than imported, because importing target.py resolves a target
    and `make test` promises no emulator and no fixture wheels.
    """
    tree = ast.parse((ROOT / "steps" / "target.py").read_text(encoding="utf-8"))
    names = [
        node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
        for t in node.targets
        if isinstance(t, ast.Name) and t.id == "CAPACITY"
    ]
    assert names, "platform/target.py should define CAPACITY"
    assert re.fullmatch(r"[a-z][a-z0-9]{2,62}", names[0]), (
        f"CAPACITY={names[0]!r} is not a name ARM will accept"
    )


def test_the_fabric_client_knows_nothing_about_the_source_systems():
    """Segregation, in the other direction.

    Contoso POS, Web and ERP are vendors a Fabric pipeline pulls from — not
    Fabric. A Fabric client that knows what a vendor's DSN looks like has mixed
    two things that change for different reasons and at different times.
    """
    src = (ROOT / "steps" / "fabric.py").read_text(encoding="utf-8")
    for leak in ("POS_", "ERP_", "DEBEZIUM", "REDPANDA", "postgresql://"):
        assert leak not in src, f"fabric.py should not mention {leak}"


def test_the_transforms_are_engine_side():
    """bronze and silver must scale.

    They run in the engine — Spark reading OneLake directly — so the same code
    holds at a hundred million rows. A client-side dataframe library here would
    put one machine in the data path and cap the platform at its memory, which
    is exactly what a single-node engine quietly does.
    """
    # BOTH are Fabric notebooks now, and neither builds a session. `spark` is
    # ambient inside a notebook — Fabric's pool binds it to a session already
    # carrying the workspace identity — so constructing a second one is the thing
    # this rule exists to prevent. bronze used to be a script calling
    # `spark.session()`, which meant Spark Connect, which no Fabric tenant
    # exposes: it could not have run in production.
    transforms = [
        "definitions/bronze-ingest.Notebook/notebook-content.py",
        "definitions/silver-conform.Notebook/notebook-content.py",
    ]
    for name in transforms:
        src = (ROOT / "steps" / name).read_text(encoding="utf-8")
        # THE AMBIENT SESSION, HANDED TO THE PRODUCT. The reads themselves are
        # `contoso_product`'s now, so what this file must show is that it passes
        # the session Fabric bound rather than making one, and that the engine
        # is what touches the data.
        assert "spark," in src or "spark, " in src or "(spark" in src, (
            f"{name} does not hand the ambient session to the product"
        )
        assert not builds_a_session(src), (
            f"{name} builds its own session — inside a notebook `spark` is "
            f"ambient, and a second session ignores the bound lakehouse"
        )
        for single_node in ("duckdb", "pandas", "deltalake", "pyarrow"):
            assert single_node not in src, (
                f"{name} pulls data client-side with {single_node} — the "
                f"transforms must stay in the engine to scale"
            )

    # AND THE OPERATORS HOLD NO SESSION AT ALL. This is the half that was
    # unenforceable while bronze was a script: a step that submits a notebook and
    # then also builds a dataframe has quietly put one machine back in the data
    # path. Reading the run metrics with delta-rs is allowed and is not that —
    # one row, by construction (see bronze.py's metrics_row).
    for operator in ("bronze.py", "silver.py"):
        src = (ROOT / "steps" / operator).read_text(encoding="utf-8")
        assert not builds_a_session(src), (
            f"{operator} holds a Spark session — the transform runs on Fabric's "
            f"engine now, so the operator only publishes, submits and grades"
        )


def test_every_file_read_and_write_names_its_encoding():
    """`read_text()` uses the LOCALE default, and the locale is not ours.

    On Windows that is cp1252, so the first em dash in a file this repository
    reads raises `UnicodeDecodeError: 'charmap' codec can't decode byte 0x8f`.
    Six tests died that way in CI — on a platform none of this was written on,
    from source that was fine — and the message names an encoding nobody chose.

    Every read here is of a file in THIS repository, all of which are UTF-8, so
    the encoding is never in doubt; it just has to be said. This guard exists
    because the failure is invisible on macOS and Linux, which is where the code
    gets written: the developer cannot reproduce it, and CI reports it as six
    unrelated tests breaking at once.
    """
    # Split, so the needles do not appear literally in this file and make the
    # guard flag its own source — which is exactly what it did first time.
    bare_read_needle = ".read_text" + "()"
    write_needle = ".write_text" + "("

    offenders = []
    for d in ("tests", "scripts", "steps"):
        for path in sorted((ROOT / d).rglob("*.py")):
            for i, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), start=1
            ):
                bare_read = bare_read_needle in line
                # Only single-line calls are checkable here; one split across
                # lines carries its encoding on a later one.
                bare_write = (
                    write_needle in line
                    and line.rstrip().endswith(")")
                    and "encoding=" not in line
                )
                if bare_read or bare_write:
                    offenders.append(f"{path.relative_to(ROOT)}:{i}: {line.strip()}")
    assert not offenders, (
        "these read or write a file without naming an encoding, so they use "
        "the locale default and break on Windows:\n  " + "\n  ".join(offenders)
    )


def test_fabric_refuses_a_path_that_already_carries_v1():
    """The helper checks its own argument, not just a lint on call sites.

    `test_no_fabric_call_carries_its_own_v1_prefix` reads source text, so it
    cannot see a path assembled at runtime. This is the guard that holds either
    way: `fabric()` raises rather than building `/v1/v1/...` and letting the
    emulator answer 404 `UnknownEndpoint` — a successful HTTP response that
    `requests` does not raise on, which is how the original bug stayed silent.
    """
    # `apipath`, not `fabric`: importing the client resolves a target, which
    # needs the fabric-target wheel `make fixtures` installs. This file is the
    # part of CI that runs without it (see the module docstring), so the rule
    # lives in a module with no dependencies and the guard is tested there.
    sys.path.insert(0, str(ROOT / "steps"))
    import apipath

    def refused(path):
        try:
            apipath.check(path)
        except ValueError as e:
            return str(e)
        return None

    for bad in ("/v1", "/v1/workspaces/abc/lineage"):
        msg = refused(bad)
        assert msg, f"fabric() accepted {bad!r} and would request /v1/v1/..."
        # The message must say what to pass INSTEAD; an error that only says
        # "no" leaves the reader to rediscover the prefix rule.
        assert "adds the /v1 prefix" in msg, msg

    assert refused("workspaces/abc"), "a path without a leading slash was accepted"

    # And the correct form is NOT refused — a guard that rejects everything
    # would pass every assertion above while breaking every call in the repo.
    # Asserted against the rule itself rather than by calling `fabric()` and
    # swallowing the transport error: `except Exception: pass` would also have
    # accepted a TypeError from an unrelated bug, which is not what is meant.
    assert apipath.check("/workspaces") == "/workspaces"


def test_no_fabric_call_carries_its_own_v1_prefix():
    """`fabric()` adds `/v1`; a caller that adds one too gets `/v1/v1/...`.

    That is not hypothetical. `emulator_producers` did it, so every provenance
    lookup 404'd — and because a 404 is not an exception, the `except` never
    fired and `.get("value", [])` turned the failure into an empty graph. The
    platform then labelled every edge "the emulator recorded no edge" and
    printed `0 carrying a producer the emulator observed`.

    Zero is exactly what a correct lookup reports for a platform that only ever
    declares its lineage, which is why the number survived so long unquestioned.
    The real answer was four.
    """
    offenders = []
    for path in sorted((ROOT / "steps").rglob("*.py")):
        for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if 'fabric("' in line and '"/v1' in line:
                offenders.append(f"{path.relative_to(ROOT)}:{i}: {line.strip()}")
            # The two-line form: path built above, passed below.
            if 'path = f"/v1/' in line or 'path = "/v1/' in line:
                offenders.append(f"{path.relative_to(ROOT)}:{i}: {line.strip()}")
    assert not offenders, (
        "fabric() already prefixes /v1; these add a second one and 404:\n  "
        + "\n  ".join(offenders)
    )


def test_credentials_come_from_key_vault():
    """Secrets live in Key Vault — the emulator's locally, a real Azure Key
    Vault in production — never in the source tree.

    A credential in a repository has already leaked: it is in every clone, in
    the reflog, and in whatever CI cached the checkout. It also skips the part
    a real deployment must get right — an identity permitted to read a vault,
    and rotation without a code change.

    Two exceptions, both structural:
      * target.py holds the BOOTSTRAP Entra credential — reading the vault
        requires it, so it cannot live there.
      * seed_secrets.py is the one place a value appears, because a clone has
        to be self-contained. It does not run against real vendors.
    """
    allowed = {"target.py", "seed_secrets.py"}
    pattern = re.compile(r"(password|secret|api[_-]?key)\s*=\s*[\"'][^\"']{6,}", re.I)
    offenders = []
    for p in sorted((ROOT / "steps").glob("*.py")):
        if p.name in allowed:
            continue
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or "_SECRET" in stripped:
                continue
            if pattern.search(stripped):
                offenders.append(f"{p.name}:{i}: {stripped}")
    assert not offenders, f"read these from the vault instead: {offenders}"


def test_the_schedule_step_asserts_the_run_SUCCEEDED():
    """ "It fired" is not the claim; "it ran unattended" is.

    This step asserted only that a job instance with invokeType=Scheduled had
    appeared. It logged "the platform runs unattended" over a run that died
    mid-notebook on a Delta commit conflict, and `make verify` reported 14/14
    across two such failures. A schedule that reliably starts something that
    reliably fails is an alarm nobody wired up.
    """
    src = (ROOT / "steps" / "schedule.py").read_text(encoding="utf-8")
    assert "await_terminal(" in src, (
        "the fired job is never polled to a terminal state, so its outcome is "
        "unknown and the step passes on a failure"
    )
    assert 'detail.get("status") == "Completed"' in src, (
        "the fired job's status is never asserted"
    )
    assert "await_quiet(" in src, (
        "the schedule is created without waiting for the previous step's run "
        "to finish; both write silver and one loses the Delta commit race"
    )
    assert 'detail["endTimeUtc"] >= detail["startTimeUtc"]' in src, (
        "nothing checks that the fired job did not end before it started, "
        "which is what resetting the clock under a running job produces"
    )


def test_silver_runs_as_a_fabric_notebook():
    """The transform is notebook code, not code that resembles it.

    `spark.py` claims the transforms are paste-able into a Fabric notebook. A
    claim like that is worth nothing until something runs it as one — so silver
    publishes a Notebook item and submits a RunNotebook job, and the notebook's
    source is a REAL FILE rather than a string assembled at publish time.

    The file matters as much as the job. A notebook built from an f-string is
    not Python as far as any tool is concerned: ruff does not lint it, ty does
    not check it, and a syntax error in the transform surfaces as a failed cell
    on a remote engine instead of at `make lint`.
    """
    nb = (
        ROOT
        / "steps"
        / "definitions"
        / "silver-conform.Notebook"
        / "notebook-content.py"
    )
    assert nb.exists(), "the silver transform is not a notebook file"
    assert nb.read_text(encoding="utf-8").startswith("# Fabric notebook source"), (
        "a Fabric notebook is identified by its first line; without it the "
        "emulator's parser sees one unmarked cell rather than the cells written"
    )
    assert "# CELL " in nb.read_text(encoding="utf-8"), "the notebook declares no cells"

    # The job is submitted through the shared operator, so the literal lives
    # there. Asserted in BOTH places on purpose: that silver delegates, and that
    # what it delegates to really submits a RunNotebook job. Checking only the
    # first would pass against a `notebookjob` that did nothing at all.
    runner = (ROOT / "steps" / "notebookjob.py").read_text(encoding="utf-8")
    assert "jobType=RunNotebook" in runner, "notebookjob never submits a notebook job"
    for step in ("silver.py", "bronze.py"):
        src = (ROOT / "steps" / step).read_text(encoding="utf-8")
        assert "notebookjob.submit(" in src, f"{step} never submits a notebook job"
        # Matched on the CALL, not its exact signature — the earlier form of this
        # assertion broke the moment an encoding argument was added, which is not
        # a change it cares about.
        assert "notebookjob.content(" in src, (
            f"{step} must publish the notebook FILE — a body built inline is the "
            f"thing this test exists to prevent"
        )


def test_notebook_lineage_is_observed_not_declared():
    """An edge records what happened. Nothing else is allowed to invent one.

    The first version of this had the publishing step declare one read set and
    one write set for the whole notebook. The emulator paired every read with
    every write — correctly, given what it was told — and silver, which reads
    two tables and writes three, produced six edges for three movements. Half
    the graph described data that never moved, and it looked exactly as
    plausible as the half that did.

    So the notebook's own IO helpers record each movement as it happens, and
    the engine attributes the delta to the cell that produced it. A declared
    set drifts from the code the moment either changes; this one cannot,
    because it is the code.
    """
    nb = (
        ROOT
        / "steps"
        / "definitions"
        / "silver-conform.Notebook"
        / "notebook-content.py"
    ).read_text(encoding="utf-8")
    # THE OBSERVATION MOVED, THE RULE DID NOT. The transform now lives in
    # `contoso-data-product`, so the IO helpers that record each movement live
    # there too. Asserted against the installed package, because that is the
    # code that runs.
    from contoso_product import silver as product_silver

    src_product = pathlib.Path(product_silver.__file__).read_text(encoding="utf-8")
    assert 'lineage.append(("read"' in src_product, (
        "the product's silver does not record reads"
    )
    assert 'lineage.append(("write"' in src_product, (
        "the product's silver does not record writes"
    )
    # And the notebook must still hand the result on rather than inventing one.
    assert "run_silver(" in nb, "the notebook no longer calls the product"

    # Per-cell attribution is now the EMULATOR's job — it watches its own data
    # plane and tags each access with the cell that made it. What this platform
    # must not do is go back to declaring a set, which is what cross-multiplied
    # into phantom edges. The edge COUNT is asserted for real by the govern
    # step in `make verify`, which is where a regression would actually show.
    src = (ROOT / "steps" / "silver.py").read_text(encoding="utf-8")
    for declared in ("READS", "WRITES"):
        assert f"{declared} = [" not in src, (
            f"silver declares {declared} again — the read/write set must be "
            f"observed by the notebook, never named by the step publishing it"
        )


def test_the_platform_proves_it_runs_unattended():
    """A schedule that is created but never observed to fire proves nothing.

    This is the failure mode the step exists for: if scheduling breaks, the
    data is simply yesterday's and every row count, table and dashboard still
    looks correct. So the assertion is not "a schedule exists" but "a run
    appeared that the SCHEDULER produced" — which only `invokeType` can say,
    because a scheduled run and a manual one are otherwise the same job doing
    the same work.
    """
    src = (ROOT / "steps" / "schedule.py").read_text(encoding="utf-8")
    assert '"Scheduled"' in src, (
        "the step must filter job instances by invokeType — without it the "
        "assertion cannot tell a scheduled run from the manual one that "
        "already ran earlier in the pipeline"
    )
    assert "T.clock_is_controllable" in src, (
        "moving time is emulator-only and must be gated by the target"
    )
    # The real branch must SAY it is not asserting the firing. A silent skip
    # would report success for a scheduler nobody exercised.
    assert "is not asserted here" in src, (
        "the real target skips the firing assertion and must say so out loud"
    )


def test_the_schedule_step_runs_last():
    """The clock jump must not land under another step.

    Proving a schedule fires means advancing the emulator's clock, and every
    job status and long-running operation in the stack derives from that clock.
    An hour jumped mid-run would surface as a completed job or an expired
    operation in whatever step came next — an unrelated-looking failure with a
    cause nobody would find from the error.
    """
    steps = re.findall(
        r'^\s*\("([a-z_]+)",',
        (ROOT / "steps" / "pipeline.py").read_text(encoding="utf-8"),
        re.M,
    )
    assert steps, "pipeline.py declares no steps"
    assert "schedule" in steps, "the schedule step is not in the pipeline at all"
    after = steps[steps.index("schedule") + 1 :]
    assert not after, f"schedule must run last; these follow it: {after}"


def test_the_clock_advance_fits_inside_one_token_lifetime():
    """Moving Fabric's clock does not move the identity provider's.

    Only the Fabric emulator's clock responds to the advance; the Entra
    emulator that mints the tokens keeps its own. Jump further than a token
    lives and the two disagree permanently: every call afterwards 401s with
    `invalid token: expired`, INCLUDING freshly minted ones, because the new
    token is already past its expiry as far as Fabric is concerned.

    It cost an hour to diagnose once, because it presents as an authentication
    fault and is really a consequence of the clock lever the schedule step
    exists to pull. The step asserts it at import; this asserts the assertion,
    so raising the cadence cannot quietly remove the guard.
    """
    src = (ROOT / "steps" / "schedule.py").read_text(encoding="utf-8")
    m_int = re.search(r"^INTERVAL_MINUTES = (\d+)", src, re.M)
    m_life = re.search(r"^TOKEN_LIFETIME_SECONDS = (\d+)", src, re.M)
    assert m_int and m_life, "the step no longer declares its cadence and lifetime"
    interval, lifetime = int(m_int.group(1)), int(m_life.group(1))
    advance = interval * 60 + 300
    assert advance < lifetime, (
        f"advancing {advance}s outruns the {lifetime}s token lifetime; "
        f"every call after the jump would fail as an auth error"
    )
    assert "TOKEN_LIFETIME_SECONDS" in src and "assert ADVANCE_SECONDS <" in src, (
        "the step must guard this itself, or a future cadence change fails "
        "three calls later with an error naming neither the clock nor this rule"
    )


def test_the_schedule_step_puts_the_clock_back():
    """A stack left an interval ahead hands out tokens Fabric reads as expired.

    The next unrelated command — the portal, a re-run, capture — then fails
    with an authentication error nobody would trace back to a schedule
    assertion minutes earlier. Restoring the offset is part of the step, not
    cleanup someone is trusted to remember.
    """
    src = (ROOT / "steps" / "schedule.py").read_text(encoding="utf-8")
    assert "def reset_clock()" in src, "the step never restores the clock"

    # THE GUARANTEE, NOT THE MECHANISM. This used to assert that reset_clock()
    # appeared textually before the first assertion — which did give the
    # property, but pinned one particular way of getting it. The step now has
    # to poll the fired job to a terminal state while the clock is STILL
    # advanced: that job's startTimeUtc was stamped in the advanced frame, so
    # resetting first lands its endTimeUtc in the old one and the instance
    # comes back having ended before it began. `finally` restores the clock on
    # every path, failing ones included, without dictating the order.
    body = src[src.index("def main()") :]
    assert "finally:" in body, (
        "the clock is restored on the happy path only; a failing assertion "
        "leaves the stack an interval ahead of the token issuer"
    )
    tail = body[body.index("finally:") :]
    assert "reset_clock()" in tail[:300], (
        "reset_clock() is not in the finally block, so a failed run leaves the "
        "stack broken for whatever runs next"
    )


def test_the_platform_proves_it_reacts_to_a_delivery():
    """A schedule answers "run at 02:00"; it cannot answer "run when it lands".

    For an external feed that is the question that matters — the vendor's
    export finishes when it finishes, and a fixed hour either reprocesses
    yesterday or processes nothing. So the assertion is that a DROPPED FILE
    started a run, evidenced by `invokeType: "EventTriggered"`, not that a
    trigger record exists.
    """
    src = (ROOT / "steps" / "trigger.py").read_text(encoding="utf-8")
    assert '"EventTriggered"' in src, (
        "the step must filter job instances by invokeType — nothing else can "
        "say the trigger is what started the run"
    )
    assert "T.event_triggers_have_rest_api" in src, (
        "binding a trigger is emulator-only and must be gated by the target"
    )
    # The real branch must SAY it asserts nothing, for the same reason the
    # schedule step must: a silent skip reports success for an unwired feature.
    assert "asserts nothing" in src, (
        "the real target cannot bind a trigger and must say so out loud"
    )


def test_the_trigger_watches_a_marker_not_the_landing_zone():
    """A prefix over the vendor's parts would fire once per part.

    The POS export lands as 21 files. A trigger watching that prefix would
    start 21 refreshes of the same delivery — each one correct in isolation and
    the set of them nonsense. File-arrived and delivery-finished are different
    events, and only the second is worth acting on, which is why the watched
    prefix is a dedicated marker path.
    """
    src = (ROOT / "steps" / "trigger.py").read_text(encoding="utf-8")
    watched = re.search(r'^WATCHED_PREFIX = "([^"]+)"', src, re.M)
    marker = re.search(r'^MARKER = "([^"]+)"', src, re.M)
    assert watched and marker, "the step no longer declares what it watches"
    assert marker.group(1).startswith(watched.group(1)), (
        "the marker must land under the watched prefix, or the trigger cannot fire"
    )
    # The guard that matters: the watched prefix must not be an ancestor of the
    # feed's own parts, which land directly under the vendor directory.
    assert not watched.group(1).rstrip("/").endswith("contoso_pos"), (
        "watching the vendor directory itself fires once per landed part"
    )


def test_the_source_systems_are_named_in_lineage():
    """A medallion does not begin in Fabric. It begins at a vendor.

    Every edge used to need a (workspace, item, path) triple at both ends, so
    the first hop could only be drawn from a file already sitting in
    `Files/landing/` and the system that PUT it there could not be said at all.
    `ingest_pos.py` claimed in its own docstring that the vendor was "a node in
    the lineage graph rather than a filename in Files/landing" long before
    anything made that true.

    A CONNECTION and not a URI: it holds the credential, carries a display
    name, and is what the ingesting client actually authenticated through — so
    naming it records what happened instead of a string this platform invented.
    """
    for step, vendor in (
        ("ingest_pos.py", "Contoso POS"),
        ("ingest_erp_cdc.py", "Contoso ERP"),
    ):
        src = (ROOT / "steps" / step).read_text(encoding="utf-8")
        assert "connections.ensure(" in src, f"{step} names no source system"
        assert vendor in src, f"{step} does not identify {vendor}"
        assert "connections.announce(" in src, f"{step} never reports its lineage"

    helper = (ROOT / "steps" / "connections.py").read_text(encoding="utf-8")
    assert '"connectionId"' in helper, (
        "the read side of a source edge must carry connectionId — a source "
        "system has no workspace and no path inside it"
    )


def test_lineage_reports_use_the_precise_move_form():
    """Flat read/write lists cross-product, and the cross product overstates.

    A step reading two feeds and writing two paths would claim four movements
    where two happened. That is not hypothetical: it put three phantom edges
    into this repository's own graph once, each as plausible-looking as the
    real ones. `moves` pairs each derivation explicitly.
    """
    helper = (ROOT / "steps" / "connections.py").read_text(encoding="utf-8")
    assert '"moves": moves' in helper, (
        "reports must use the precise `moves` form, never flat reads/writes"
    )
    # One move PER path, not one move listing them all: the comprehension is
    # what keeps the customers feed from being credited with the orders file.
    body = helper[helper.index("def from_source") : helper.index("def announce")]
    assert "for p in paths" in body, (
        "from_source must produce one move per landed path — the customers "
        "feed did not produce the orders file"
    )


def test_the_landing_hop_is_reported_so_sources_are_not_orphans():
    """Naming the vendor is not enough on its own.

    bronze reads `abfs://` with Spark, so the emulator sees bytes leave OneLake
    and bytes arrive with nothing tying one to the other — it records no
    landing->bronze edge. Without bronze reporting that hop, the vendor nodes
    hang off landing paths no later edge mentions, and the source systems float
    beside the medallion instead of feeding it.

    The paths must also AGREE. The ingest steps write under a date partition
    and bronze reads the same partition; a reported target that merely looks
    right joins the vendor to a node nothing else references.
    """
    bronze = (ROOT / "steps" / "bronze.py").read_text(encoding="utf-8")
    assert "connections.announce(" in bronze, (
        "bronze must report landing->bronze, or the source nodes are orphans"
    )
    ingest = (ROOT / "steps" / "ingest_pos.py").read_text(encoding="utf-8")
    # Both sides key the path on the landing day, which is what makes the
    # vendor edge and the bronze edge meet at the same node.
    assert "contoso_pos/{day}/" in ingest, (
        "the reported landing path must carry the date partition bronze reads"
    )
    assert "contoso_pos/{day}/" in bronze, "bronze no longer reads a dated partition"


def test_the_pbip_folder_carries_what_a_real_one_requires():
    """A PBIP semantic-model folder has two required files and we write both.

    Microsoft marks `definition.pbism` required always, and `model.bim`
    required when saving in TMSL format — which `definition.pbism` version 1.0
    is what declares. Getting the pair inconsistent produces a folder that
    looks right in a listing and that Power BI Desktop refuses to open, with an
    error naming a file rather than the mismatch.
    """
    import json
    import shutil
    import sys
    import tempfile

    sys.path.insert(0, str(ROOT / "steps"))
    import pbip

    tmp = pathlib.Path(tempfile.mkdtemp())
    try:
        model = {"name": "M", "compatibilityLevel": 1550, "model": {"tables": []}}
        folder = pbip.write(tmp, model)

        names = {p.name for p in folder.iterdir()}
        assert {"definition.pbism", "model.bim", ".platform"} <= names, names
        assert folder.name.endswith(".SemanticModel"), folder.name

        pbism = json.loads((folder / "definition.pbism").read_text(encoding="utf-8"))
        # version 1.0 means "the model is TMSL, in model.bim". Declaring 4.0+
        # would also permit a TMDL folder this platform cannot write.
        assert pbism["version"] == "1.0", pbism
        assert (folder / "model.bim").exists(), "version 1.0 requires model.bim"

        plat = json.loads((folder / ".platform").read_text(encoding="utf-8"))
        assert plat["metadata"]["type"] == "SemanticModel", plat
        # The logicalId is what makes a redeploy update the SAME item rather
        # than create a second one, so it must be stable across runs.
        again = pbip.write(tmp, model)
        assert (
            json.loads((again / ".platform").read_text(encoding="utf-8"))["config"][
                "logicalId"
            ]
            == (plat["config"]["logicalId"])
        )

        # The definition on disk is the one that was published, byte for byte.
        assert json.loads((folder / "model.bim").read_text(encoding="utf-8")) == model
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_the_repo_tests_need_no_fixture_wheels():
    """This file's first paragraph is a promise. Here it is enforced.

    These tests are the part of CI that is green from day one on all three
    platforms — no emulator, no Docker, no wheels installed from a release.
    `make test` runs them with `uv run --frozen`, which knows only `uv.lock`,
    and `fabric-target` is deliberately NOT in the lock: which release it came
    from is the thing under test.

    So importing `fabric` or `target` from here fails on a clean checkout with
    `ModuleNotFoundError: No module named 'fabric_target'`. That is not
    hypothetical — it turned CI red on ubuntu, macOS and Windows simultaneously
    the day a runtime guard was tested by importing the client that carries it.
    The guard moved to `platform/apipath.py`, which has no dependencies, and
    this test stops the next one going the same way.

    It did not. It checked only THIS file, and only DIRECT imports, so
    `tests/test_reconcile.py` walked straight past it importing `reconcile`,
    which reached the wheel three hops later through `state` and `fabric`. CI
    was red for three days. Both holes are closed below: every test module is
    read, and the taint is followed through the import graph rather than
    guessed at from a list of names.
    """
    plat = ROOT / "steps"

    def interesting(name):
        return name in WHEELS or (plat / f"{name}.py").exists()

    def imports_in(node):
        """Top-level module names imported anywhere under `node`."""
        names = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Import):
                names |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
                names.add(n.module.split(".")[0])
        return {n for n in names if interesting(n)}

    def module_scope_imports(path):
        """What importing this file executes, so function bodies are dropped.

        Deferring an import into the function that needs a running stack is the
        sanctioned fix — it is how `reconcile` keeps `compare` reachable — so a
        function-scoped import is not a module-scope dependency. `ast.walk`
        descends into functions, hence the strip.
        """
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tree.body = [
            n
            for n in tree.body
            if not isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
        ]
        return imports_in(tree)

    # Which of our own modules reach a wheel, following imports to a fixpoint.
    tainted = set(WHEELS)
    graph = {p.stem: module_scope_imports(p) for p in plat.glob("*.py")}
    changed = True
    while changed:
        changed = False
        for mod, deps in graph.items():
            if mod not in tainted and deps & tainted:
                tainted.add(mod)
                changed = True

    offenders = {}
    for test in sorted((ROOT / "tests").glob("test_*.py")):
        tree = ast.parse(test.read_text(encoding="utf-8"))
        # Importing the module runs these, so they are unconditional.
        for hit in sorted(module_scope_imports(test) & tainted):
            offenders.setdefault(test.name, []).append(hit)
        # Inside a test body the import still runs when the test does, so
        # deferring buys nothing here — it only earns the right to be marked.
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            marked = any(
                isinstance(d, ast.Attribute) and d.attr == "fixtures"
                for d in node.decorator_list
            )
            if marked:
                continue
            for hit in sorted(imports_in(node) & tainted):
                offenders.setdefault(test.name, []).append(f"{node.name} -> {hit}")

    assert not offenders, (
        f"these tests reach code that needs `make fixtures`, which `make test` "
        f"promises not to require: {offenders}. Put the part under test in a "
        f"dependency-free module, defer the heavy import into the function that "
        f"needs a running stack, or mark the test `@pytest.mark.fixtures` so "
        f"`make test-fixtures` runs it where the wheels exist."
    )


def test_the_schedule_step_identifies_its_run_by_id_not_position():
    """The API returns job instances NEWEST-FIRST.

    `after[-1]` therefore picks the OLDEST scheduled run — the occurrence that
    fired when the schedule was created, not the one the clock advance
    produced. That is not a style point: the step asserts the fired run reached
    Completed, and pointing that assertion at the wrong job is how a scheduled
    run that died with `Failed to commit transaction: 0` was recorded on video
    while `make verify` reported 14/14 green.

    A set difference over ids cannot pick the wrong job whichever way the list
    is ordered, so the ordering stops being something this step has to know.
    """
    src = (ROOT / "steps" / "schedule.py").read_text(encoding="utf-8")
    assert 'j["id"] not in before' in src, (
        "the fired run must be identified by id difference, not list position"
    )
    # CODE, not prose — the comment above the fix necessarily names the wrong
    # spelling in order to warn against it, and a bare substring search cannot
    # tell the warning from the mistake. This is the same trap
    # test_the_toggle_contract_is_installed_not_restated documents for
    # `grant_type`.
    code = [ln for ln in src.splitlines() if not ln.lstrip().startswith("#")]
    offenders = [ln.strip() for ln in code if "after[-1]" in ln]
    assert not offenders, (
        f"after[-1] is the OLDEST scheduled run — the API returns "
        f"newest-first: {offenders}"
    )


def test_the_schedule_step_does_not_race_its_own_first_occurrence():
    """Creating the schedule fires an occurrence; advancing fires another.

    They are an interval apart in VIRTUAL time and seconds apart in real time,
    so both notebooks execute at once and collide writing the same silver
    tables. Compressing time compresses the executions too — the one property
    of a controllable clock with no real-Fabric counterpart, and the actual
    cause of the failure above.

    So the step waits for the create-fired run to settle before it advances.
    """
    src = (ROOT / "steps" / "schedule.py").read_text(encoding="utf-8")
    body = src[src.index("def main()") :]
    quiets = [i for i in range(len(body)) if body.startswith("await_quiet(", i)]
    assert len(quiets) >= 2, (
        "main() must quiet the notebook twice: once before creating the "
        "schedule (step 13's trigger run) and once before advancing (the "
        "occurrence the create itself fired)"
    )
    assert quiets[-1] < body.index("advance(ADVANCE_SECONDS)"), (
        "the second await_quiet must come BEFORE the advance, or the two runs race"
    )


def test_the_grpc_transport_is_quiet_before_pyspark_loads():
    """gRPC's C-core logs at INFO, and the order of these lines matters.

    Spark Connect talks gRPC. Every step that holds a session and then spawns a
    subprocess emits one `ev_poll_posix.cc:593] FD from fork parent still in
    poll list` per inherited descriptor — 14 lines per `govern.py` run,
    measured — which buries the step's own output and, in a recorded demo, the
    whole terminal pane.

    The C-core reads GRPC_VERBOSITY when it initialises, so setting it AFTER
    pyspark is imported is a no-op that looks like a fix. Hence the ordering
    assertion rather than a presence one.
    """
    src = (ROOT / "steps" / "spark.py").read_text(encoding="utf-8")
    assert "GRPC_VERBOSITY" in src, "the gRPC transport's INFO chatter is unmuted"
    assert src.index("GRPC_VERBOSITY") < src.index("from pyspark"), (
        "GRPC_VERBOSITY must be set BEFORE pyspark is imported — the C-core "
        "reads it at initialisation, so a later assignment silences nothing"
    )
    # ERROR, not NONE: a transport that genuinely fails must still say so.
    assert '"GRPC_VERBOSITY", "ERROR"' in src, (
        "silence the INFO chatter, not the diagnostics"
    )


def test_no_table_with_a_nested_column_is_read_over_tds():
    """A nested column corrupts its whole table through the SQL endpoint.

    The emulator's reflection walks Parquet LEAF columns POSITIONALLY and hands
    each top-level column whatever leaf shares its index. A `struct`/`array`/`map`
    contributes several leaves, so it does not merely reflect wrong itself — it
    SHIFTS EVERY COLUMN AFTER IT, and their real values are dropped. Nothing
    raises: the types look ordinary and the values are plausible.

    Since fabric-emulator v0.16.0 the displacement is fixed — measured against
    the released build, with a flat column positioned after three nested ones
    coming back correct. So this is no longer about corruption.

    IT STILL HOLDS, because nested data is not reachable over TDS on any build.
    Real Fabric does not represent these columns at all: "Types that aren't
    listed in the table aren't represented as the table columns in the SQL
    analytics endpoint", and its Delta-to-SQL mapping lists no struct, array or
    map.

    The pinned build does that too, as of v0.16.1 — measured: a table written
    with array, struct and map columns reflects with those three absent, and a
    bigint sentinel placed after them still reads 999, so nothing shifts. It was
    not always so. v0.16.0 surfaced them as varchar NULL and v0.15.3 filled them
    with another column's value, displacing everything after. This test does not
    depend on which, because the invariant is about which tables get read over
    TDS at all — it held through both and needs no revisiting at the next bump.

    So a table read over TDS loses whatever the vendor nested inside it, whether
    by returning nothing or by not offering the column. Spark reads Delta
    directly and gets the real thing, which is why bronze_web_orders is read by
    the notebook and never by dbt or the catalog.
    """
    # WHAT ACTUALLY GOES OVER TDS, and only that. govern reads MEDALLION's
    # LAKEHOUSE entries through Spark — Delta directly, immune to reflection —
    # so listing a nested-bearing table there is fine and the catalog should
    # carry it. The TDS readers are dbt (its declared sources) and govern's
    # WAREHOUSE column discovery. An earlier version of this test swept all of
    # MEDALLION into the TDS set and fired on a safe lakehouse entry — an
    # over-broad guard is how a true invariant gets deleted in annoyance.
    govern = (ROOT / "steps" / "govern.py").read_text(encoding="utf-8")
    start = govern.index("MEDALLION = [")
    end = govern.index("]", govern.index("fct_revenue_summary", start))
    warehouse_entries = set(
        re.findall(r'\(\s*"warehouse",\s*"(\w+)"', govern[start:end])
    )

    sources = (gold_root() / "models" / "sources.yml").read_text(encoding="utf-8")
    dbt_sources = set(re.findall(r"^\s+- name: (\w+)$", sources, re.M))

    # Declared in web_schema.py as a map, which is how that module says "array
    # of struct". Any future nested column has to be added here.
    nested_tables = {"bronze_web_orders"}

    over_tds = warehouse_entries | dbt_sources
    leaked = nested_tables & over_tds
    assert not leaked, (
        f"{sorted(leaked)} carries a nested column and is read over TDS — every "
        f"column at or after the nested one reflects another column's value, "
        f"silently. Read it with Spark, or flatten it in silver first."
    )


def test_money_is_catalogued_as_decimal_not_double():
    """The catalog must not describe money as a binary float.

    Money is stored as `decimal(19,4)` so that a P&L is exact rather than close.
    A catalog publishing it as DOUBLE tells every downstream consumer the
    opposite of what the warehouse guarantees — and a catalog is believed
    precisely because nobody re-derives it.

    ORDER IS THE ASSERTION. `decimal` has to be matched before the float branch;
    folded together, as it was, `decimal` fell through to DOUBLE. That was
    harmless while money was a float and wrong the moment it stopped being one.

    Compiled rather than imported, like
    `test_the_partition_columns_match_the_gold_export` does: govern.py reaches a
    live target at import, and this is a pure function.
    """
    text = (ROOT / "steps" / "govern.py").read_text(encoding="utf-8")
    start = text.index("def _om_type")
    end = text.index("def _pbi_service")
    ns: dict = {}
    exec(compile(text[start:end], "govern._om_type", "exec"), ns)
    om = ns["_om_type"]

    assert om("decimal(19,4)") == "DECIMAL"
    assert om("decimal") == "DECIMAL"
    assert om("numeric(10,2)") == "DECIMAL"
    # The float family must still be DOUBLE — this is a split, not a rename.
    assert om("float") == "DOUBLE"
    assert om("double") == "DOUBLE"
    assert om("real") == "DOUBLE"
    # And the rest of the vocabulary is unmoved.
    assert om("bigint") == "INT"
    assert om("date") == "DATE"
    assert om("datetime2") == "TIMESTAMP"
    assert om("bit") == "BOOLEAN"
    # Both spellings of a Delta string land on STRING, so the emulator's
    # nvarchar -> varchar change does not move anything here.
    assert om("nvarchar") == "STRING"
    assert om("varchar") == "STRING"


def test_the_relationships_contract_rule_actually_checks_the_relationship():
    """A published contract must not claim more than its query checks.

    THIS RULE USED TO EMIT `select count(*) from {object} where <col> is null`
    while being named `<col>_resolves`, tagged dimension `consistency`, and
    described as "every <col> matches <to>.<field>". That is a not-null check.
    It passes with every foreign key dangling, so long as none of them is NULL.

    Most of this repository's near-misses have been a correct assertion beside
    prose that drifted. This was the inverse — the prose was right and the
    assertion was a no-op — and it is the worse direction, because a contract
    exists to tell a consumer they do NOT need their own check. Claiming a key
    resolves when only its presence was confirmed removes the reason to look
    without supplying the guarantee.

    Compiled rather than imported: govern.py reaches a live target at import.
    """
    import re as _re

    src = (ROOT / "steps" / "govern.py").read_text(encoding="utf-8")
    ns: dict = {"re": _re}
    for fn in ("_ref_target", "_relationship_rule"):
        start = src.index(f"def {fn}")
        end = src.index("def ", start + 10)
        exec(compile(src[start:end], fn, "exec"), ns)
    rule = ns["_relationship_rule"]

    r = rule("customer_id", {"to": "ref('dim_customer')", "field": "customer_id"})
    q = r["query"].lower()
    assert "join dim_customer" in q, q
    assert "r.customer_id is null" in q, q
    # The `is not null` guard keeps this about REFERENCES, not presence — a NULL
    # key belongs to the not_null rule, and counting it here would be the old
    # confusion running the other way.
    assert "t.customer_id is not null" in q, q
    assert r["dimension"] == "consistency"

    # A bare presence check must never again be published as `_resolves`.
    assert not _re.fullmatch(
        r"select count\(\*\) from \{object\} where \w+ is null", r["query"].strip()
    ), "the relationships rule has reverted to a not-null check"

    # UNRESOLVABLE TARGET: the rule may fall back, but it must stop claiming
    # referential integrity when it does — no `_resolves`, no `consistency`.
    f = rule("customer_id", {"to": "dim_customer", "field": "customer_id"})
    assert not f["name"].endswith("_resolves"), f["name"]
    assert f["dimension"] != "consistency", f
    assert "NOT asserted" in f["description"], f["description"]


def test_openmetadata_url_is_env():
    """OM_URL is the catalog address. Hardcoding localhost ships the
    emulator into production the same way a hardcoded vault URL would."""
    src = (ROOT / "steps" / "govern.py").read_text(encoding="utf-8")
    assert 'os.environ.get("OM_URL"' in src
    assert 'OM = "http://localhost:8585' not in src


def test_the_product_is_imported_not_restated():
    """The transforms and gold SQL come from the package, never from a copy.

    THIS RULE WAS UNENFORCED AND ALREADY BROKEN. RULES.md named
    `test_gold_sql_is_portable`, which asserts that gold SQL avoids T-SQL `bit`.
    True, useful, and about something else: it passed happily against a
    checked-in fork of all nine models. The fork drifted from the package and
    nothing noticed, which is the exact failure the product split exists to
    prevent.

    So this checks the thing the rule actually says.
    """
    for step, symbol in (("bronze", "run_bronze"), ("silver", "run_silver")):
        nb = (
            ROOT
            / "steps"
            / "definitions"
            / f"{'bronze-ingest' if step == 'bronze' else 'silver-conform'}.Notebook"
            / "notebook-content.py"
        ).read_text(encoding="utf-8")
        assert f"from contoso_product import {symbol}" in nb, (
            f"the {step} notebook does not import the product"
        )

    gold = (ROOT / "steps" / "gold.py").read_text(encoding="utf-8")
    assert "from contoso_product import gold_dir" in gold, (
        "gold.py does not take its dbt project from the product"
    )

    # And no copy may come back. `gold/models` is staged from the package on
    # every run and gitignored; a tracked one is the fork returning.
    tracked = subprocess.run(
        ["git", "ls-files", "gold/models", "gold/macros", "gold/tests"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout.split()
    assert not tracked, f"the gold fork is tracked again: {tracked}"


def test_the_domain_is_core_s_name_not_restated_here():
    """A platform consumes what core names. It does not declare its own.

    This file declared `DOMAIN = "contoso-sales"` while contoso-data-product
    already named the same domain `contoso-commerce`, and the Databricks
    platform imported that one. So the two runtimes published ONE product into
    TWO domains, described two ways — the disagreement a shared catalog exists
    to remove, reproduced inside it.

    Nothing detected it because each platform was self-consistent and neither
    read the other, which is exactly why this is a test rather than a comment.
    RULES.md §1 already said it; the rule needed teeth.
    """
    import ast

    src = (ROOT / "steps" / "govern.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "contoso_product.contracts"
        for alias in node.names
    }
    assert "DOMAIN" in imported, (
        "govern.py must import DOMAIN from contoso_product.contracts, not name "
        "the domain itself"
    )

    assigned = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert "DOMAIN" not in assigned, (
        "govern.py assigns DOMAIN locally — that is how one product came to "
        "live in two domains"
    )


def test_gold_sql_is_portable():
    """T-SQL BIT must not appear in gold models. The flag() macro is the
    dialect adapter; restating `cast(0 as bit)` is how the Databricks
    consumer would need a second copy of the product."""
    sales = (gold_root() / "models" / "fct_sales.sql").read_text(encoding="utf-8")
    assert "as bit" not in sales.lower()
    assert "{{ flag(" in sales
    assert (gold_root() / "macros" / "flag.sql").is_file()


def test_the_product_does_not_supply_its_own_engine():
    """The notebooks run on the platform's engine, not one this product starts.

    A product that stood up its own Spark would be answering a question the
    platform exists to answer, and the two would drift. The other half of this
    check -- that the platform's compose actually provides a spark-agent -- is
    the platform's to make, and lives there: neither repository can assert both
    halves now that they are separate, which is the honest consequence of the
    split rather than a gap in it.
    """
    assert not (ROOT / "steps" / "engine.py").exists(), (
        "steps/engine.py is back — the platform supplies the engine"
    )


@pytest.mark.fixtures
def test_the_partition_columns_match_the_gold_export():
    """A partition must name columns the warehouse really has.

    `export_gold.py` selects snake_case from gold and renames to PascalCase for
    the model; `semantic_model.gold_column` reverses that so a partition's SQL
    names real columns. Two encodings of one mapping is exactly the shape that
    drifts — and this drift is SILENT, because a partition naming a column that
    does not exist fails only when something refreshes the model, which nothing
    in this platform does today. Power BI Desktop would be the first to notice,
    on somebody else's machine.

    So the derivation is checked against the SELECTs the exporter really issues,
    parsed out of its source rather than restated here.
    """
    export = ROOT / "gold" / "tools" / "export_gold.py"
    if not export.exists():
        candidates = list(ROOT.rglob("export_gold.py"))
        assert candidates, "export_gold.py not found — the mapping has no authority"
        export = candidates[0]
    src = export.read_text(encoding="utf-8")

    # Only the pure function is wanted; importing the module would drag in the
    # whole platform (state, fabric, a live target). So the function is located by
    # PARSING rather than by slicing between two string landmarks: this read
    # `text.index("def partition")` and broke the moment that function was deleted
    # in the Direct Lake conversion — failing with `ValueError: substring not
    # found`, which says nothing about the mapping it exists to check.
    import ast as _ast

    text = (ROOT / "steps" / "semantic_model.py").read_text(encoding="utf-8")
    tree = _ast.parse(text)
    fn = next(
        (
            n
            for n in tree.body
            if isinstance(n, _ast.FunctionDef) and n.name == "gold_column"
        ),
        None,
    )
    assert fn is not None, "gold_column is gone — the mapping has no derivation"
    segment = _ast.get_source_segment(text, fn)
    assert segment, "gold_column's source could not be extracted"
    ns: dict = {}
    exec(compile(segment, "semantic_model.py", "exec"), ns)
    gold_column = ns["gold_column"]

    # Every `SELECT a, b, c FROM <table>` the exporter issues.
    selects = re.findall(r'"SELECT ([^"]+?) "?\s*"?FROM (\w+)"', src)
    assert selects, f"no SELECTs parsed out of {export.name}"

    for cols, table in selects:
        warehouse_cols = [c.strip() for c in cols.split(",") if c.strip()]
        for wc in warehouse_cols:
            # Round-trip: PascalCase the warehouse column the way the exporter
            # does, then derive it back and require the original.
            model_col = "".join(p.capitalize() for p in wc.split("_"))
            assert gold_column(model_col) == wc, (
                f"{table}.{wc} -> model {model_col} -> {gold_column(model_col)}; "
                f"the Direct Lake partition's sourceColumn would name a column "
                f"gold does not have"
            )


def test_tls_verification_is_never_hardcoded_off():
    """The real target must not be able to run with verification disabled."""
    real = real_branch()
    assert "verify_tls=True" in real, "the real target must verify TLS"
    assert "allow_invalid" not in real


def test_the_real_target_never_creates_a_capacity():
    """A pipeline run must not be able to provision billable Azure resources.

    A `Microsoft.Fabric/capacities` resource costs money for as long as it
    exists. The emulator may create one freely, and does, so that the local run
    exercises the real ordering — ARM first, then assign — rather than leaning
    on the seeded capacity the emulator attaches for convenience. Against real
    Fabric the same code must resolve a capacity an operator already made.

    Read as text for the same reason as the TLS check above: the guarantee is
    that no configuration can reach the creating branch, which is a property of
    the literal, not of a value some environment might produce.
    """
    real = real_branch()
    assert "capacity_arm=None" in real, (
        "the real target must declare capacity_arm=None — anything else lets a "
        "pipeline run create billable infrastructure"
    )
    assert "ArmCapacity(" not in real, (
        "the real target must not construct an ArmCapacity: that is the object "
        "that says where a capacity may be created"
    )


def test_the_real_target_reads_its_capacity_name_from_configuration():
    """Real mode takes the capacity name from FABRIC_CAPACITY, never a literal.

    Optional on purpose. It is consulted only when a workspace has to be
    CREATED; an existing one already carries its capacity and the platform
    adopts it. Requiring it unconditionally was worse than redundant, because
    a value that disagreed with a live workspace got "corrected" by moving the
    workspace.
    """
    real = real_branch()
    assert "FABRIC_CAPACITY" in real, (
        "real mode must read the capacity name from FABRIC_CAPACITY"
    )
