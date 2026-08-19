"""Publish a notebook definition, run it, and wait for a real verdict.

WHY THIS IS SHARED RATHER THAN COPIED. Two steps now hand code to Fabric instead
of doing the work themselves — bronze and silver — and the operator half of that
is identical for both: substitute the parameters cell, map the definition
directory to parts, submit a RunNotebook job, poll to a terminal state, read the
run detail. Only the transform and the grading differ, and those are the parts
that should differ.

WHAT A CALLER STILL OWNS: the placeholders it substitutes, and every assertion
about what came back. This module deliberately makes no claim about the numbers.

THE STATUS IS EVIDENCE, not decoration. A RunNotebook job whose cells are still
outstanding has no completion time at all, so reaching a terminal state here
means an engine really executed and reported. It did not always: the job used to
complete on a clock, reading `Completed` with every cell Pending and no engine
having run a line.
"""

from __future__ import annotations

import base64
import pathlib
import time

import provision
from fabric import await_operation, fabric, log

DEFINITIONS = pathlib.Path(__file__).resolve().parent / "definitions"

# Long enough for a cold Spark session on a laptop, and bounded so a wedged
# engine fails the step rather than hanging the pipeline.
POLL_SECONDS = 180
TERMINAL = ("Completed", "Failed", "Cancelled", "Deduped")


def definition_dir(notebook: str) -> pathlib.Path:
    """The `{display name}.Notebook/` directory holding this notebook's parts."""
    d = DEFINITIONS / f"{notebook}.Notebook"
    if not d.is_dir():
        raise SystemExit(f"no definition directory for {notebook}: {d}")
    return d


def content(notebook: str, **subs: str) -> bytes:
    """The notebook's bytes with its placeholders filled in.

    Real Fabric would pass ids and the landing day through the job's
    `executionData.parameters` and leave the file untouched. The emulator
    implements no parameter override, so they are substituted before publishing
    — the one place this platform edits code rather than configuring it, and the
    reason the placeholders are shaped (`@@NAME@@`) so that an unsubstituted
    notebook cannot silently resolve to somewhere real.
    """
    src = (definition_dir(notebook) / "notebook-content.py").read_text(encoding="utf-8")
    for name, value in subs.items():
        src = src.replace(f"@@{name}@@", value)
    # Fails here rather than in the engine. A surviving placeholder would either
    # crash mid-run with a path nobody recognises, or — worse for a reader —
    # resolve to a literal directory name and write real rows somewhere wrong.
    assert "@@" not in src, (
        f"{notebook}: a placeholder survived substitution — passed {sorted(subs)}"
    )
    return src.encode()


def _parts(notebook: str, body: bytes) -> list[dict]:
    """Every file in the definition directory, keyed by its path relative to it.

    The same mapping Git integration uses, which is what makes the committed
    directory and the published item the same thing. The notebook body is passed
    in because its parameters cell is substituted first; everything else
    (`.platform`, carrying the logicalId) ships as committed.
    """
    parts = [
        {
            "path": "notebook-content.py",
            "payload": base64.b64encode(body).decode(),
            "payloadType": "InlineBase64",
        }
    ]
    for extra in sorted(definition_dir(notebook).iterdir()):
        if extra.name == "notebook-content.py" or not extra.is_file():
            continue
        parts.append(
            {
                "path": extra.name,
                "payload": base64.b64encode(extra.read_bytes()).decode(),
                "payloadType": "InlineBase64",
            }
        )
    return parts


def publish(tok: str, workspace: str, notebook: str, body: bytes) -> str:
    """Create the Notebook item, or update the definition of the existing one.

    Resolve-or-create by NAME, like every other item in this platform: ids
    cannot match across targets, and a step that only works on a fresh
    workspace is not one anybody can operate.
    """
    definition = {"parts": _parts(notebook, body)}

    found = provision.find_item(tok, workspace, notebook, "Notebook")
    if found:
        r = fabric(
            "POST",
            f"/workspaces/{workspace}/items/{found['id']}/updateDefinition",
            tok,
            json={"definition": definition},
        )
        await_operation(r, tok, "updateDefinition")
        log(f"updated notebook {notebook}")
        return found["id"]

    r = fabric(
        "POST",
        f"/workspaces/{workspace}/items",
        tok,
        json={"displayName": notebook, "type": "Notebook", "definition": definition},
    )
    created = await_operation(r, tok, "create notebook")
    assert created.get("id"), created
    log(f"created notebook {notebook}")
    return created["id"]


def submit(tok: str, workspace: str, item: str) -> str:
    """Start a RunNotebook job and return its instance id."""
    r = fabric(
        "POST",
        f"/workspaces/{workspace}/items/{item}/jobs/instances?jobType=RunNotebook",
        tok,
    )
    assert r.status_code in (200, 202), (r.status_code, r.text[:300])
    job = r.headers["Location"].rstrip("/").rsplit("/", 1)[-1]
    log(f"submitted RunNotebook job {job}")
    return job


def await_job(tok: str, workspace: str, item: str, job: str) -> dict:
    """Poll the job to a terminal state and return the notebook run detail."""
    base = f"/workspaces/{workspace}/items/{item}/jobs/instances/{job}"
    for _ in range(POLL_SECONDS):
        r = fabric("GET", base, tok)
        assert r.status_code == 200, (r.status_code, r.text[:200])
        status = r.json().get("status")
        if status in TERMINAL:
            assert status == "Completed", r.json()
            detail = fabric("GET", f"{base}/notebookRun", tok)
            assert detail.status_code == 200, (detail.status_code, detail.text[:200])
            return detail.json()
        time.sleep(1)
    raise SystemExit("the RunNotebook job never reached a terminal state")
