"""Run the same DAX query through a real BI client, over XMLA.

`semantic_model.py` proves the model answers over the Power BI REST wire. This
proves — or reports precisely why it cannot — that a **BI tool** can reach it:
Power BI Desktop, DAX Studio and Tabular Editor all connect through XMLA using
Microsoft's ADOMD.NET client and a token in the connection string, which is
exactly what the probe does.

THE SAME QUERY, DELIBERATELY. If XMLA answers, its total must equal the total
REST returned. Two independent surfaces agreeing on one number is a much
stronger statement than either alone, and it is free once the query is shared.

TODAY THERE IS NO XMLA SURFACE. The emulator defers it on cost (docs/24), so
the expected outcome is a refused connection — and that is asserted as a KNOWN
state rather than skipped. Anything else means something is listening and
behaving unexpectedly, which fails. The day an XMLA surface ships, this step
starts cross-checking the number without anyone remembering to enable it.
"""

from __future__ import annotations

import pathlib
import subprocess
import tempfile

import state
from fabric import T, log, server_cert_pem, token
from semantic_model import DAX, PBI_AUD

ROOT = pathlib.Path(__file__).resolve().parent.parent
IMAGE = "fabric-platform-notebook-pipelines-xmla"

RAN, NO_SURFACE = 0, 3


def build() -> None:
    subprocess.run(
        ["docker", "build", "-q", "-f", "docker/xmla/Dockerfile", "-t", IMAGE, "."],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )


def main() -> int:
    import source_system as src

    st = state.load()
    dataset = st.get("dataset")
    assert dataset, "no semantic model published — run semantic_model first"

    # The XMLA endpoint is the Fabric host without its scheme; ADOMD builds
    # `powerbi://host:port/v1.0/myorg/<workspace>` itself.
    host = T.api_root.split("://", 1)[1]

    log("querying the semantic model with ADOMD.NET (a real BI client)")
    build()
    # -e per variable: `docker run` does not inherit the caller's environment,
    # so setting it on the CLI process would leave the container with none —
    # which the probe reports as an argument error rather than as "no surface",
    # correctly refusing to confuse a harness bug with a platform state.
    settings = {
        "XMLA_TARGET": host,
        "XMLA_TOKEN": token(PBI_AUD),
        "XMLA_WORKSPACE": st["workspace"],
        "XMLA_DATASET": dataset,
        "XMLA_QUERY": DAX,
    }
    passthrough = [x for k, v in settings.items() for x in ("-e", f"{k}={v}")]

    # Hand the client the certificate the stack is actually presenting. A BI
    # tool on a laptop would trust it the same way; against real Fabric there is
    # nothing to mount, because the chain already validates.
    with tempfile.TemporaryDirectory() as certdir:
        mounts = []
        if not T.verify_tls:
            pathlib.Path(certdir, "emulator.crt").write_text(
                server_cert_pem(T.api_root), encoding="utf-8"
            )
            mounts = ["-v", f"{certdir}:/certs:ro"]
        r = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "host",
                *mounts,
                *passthrough,
                IMAGE,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
    out = (r.stdout + r.stderr).strip()

    if r.returncode == RAN:
        total = float(
            next(x for x in out.splitlines() if x.startswith("RESULT")).split()[1]
        )
        # Two surfaces, one number. A disagreement means the model serves
        # different answers depending on how it is asked, which is worse than
        # either being wrong.
        assert abs(total - src.EXPECTED_REVENUE) < 0.01, (total, src.EXPECTED_REVENUE)
        state.save(xmla="answered")
        log(f"XMLA: a real BI client got {total:,.2f} — agrees with REST")
        return 0

    if r.returncode == NO_SURFACE:
        # Reported, never silently skipped: this is a named, expected gap with
        # a date attached to it, not an absence of information.
        state.save(xmla="no-surface")
        detail = next((x for x in out.splitlines() if x.startswith("FAILED")), out)
        log(f"XMLA: no endpoint at {host} — expected today (deferred in docs/24)")
        log(f"      {detail}")
        return 0

    raise SystemExit(
        f"the XMLA probe failed in an unexpected way (exit {r.returncode}).\n"
        f"Something is listening and did not behave like no-surface:\n{out}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
