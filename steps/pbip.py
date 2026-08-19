"""Write the semantic model to disk as a Power BI Project (PBIP).

WHY. A published model lives in a service; a PBIP is the same model as files
somebody can open, diff and commit. It is the format Fabric git integration
writes and Power BI Desktop opens, and it is the only artifact this platform
can hand a BI developer that is neither a screenshot nor an API call.

SEMANTIC-MODEL ONLY, and that is a real PBIP rather than a cut-down one:
Microsoft's own FAQ says the `.pbip` file "is optional and simply serves as a
shortcut to the report folder", and a model-without-report folder is exactly
what git integration produces for a workspace that has no report. Nothing here
authors visuals, so nothing here writes a report folder.

WHAT IS DELIBERATELY ABSENT. `.pbi/cache.abf` — the local data cache — cannot
be produced without writing VertiPaq, and Desktop opens a project without one:
the model arrives with its full definition and no rows, then refreshes through
the partitions. That is why the partitions in semantic_model.py had to come
first; without them this would emit a model that opens to empty tables and says
nothing about why.
"""

from __future__ import annotations

import json
import pathlib

MODEL = "ContosoRevenue"

# The git-integration system file. Its logicalId is what lets Fabric recognise
# a redeployed item as the SAME item rather than a new one, so it is stable and
# not regenerated per run.
PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
    "platformProperties/2.0.0/schema.json"
)
LOGICAL_ID = "0f1c4b8e-2a6d-4e3f-9b7a-5c8d1e2f3a4b"

# version 1.0 pins the TMSL format — the model as one model.bim. 4.0+ would
# also permit a TMDL folder, which this platform does not produce: claiming a
# version whose other format we cannot write would be a lie a tool could catch.
PBISM = {
    "$schema": (
        "https://developer.microsoft.com/json-schemas/fabric/item/"
        "semanticModel/definitionProperties/1.0.0/schema.json"
    ),
    "version": "1.0",
    "settings": {},
}


def write(out_dir: pathlib.Path, model_bim: dict) -> pathlib.Path:
    """Write `<out>/<name>.SemanticModel/` and return the folder.

    `model_bim` is passed in rather than rebuilt so the folder on disk carries
    the SAME definition that was published. A second construction here could
    drift from the service by a partition or a measure, and the whole value of
    the artifact is that it describes what is actually deployed.
    """
    folder = out_dir / f"{MODEL}.SemanticModel"
    folder.mkdir(parents=True, exist_ok=True)

    (folder / ".platform").write_text(
        json.dumps(
            {
                "$schema": PLATFORM_SCHEMA,
                "metadata": {"type": "SemanticModel", "displayName": MODEL},
                "config": {"version": "2.0", "logicalId": LOGICAL_ID},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (folder / "definition.pbism").write_text(
        json.dumps(PBISM, indent=2) + "\n", encoding="utf-8"
    )
    (folder / "model.bim").write_text(
        json.dumps(model_bim, indent=2) + "\n", encoding="utf-8"
    )
    return folder
