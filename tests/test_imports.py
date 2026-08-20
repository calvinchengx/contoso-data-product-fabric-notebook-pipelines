"""Every `from <step> import <name>` names something that exists.

WHY THIS TEST EXISTS. `bronze.py` shipped `from target import T`. `target.py` has
never bound `T` — it lives in `fabric.py`, which is where every other step imports
it from — so the module raised `ImportError` the moment anything loaded it, and
`make verify` was broken on main from the commit that introduced it.

**Three green checks passed that PR.** All three were `make targets work`, and none
of them loads a platform module: `acceptance.yml`, which actually runs the steps,
fires on `repository_dispatch` and `schedule` and never on `pull_request`. ruff
does not resolve cross-module names, and ty cannot: this repository sets
`unresolved-import = "ignore"` because the generator wheels arrive from a pinned
release and are absent on a clean checkout, which switches off the one rule that
would have said so. So a name that does not exist reached main behind a green
rollup — the same shape as a job reporting success having run nothing.

STATIC, NOT AN IMPORT. `make test` promises no emulator, no Docker and no fixture
wheels, and importing these modules breaks all three: `fabric` resolves a target
at import time and several steps import generators that only exist once
`make fixtures` has run. Parsing costs none of that and catches this whole class —
a missing name is missing whether or not the wheel is installed.
"""

from __future__ import annotations

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
PLATFORM = ROOT / "steps"


def module_paths() -> list[pathlib.Path]:
    return sorted(p for p in PLATFORM.glob("*.py") if not p.name.startswith("_"))


def bound_names(tree: ast.Module) -> set[str]:
    """Every name a module binds at module level.

    Includes the bodies of top-level `if` and `try`, because a conditional import
    or a platform guard still binds the name for an importer. Anything inside a
    function or class does not, which is the distinction that makes this useful.
    """
    names: set[str] = set()

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                for t in node.targets:
                    for n in ast.walk(t):
                        if isinstance(n, ast.Name):
                            names.add(n.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names.add(node.target.id)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for a in node.names:
                    names.add(a.asname or a.name.split(".")[0])
            elif isinstance(node, ast.If):
                visit(node.body)
                visit(node.orelse)
            elif isinstance(node, ast.Try):
                visit(node.body)
                for h in node.handlers:
                    visit(h.body)
                visit(node.orelse)
                visit(node.finalbody)

    visit(tree.body)
    return names


def test_every_cross_module_import_resolves():
    local = {p.stem: ast.parse(p.read_text(encoding="utf-8")) for p in module_paths()}
    exported = {stem: bound_names(tree) for stem, tree in local.items()}

    missing: list[str] = []
    for stem, tree in local.items():
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.level:
                continue
            target = (node.module or "").split(".")[0]
            if target not in exported:
                continue  # third-party or a generator from the wheel
            for alias in node.names:
                if alias.name == "*":
                    continue
                if alias.name not in exported[target]:
                    missing.append(
                        f"{stem}.py:{node.lineno} imports {alias.name!r} from "
                        f"{target}.py, which does not bind it"
                    )

    assert not missing, "\n".join(
        ["cross-module imports naming something that does not exist:", *missing]
    )


def test_the_readme_inventory_matches_the_pinned_core():
    """The README's product list must be what this leaf's pin actually contains.

    A generated list that falls behind is worse than none: a reader trusts it
    BECAUSE it looks generated. The check lives in the core, so all seven leaves
    ask the same question of their own pin, and it fails here, in the repository
    that has to fix it.

    Regenerate with:  python -m contoso_product.show --markdown
    """
    from pathlib import Path

    from contoso_product import show

    ok, message = show.check(Path(__file__).resolve().parent.parent / "README.md")
    assert ok, message
