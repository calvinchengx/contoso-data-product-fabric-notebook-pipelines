"""The progress line every step prints. No dependencies, deliberately.

`log` lived in `fabric.py`, which resolves a target and so needs the published
`fabric-target` wheel. That put a two-line print helper behind the one import
in this repository that `make test` cannot satisfy: the wheel is installed by
`make fixtures` from a pinned release and is deliberately outside `uv.lock`,
because which release it came from is the thing under test.

Anything that only wants to SAY something then had to buy the whole client.
`reconcile.compare` is the case that proved it — a pure comparison over two
dicts of decimals, untestable on a clean checkout because it logged a line per
row. Same lesson as `apipath.py`: code with no dependencies should not live
behind any.

`fabric` re-exports this name, so the nineteen modules that do
`from fabric import log` keep working unchanged.
"""

from __future__ import annotations


def log(msg: str) -> None:
    print(f"==> {msg}", flush=True)
