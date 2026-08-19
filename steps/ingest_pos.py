"""Pull Contoso POS over HTTP and land it verbatim in OneLake.

The difference between this platform and the emulator's own examples starts
here: they call the generator in-process, this fetches from a REST API the
vendor publishes an OpenAPI spec for. That spec is what makes Contoso POS a
node in the lineage graph rather than a filename in `Files/landing/`.

Landed VERBATIM — no parsing, no reshaping. Bronze's job is to be the bytes as
they arrived, so that a question about the source can be answered without going
back to the vendor.

PAGED, and the pages are landed as separate parts rather than stitched back
into one file. Reassembling here would put the whole export in this process's
memory — the exact thing paging removes — and a directory of parts is what an
engine wants to read anyway. Spark treats the directory as one dataset.
"""

from __future__ import annotations

import datetime as dt

import connections
import requests
import state
import vault
from fabric import FABRIC_AUD, STORAGE_AUD, log, token, upload
from sources import POS_API, POS_KEY_SECRET

# (operation path, landed subdirectory, part extension). Named from the OpenAPI
# spec's operations, so a spec change that renames a route fails here rather
# than landing an empty file that only bronze will notice.
FEEDS = [
    ("/api/v1/export/customers", "customers", "csv"),
    ("/api/v1/export/orders", "orders", "jsonl"),
]


def fetch(path: str, key: str, page: int | None = None) -> requests.Response:
    params = {} if page is None else {"page": page}
    return requests.get(
        f"{POS_API}{path}", headers={"X-Api-Key": key}, params=params, timeout=600
    )


def main() -> int:
    st = state.load()
    tok = token(STORAGE_AUD)
    day = dt.date.today().isoformat()

    # The credential is enforced by the vendor, not by us. The emulator's own
    # example asserts a wrong key raises PermissionError in-process; over HTTP
    # the same guarantee is a 401, which is what a real client would meet.
    api_key = vault.get(POS_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key", 1)
    assert refused.status_code == 401, (
        f"the vendor accepted a bad API key: {refused.status_code}"
    )

    landed = {}
    for path, subdir, ext in FEEDS:
        # Page 1 first, because the vendor reports the total in its response.
        # Asking an index endpoint how many pages there are, then trusting it,
        # would be a second source of truth for something every response
        # already carries.
        first = fetch(path, api_key, 1)
        assert first.status_code == 200, (path, first.status_code, first.text[:200])
        total_pages = int(first.headers["X-Total-Pages"])
        assert total_pages >= 1, (path, total_pages)

        written_total, parts = 0, 0
        for page in range(1, total_pages + 1):
            r = first if page == 1 else fetch(path, api_key, page)
            assert r.status_code == 200, (path, page, r.status_code, r.text[:200])
            # The vendor says which page this is. Checking it is what catches a
            # server that ignores the parameter and returns page 1 every time —
            # which would land the right byte count and the wrong data.
            assert int(r.headers["X-Page"]) == page, (r.headers.get("X-Page"), page)
            blob = r.content
            assert blob, f"{path} page {page} returned an empty body"

            dest = f"Files/landing/contoso_pos/{day}/{subdir}/part-{page:04d}.{ext}"
            written = upload(st["workspace"], st["lakehouse"], dest, blob, tok)
            assert written == len(blob), (written, len(blob))
            written_total += written
            parts += 1

        # One past the end must be refused. Without this a vendor that answered
        # every page number would look identical to one that paged correctly,
        # and the loop above would have no way to know it had stopped early.
        over = fetch(path, api_key, total_pages + 1)
        assert over.status_code == 404, (
            f"{path} served page {total_pages + 1} of {total_pages}: {over.status_code}"
        )

        landed[subdir] = {"bytes": written_total, "parts": parts}
        log(f"landed {subdir}/ — {parts} part(s), {written_total:,} bytes")

    # NAME THE VENDOR. Everything above landed bytes into OneLake; without this
    # the graph would start at those files and Contoso POS — the system that
    # actually produced them, over HTTP, against a key from Key Vault — would
    # appear nowhere. The connection is what the fetches above authenticated
    # through, so it is the honest identity for the source end of the edge.
    ftok = token(FABRIC_AUD)
    pos = connections.ensure(
        ftok,
        "Contoso POS",
        "ShareableCloud",
        connections.details("Web", "Web", url=POS_API),
    )
    # One move per feed: the customers export did not produce the orders file.
    connections.announce(
        ftok,
        st["workspace"],
        "ingest_pos",
        "Contoso POS",
        connections.from_source(
            pos,
            st["lakehouse"],
            # The paths bronze actually reads, date partition included. A
            # target that merely looks right joins the vendor to a node no
            # other edge mentions, and the graph gains a source system floating
            # beside the medallion instead of feeding it.
            [f"Files/landing/contoso_pos/{day}/{subdir}" for subdir in landed],
        ),
    )

    state.save(landing_day=day, pos_landed=landed, pos_connection=pos)
    total = sum(v["bytes"] for v in landed.values())
    n_parts = sum(v["parts"] for v in landed.values())
    log(f"Contoso POS: {n_parts} part(s) across {len(landed)} feed(s), {total:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
