"""Pull Contoso Web over HTTP and land it verbatim in OneLake.

THE SECOND VENDOR, and the reason there is one. A platform that ingests a
single source proves it can ingest a single source. Two vendors is where the
work actually starts: two credentials that rotate separately, two formats that
agree about nothing, and two customer lists that describe overlapping people
without either system knowing the other exists.

WHAT THIS VENDOR SENDS, and none of it is smoothed over here:

  * JSON arrays, not the delimited text and JSON Lines the POS system ships
  * ORDERS ARE NESTED — one order carries its own `lines` array, because the
    storefront thinks in baskets. Flattening is a decision, and it belongs
    downstream where it is visible, not in the step that records what arrived
  * NO CUSTOMER ID — accounts are keyed on email, which is what makes joining
    this to the POS system a resolution problem rather than a join
  * `country` as the shopper typed it — "United States", not "US"

Landed VERBATIM, exactly as `ingest_pos` does. Bronze's job is to be the bytes
as they arrived, so a question about the vendor can be answered without going
back to the vendor.
"""

from __future__ import annotations

import datetime as dt

import connections
import requests
import state
import vault
from fabric import FABRIC_AUD, STORAGE_AUD, log, token, upload
from sources import WEB_API, WEB_KEY_SECRET

# (operation path, landed subdirectory, part extension). Named from the
# OpenAPI spec's operations, so a spec change that renames a route fails here
# rather than landing an empty file that only bronze will notice.
FEEDS = [
    ("/api/v2/export/customers", "customers", "json"),
    ("/api/v2/export/products", "products", "json"),
    ("/api/v2/export/orders", "orders", "json"),
]


def fetch(path: str, key: str, page: int | None = None) -> requests.Response:
    params = {} if page is None else {"page": page}
    return requests.get(
        f"{WEB_API}{path}", headers={"X-Api-Key": key}, params=params, timeout=600
    )


def main() -> int:
    st = state.load()
    tok = token(STORAGE_AUD)
    day = st.get("landing_day") or dt.date.today().isoformat()

    # This vendor's own key, from this vendor's own secret. Using the POS key
    # here would still land bytes — mokapi-pos and mokapi-web are separate
    # processes with separate keys — but it would prove nothing about either.
    api_key = vault.get(WEB_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key", 1)
    assert refused.status_code == 401, (
        f"Contoso Web accepted a bad API key: {refused.status_code}"
    )

    landed = {}
    for path, subdir, ext in FEEDS:
        # Page 1 first, because the vendor reports the total in its response.
        first = fetch(path, api_key, 1)
        assert first.status_code == 200, (path, first.status_code, first.text[:200])
        total_pages = int(first.headers["X-Total-Pages"])
        assert total_pages >= 1, (path, total_pages)

        written_total, parts = 0, 0
        for page in range(1, total_pages + 1):
            r = first if page == 1 else fetch(path, api_key, page)
            assert r.status_code == 200, (path, page, r.status_code, r.text[:200])
            # The vendor says which page this is. Checking it catches a server
            # that ignores the parameter and returns page 1 every time — which
            # would land the right byte count and the wrong data.
            assert int(r.headers["X-Page"]) == page, (r.headers.get("X-Page"), page)
            blob = r.content
            assert blob, f"{path} page {page} returned an empty body"
            # Each page must be a COMPLETE array, not a fragment. A vendor that
            # split on bytes would hand back something no reader could parse
            # alone, and the failure would surface in bronze as a Spark error
            # naming neither the vendor nor the page.
            assert blob[:1] == b"[" and blob[-1:] == b"]", (
                f"{path} page {page} is not a self-contained JSON array"
            )
            # ONE LINE PER PAGE, which is a constraint the ENGINE imposes rather
            # than a rule the vendor agreed to. Its JSON reader is NDJSON-only
            # and ignores both `multiLine` and `wholetext`, so bronze parses
            # these pages as text — one row per line. A pretty-printed page would
            # be split across rows, each fragment would fail to parse, and the
            # column would arrive full of NULLs. Caught here, the message names
            # the cause; caught in bronze, it looks like a Spark problem.
            assert b"\n" not in blob.strip(), (
                f"{path} page {page} contains newlines — bronze reads these "
                f"pages a line at a time, so a pretty-printed page parses to "
                f"NULLs rather than failing"
            )

            dest = f"Files/landing/contoso_web/{day}/{subdir}/part-{page:04d}.{ext}"
            written = upload(st["workspace"], st["lakehouse"], dest, blob, tok)
            assert written == len(blob), (written, len(blob))
            written_total += written
            parts += 1

        # One past the end must be refused. Without this a vendor that answered
        # every page number would look identical to one that paged correctly.
        over = fetch(path, api_key, total_pages + 1)
        assert over.status_code == 404, (
            f"{path} served page {total_pages + 1} of {total_pages}: {over.status_code}"
        )

        landed[subdir] = {"bytes": written_total, "parts": parts}
        log(f"landed {subdir}/ — {parts} part(s), {written_total:,} bytes")

    # NAME THE VENDOR, as ingest_pos does. Without this the graph would start
    # at the landed files and Contoso Web — a different company, reached over
    # HTTP against its own key — would appear nowhere.
    ftok = token(FABRIC_AUD)
    web = connections.ensure(
        ftok,
        "Contoso Web",
        "ShareableCloud",
        connections.details("Web", "Web", url=WEB_API),
    )
    connections.announce(
        ftok,
        st["workspace"],
        "ingest_web",
        "Contoso Web",
        connections.from_source(
            web,
            st["lakehouse"],
            # The paths bronze reads, date partition included.
            [f"Files/landing/contoso_web/{day}/{subdir}" for subdir in landed],
        ),
    )

    state.save(landing_day=day, web_landed=landed, web_connection=web)
    total = sum(v["bytes"] for v in landed.values())
    n_parts = sum(v["parts"] for v in landed.values())
    log(f"Contoso Web: {n_parts} part(s) across {len(landed)} feed(s), {total:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
