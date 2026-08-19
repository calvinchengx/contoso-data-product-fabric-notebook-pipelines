"""Pull Contoso Reference over HTTP and land it verbatim in OneLake.

THE FOURTH VENDOR, and the first that is not an operational system. POS, Web
and ERP each record things that happened; this one publishes the definitions
they are all reported against. It is a vendor rather than a table maintained
inside the platform because that is what it is in the business: the group data
office owns it, issues a credential for it, and changes it on its own schedule.

WHAT THIS VENDOR SENDS, none of it smoothed over here:

  * PARQUET — a binary columnar file, where POS ships delimited text and JSON
    Lines and Web ships JSON arrays. Three vendors, three dialects.
  * NOT PAGED, because the whole export is about four kilobytes. The other two
    page to keep a request's cost bounded; doing it here would be decoration,
    and a Parquet file cannot be split on line boundaries anyway.
  * FX RATES WITH GAPS. Rates are published for trading days only, so weekends
    are absent. That is the vendor reporting what was published; carrying the
    last rate forward is the consumer's decision and is made downstream, where
    it is visible.

WHY THIS STEP VERIFIES A CHECKSUM WHEN NO OTHER INGEST DOES. Every other feed
is text, so damage in transit announces itself — a truncated CSV or a mangled
JSON array fails to parse. Parquet does not: it keeps its `PAR1` magic and its
`PAR1` footer through byte-level corruption, so a ruined file still passes
every cheap check and fails much later, deep inside a Parquet reader, with a
message naming neither the transport nor the cause.

That is not hypothetical here. mokapi's ordinary response path cannot carry
binary at all: `read()` returns a Go string, goja decodes it as UTF-8, and every
byte that is not valid UTF-8 becomes U+FFFD. Measured against these exact files,
that inflates fx_rates.parquet from 2,268 bytes to 3,301 — a 46% change with
both `PAR1` markers still in place. serve.js avoids it by putting raw bytes on
`response.data`, and this step verifies the digest so that if it ever silently
reverts, the failure lands here, at the boundary, naming the cause.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import connections
import requests
import state
import vault
from fabric import FABRIC_AUD, STORAGE_AUD, log, token, upload
from sources import REFERENCE_API, REFERENCE_KEY_SECRET

# (operation path, landed filename). Named from the OpenAPI spec's operations,
# so a spec change that renames a route fails here rather than landing nothing.
FEEDS = [
    ("/reference/v1/product-hierarchy", "product_hierarchy.parquet"),
    ("/reference/v1/fx-rates", "fx_rates.parquet"),
]


def fetch(path: str, key: str) -> requests.Response:
    return requests.get(
        f"{REFERENCE_API}{path}", headers={"X-Api-Key": key}, timeout=600
    )


def main() -> int:
    import reference_data as ref

    st = state.load()
    tok = token(STORAGE_AUD)
    day = st.get("landing_day") or dt.date.today().isoformat()

    # This vendor's own key, from this vendor's own secret — as with the other
    # two. A key that works across vendors would prove nothing about either.
    api_key = vault.get(REFERENCE_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key")
    assert refused.status_code == 401, (
        f"Contoso Reference accepted a bad API key: {refused.status_code}"
    )

    landed: dict[str, dict[str, str]] = {}
    total = 0
    for path, filename in FEEDS:
        r = fetch(path, api_key)
        assert r.status_code == 200, (path, r.status_code, r.text[:200])
        blob = r.content
        assert blob, f"{path} returned an empty body"

        # NECESSARY AND NOT SUFFICIENT, which is exactly why the checksum below
        # exists. Both markers survive the corruption this step guards against,
        # so passing this pair proves only that something Parquet-shaped
        # arrived — it is worth asserting to catch a 200 carrying an HTML error
        # page, and worth nothing at all against a mangled body.
        assert blob[:4] == b"PAR1" and blob[-4:] == b"PAR1", (
            f"{path} is not a Parquet file: starts {blob[:4]!r}, ends {blob[-4:]!r}"
        )

        # THE REAL CHECK. The vendor publishes the digest of what it sent; if
        # what arrived hashes differently, the transport changed the bytes.
        published = r.headers.get("X-Content-SHA256", "")
        assert published, (
            f"{path} served no X-Content-SHA256 — this vendor's whole format "
            f"corrupts quietly, so an unverifiable body is not usable"
        )
        got = hashlib.sha256(blob).hexdigest()
        assert got == published, (
            f"{path} arrived corrupted: the vendor sent sha256 {published} and "
            f"{len(blob):,} bytes hashing to {got}. Parquet keeps its PAR1 "
            f"markers through this, so nothing downstream would have noticed. "
            f"The usual cause is mokapi's text response path mangling binary — "
            f"serve.js must put raw bytes on `response.data`, never `body`."
        )

        dest = f"Files/landing/contoso_reference/{day}/{filename}"
        written = upload(st["workspace"], st["lakehouse"], dest, blob, tok)
        assert written == len(blob), (written, len(blob))

        landed[filename] = {"bytes": str(written), "sha256": got}
        total += written
        log(f"landed {filename} — {written:,} bytes, sha256 verified")

    # The vendor's own numbers, asserted against what it served. These come
    # from the fixture contract, so a generator that changed its shape fails
    # here rather than surfacing as a wrong revenue figure in the star.
    assert len(landed) == 2, sorted(landed)

    # NAME THE VENDOR, as the other ingest steps do. Without this the graph
    # would start at the landed files and the group data office — a separate
    # publisher, reached over HTTP against its own key — would appear nowhere.
    ftok = token(FABRIC_AUD)
    reference = connections.ensure(
        ftok,
        "Contoso Reference",
        "ShareableCloud",
        connections.details("Web", "Web", url=REFERENCE_API),
    )
    connections.announce(
        ftok,
        st["workspace"],
        "ingest_reference",
        "Contoso Reference",
        connections.from_source(
            reference,
            st["lakehouse"],
            [f"Files/landing/contoso_reference/{day}/{name}" for name in landed],
        ),
    )

    state.save(landing_day=day, reference_landed=landed, reference_connection=reference)
    log(
        f"Contoso Reference: {len(landed)} feed(s), {total:,} bytes — "
        f"{ref.EXPECTED_PRODUCTS} products over "
        f"{ref.EXPECTED_DEPARTMENTS} departments, "
        f"{ref.EXPECTED_FX_ROWS} FX rows across "
        f"{ref.EXPECTED_FX_PUBLISHED_DAYS} published days"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
