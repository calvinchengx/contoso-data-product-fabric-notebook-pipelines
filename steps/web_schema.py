"""The shape Contoso Web's JSON pages are parsed with, declared once.

WHY THIS IS A MODULE AND NOT THREE STRINGS IN bronze.py: the test that keeps it
honest has to import it, and bronze.py imports `fabric` at module scope — so a
test importing bronze would need the vendored wheel to be installed. Same reason
apipath.py exists. Nothing in here imports anything.

WHY BRONZE NEEDS A SCHEMA AT ALL, when it reads the POS export without one:
the engine's JSON reader is NDJSON-only, so a page that is one big array has to
be read as text and parsed with `from_json` — and `from_json` cannot infer.
bronze.py carries the measurement behind that claim.

EVERY LEAF IS A STRING, deliberately. `list_price` is a JSON number and could be
declared a double; typing it here would make bronze an interpretation of the
vendor rather than a record of it, which is the same reason the POS reader runs
with inferSchema off. from_json keeps the literal text, so nothing is lost and
silver still gets to decide what a price is.

The field NAMES mirror sources/contoso-web/openapi.yaml, and a test asserts they
still do — a vendor that renames a field would otherwise land a column of nulls,
which every downstream count would happily agree with.
"""

from __future__ import annotations

from typing import Any

# A dict value means "array of struct", the only nesting this vendor has.
WEB_CUSTOMER: dict[str, Any] = {
    "email": "string",
    "full_name": "string",
    "country": "string",
    "signup_ts": "string",
}

WEB_PRODUCT: dict[str, Any] = {
    "product_id": "string",
    "name": "string",
    "category": "string",
    "list_price": "string",
}

WEB_ORDER_LINE: dict[str, Any] = {
    "line_no": "string",
    "product_id": "string",
    "quantity": "string",
    "unit_price": "string",
}

WEB_ORDER: dict[str, Any] = {
    "web_order_id": "string",
    "email": "string",
    "placed_at": "string",
    "status": "string",
    # Stays nested. Exploding to a line grain is a decision, and it belongs
    # where the decision is visible rather than in the parse.
    "lines": WEB_ORDER_LINE,
}


def struct(fields: dict[str, Any]) -> str:
    """Render a field map as Spark DDL, nesting where a value is a map."""
    parts = []
    for name, kind in fields.items():
        rendered = f"array<{struct(kind)}>" if isinstance(kind, dict) else kind
        parts.append(f"{name}:{rendered}")
    return "struct<" + ",".join(parts) + ">"


def array_of(fields: dict[str, Any]) -> str:
    """A whole page: the vendor sends an array of these."""
    return f"array<{struct(fields)}>"
