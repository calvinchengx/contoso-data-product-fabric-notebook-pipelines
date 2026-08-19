"""Platform-side invariants for the vendor stack.

THE VENDORS THEMSELVES ARE NOT HERE. Their specs, serve scripts and the
invariants about what they send moved to `contoso-sources`, which owns them;
this repository used to carry a byte-identical copy and so tested a duplicate.
What remains is what is genuinely this platform's: that the stack it GENERATES
from that declaration gives every vendor its own instance and its own mounts,
and that this platform's own parsing DDL still matches what the vendor
publishes.
"""

import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCES = pathlib.Path(
    __import__("os").environ.get("SOURCES", ROOT.parent / "contoso-sources")
).resolve()
SPECS = sorted(SOURCES.glob("*/openapi.yaml"))

pytestmark = pytest.mark.skipif(
    not SPECS,
    reason=(
        "contoso-sources is not beside this repository. These tests read the "
        "vendor declaration this platform generates its stack from; set "
        "SOURCES=/path/to/contoso-sources."
    ),
)


def test_the_web_bronze_schema_matches_the_vendors_published_spec():
    """A renamed field must fail here, not arrive as a column of NULLs.

    bronze cannot infer Contoso Web's shape — the engine's JSON reader is
    NDJSON-only, so array pages are read as text and parsed with `from_json`,
    which needs the schema spelled out. A spelled-out schema is a copy of the
    vendor's, and copies drift. When they drift `from_json` does not raise: the
    field simply parses to NULL, the row count still matches (it comes from the
    array's length), and every assertion in bronze still passes.
    """
    import yaml

    sys.path.insert(0, str(ROOT / "steps"))
    import web_schema

    spec = yaml.safe_load(
        (SOURCES / "contoso-web" / "openapi.yaml").read_text(encoding="utf-8")
    )
    schemas = spec["components"]["schemas"]
    for component, declared in (
        ("WebCustomer", web_schema.WEB_CUSTOMER),
        ("WebProduct", web_schema.WEB_PRODUCT),
        ("WebOrder", web_schema.WEB_ORDER),
        ("WebOrderLine", web_schema.WEB_ORDER_LINE),
    ):
        published = set(schemas[component]["properties"])
        assert set(declared) == published, (
            f"{component}: bronze parses {sorted(declared)} but the vendor "
            f"publishes {sorted(published)}"
        )


def test_the_web_bronze_schema_keeps_every_leaf_a_string():
    """Bronze records what arrived; it does not decide what a price is.

    The POS reader runs with inferSchema off for the same reason. Typing
    `list_price` as a double here would make bronze an interpretation of the
    vendor rather than a copy of it, and would quietly discard whatever the
    vendor actually wrote when the two disagree.
    """
    sys.path.insert(0, str(ROOT / "steps"))
    import web_schema

    def leaves(fields):
        for name, kind in fields.items():
            if isinstance(kind, dict):
                yield from leaves(kind)
            else:
                yield name, kind

    for component in (
        web_schema.WEB_CUSTOMER,
        web_schema.WEB_PRODUCT,
        web_schema.WEB_ORDER,
    ):
        for name, kind in leaves(component):
            assert kind == "string", f"{name} is declared {kind}, not string"
