"""Where the source systems live.

Contoso POS, Contoso Web and Contoso ERP are NOT Fabric. They are the vendors a
Fabric pipeline pulls from, and in production they are a real REST endpoint, a
real Postgres and a real Kafka broker. Keeping their addresses out of fabric.py
is the same discipline as keeping the emulator out of the platform: a Fabric
client should not know what a vendor's DSN looks like.

Every value is overridable, because that is the only thing that changes when
this platform is pointed at real vendors.
"""

from __future__ import annotations

import os

# Contoso POS — the vendor's export API. mokapi serves the seeded generator's
# bytes here; in production this is the vendor's own hostname.
POS_API = os.environ.get("POS_API_URL", "http://localhost:18090")
# The NAME of the secret, never its value. Resolved from Key Vault at use.
POS_KEY_SECRET = os.environ.get("POS_KEY_SECRET", "contoso-pos-api-key")

# Contoso Web — the storefront's export API. A SECOND VENDOR with its own
# endpoint and its own key: the POS credential must not open this door, which
# is what having two vendors means rather than two routes on one.
WEB_API = os.environ.get("WEB_API_URL", "http://localhost:18091")
WEB_KEY_SECRET = os.environ.get("WEB_KEY_SECRET", "contoso-web-api-key")

# Contoso Reference — the group data office's master-data feed. NOT an
# operational system: it publishes the definitions the other three are reported
# against, which is why it is a vendor in its own right rather than a table
# someone maintains inside the platform. Its own key, like everyone else's.
REFERENCE_API = os.environ.get("REFERENCE_API_URL", "http://localhost:18092")
REFERENCE_KEY_SECRET = os.environ.get(
    "REFERENCE_KEY_SECRET", "contoso-reference-api-key"
)

# Contoso ERP — a relational source, captured by CDC.
ERP_HOST = os.environ.get("ERP_HOST", "localhost")
ERP_PORT = os.environ.get("ERP_PORT", "55432")
ERP_DB = os.environ.get("ERP_DB", "erp")
ERP_USER = os.environ.get("ERP_USER", "contoso")
ERP_PASSWORD_SECRET = os.environ.get("ERP_PASSWORD_SECRET", "contoso-erp-password")


def erp_dsn() -> str:
    """The ERP connection string, with the password read from Key Vault.

    A function rather than a constant: the password is fetched when it is
    needed, so it is never sitting in a module attribute for the lifetime of
    the process, and never in this file at all.
    """
    import vault

    return (
        f"postgresql://{ERP_USER}:{vault.get(ERP_PASSWORD_SECRET)}"
        f"@{ERP_HOST}:{ERP_PORT}/{ERP_DB}"
    )


DEBEZIUM = os.environ.get("DEBEZIUM_URL", "http://localhost:18083")
REDPANDA = os.environ.get("REDPANDA_BOOTSTRAP", "localhost:19092")
ERP_TOPIC = os.environ.get("ERP_TOPIC", "contoso.erp.customer")
