"""Put the source-system credentials in the vault.

A real deployment does not have this step: the secrets are already in the
vault, placed by whoever owns them, and the platform's identity is granted
read. It exists here so a `git clone` is self-contained — and it is the ONLY
place a credential value appears, which is what makes every other module able
to say it reads from the vault.

The values come from the fixtures, because for POS and ERP the "vendor" is a
seeded generator whose key is part of the fixture contract.
"""

from __future__ import annotations

import state
import vault
from fabric import log
from sources import (
    ERP_PASSWORD_SECRET,
    POS_KEY_SECRET,
    REFERENCE_KEY_SECRET,
    WEB_KEY_SECRET,
)

# Only for the local family. Against real vendors these are the customer's own
# credentials and this step does not run at all.
ERP_PASSWORD = "contoso-erp-dev"


def main() -> int:
    import reference_data as ref
    import source_system as src
    import web_store as web

    # ONE SECRET PER VENDOR. Contoso Web's key is not Contoso POS's, and the
    # two are seeded separately because in production they are issued by
    # different companies and rotate on different days. Sharing one here would
    # make that impossible to represent later without a migration.
    #
    # Contoso Reference gets its own for the same reason, and the fact that it
    # publishes non-transactional data changes nothing: reference data is not
    # automatically public just because it is not a sales figure.
    ids = {
        POS_KEY_SECRET: vault.put(POS_KEY_SECRET, src.API_KEY),
        WEB_KEY_SECRET: vault.put(WEB_KEY_SECRET, web.API_KEY),
        REFERENCE_KEY_SECRET: vault.put(REFERENCE_KEY_SECRET, ref.API_KEY),
        ERP_PASSWORD_SECRET: vault.put(ERP_PASSWORD_SECRET, ERP_PASSWORD),
    }
    # Read every one back. A vault that accepted a write and cannot serve it is
    # the failure this step exists to rule out, and a PUT returning 201 does
    # not prove a GET works.
    for name in ids:
        assert vault.get(name), name

    state.save(secrets=sorted(ids))
    log(f"vault seeded and read back: {', '.join(sorted(ids))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
