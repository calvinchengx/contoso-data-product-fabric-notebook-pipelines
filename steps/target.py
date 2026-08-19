"""One switch between the emulator and real Fabric: `FABRIC_TARGET`.

THIS PLATFORM IS FABRIC CODE. It is not emulator code that happens to run on
Fabric. Everything the two targets genuinely differ about is resolved here and
nowhere else, so a reader can see the whole difference in one file — and so
that "would this run on real Fabric?" is answerable by reading it rather than
by auditing every call site.

THE CONTRACT ITSELF IS NOT WRITTEN HERE. It is `fabric-target`, published from
the emulator's release workflow and installed by `make fixtures` alongside the
generators. This file is the CONSUMER half: the handful of decisions that are
this platform's policy rather than the toggle's — who plays the Spark pool,
whether a capacity has to be assigned, which Host header OneLake wants locally.

That split is new, and it is the fix for a real defect. While `fabric-target`
was unpublished this file RESTATED the contract, and the restatement drifted:
it resolved the real target to an Entra client-credentials flow and required
AZURE_CLIENT_SECRET, which meant `az login` did not work, a managed identity
did not work, and the platform could not have run inside a Fabric notebook at
all — a notebook has no client secret to give. The published package resolves
`DefaultAzureCredential` instead: env service-principal vars win when set, and
otherwise the developer's own `az login` does. A contract you copy is a
contract you get wrong, so this file no longer copies one.

Ids can never match across targets, so anything durable is addressed BY NAME
and resolved to a GUID per target.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol

import fabric_target


class AccessToken(Protocol):
    token: str


class TokenCredential(Protocol):
    """azure-core's credential shape, structurally.

    Declared rather than imported so this module keeps working when
    azure-identity is not installed — against the emulator it is not, and the
    credential is `fabric-target`'s own stdlib client-credentials object. What
    matters is the shape both satisfy, which is the reason a Fabric notebook's
    managed identity drops in here without anything above knowing.
    """

    def get_token(self, *scopes: str) -> AccessToken: ...


EMULATOR = "emulator"
REAL = "real"

# The one workspace this platform owns. Named here because the name is the
# cross-target address — `provision.py` resolves it to a GUID per target — and
# because real mode is workspace-scoped by construction: `fabric-target`
# refuses to operate tenant-wide, which is the right rule and one this platform
# has no reason to opt out of.
WORKSPACE = "contoso-analytics"

# The capacity this platform runs on, by name. Lowercase alphanumeric because
# that is ARM's rule for a Fabric capacity name (`^[a-z][a-z0-9]{2,62}$`), not a
# style choice — a hyphen here is a 400 from ARM.
CAPACITY = "contosocapacity"


@dataclass(frozen=True)
class ArmCapacity:
    """Where a capacity gets created, on a target that is allowed to create one.

    Azure needs all of this to place the resource, and none of it is derivable:
    a subscription, a resource group inside it, a region, and an F-series SKU.
    `admin` becomes `properties.administration.members`, which ARM requires and
    refuses an empty list for — a capacity with no administrator is not a thing
    Azure will make.
    """

    url: str
    subscription: str
    resource_group: str
    location: str
    sku: str
    admin: str


def target() -> str:
    t = os.environ.get("FABRIC_TARGET", EMULATOR).lower()
    if t not in (EMULATOR, REAL):
        raise SystemExit(f"FABRIC_TARGET must be {EMULATOR!r} or {REAL!r}, got {t!r}")
    return t


@dataclass(frozen=True)
class Target:
    name: str
    api_root: str
    tenant: str
    # A TokenCredential — `get_token(scope) -> .token`. Against the emulator it
    # is the seeded daemon; against real Fabric, DefaultAzureCredential, which
    # is what makes `az login` and a Fabric notebook's own managed identity
    # work without this platform knowing which one it got.
    credential: TokenCredential
    onelake_url: str
    # The emulator serves OneLake on the Fabric port and routes by Host header,
    # the way `curl --resolve` does. Real Fabric has its own hostname and needs
    # no override — so this is the one place that difference lives.
    onelake_host_header: str | None
    verify_tls: bool
    # THE CAPACITY, ADDRESSED BY NAME like everything else durable. A Fabric
    # capacity is an ARM resource (`Microsoft.Fabric/capacities`), created
    # through management.azure.com rather than the Fabric REST API, and the
    # workspace is then assigned to it. That sequence is the same on both
    # targets; only WHERE the capacity comes from differs, which is why it is
    # this field and not a branch in provision.py.
    capacity_name: str
    # Where the platform may CREATE one, or None when it may not. Creating a
    # capacity means creating billable Azure infrastructure, so the real target
    # never does: it resolves a capacity an operator already provisioned.
    capacity_arm: ArmCapacity | None
    # Where the Spark session comes from. In a Fabric notebook `spark` is
    # ambient and this is None; outside one, a Spark Connect endpoint.
    spark_remote: str | None
    # WHO EXECUTES A NOTEBOOK. Real Fabric runs a RunNotebook job on its own
    # Spark pool: the client submits and polls, and nothing else is required of
    # it. The emulator parses the notebook into ordered cells and records a
    # Pending run, then waits for an attached engine to execute them and report
    # back — deliberately, so that a terminal job status means execution really
    # happened rather than that a clock advanced.
    #
    # TRUE ON BOTH TARGETS since fabric-emulator 0.15.0, and the flag stays
    # because the reason it could be false is worth keeping visible. The
    # emulator parks a RunNotebook job until an engine reports — deliberately,
    # so a terminal status means execution rather than a clock advancing — and
    # for a long time nothing a consumer could pull could be that engine. This
    # platform supplied one itself, in a driver that existed only because of
    # that gap.
    #
    # It no longer does: compose runs the published spark-agent and the emulator
    # is given FABRIC_SPARK_AGENT_URL, so the SERVICE executes the notebook, as
    # real Fabric does. Point this platform at a stack without an agent and the
    # job will park — which is the honest outcome, not a regression.
    runs_notebooks_itself: bool
    # WHETHER TIME CAN BE MOVED. A schedule is the one Fabric feature whose
    # whole behaviour is "wait, then act", and waiting is not a test. The
    # emulator's clock is controllable — advance it and every schedule due in
    # the window fires — so the platform can prove a schedule really produced a
    # run. Real Fabric's clock is the world's: the schedule is created and
    # verified, and the firing is left to happen at the hour it says.
    #
    # So on both targets the schedule is created, read back and asserted. Only
    # the "did it fire" half is emulator-only, and this is what gates it.
    clock_is_controllable: bool
    # WHETHER AN EVENT TRIGGER CAN BE BOUND AS CODE, and this one is a genuine
    # asymmetry rather than a convenience.
    #
    # Real Fabric has no public REST for the binding: a Reflex rule fed by an
    # Eventstream is assembled in the PORTAL, by hand. Everything downstream of
    # it is ordinary Fabric — the filter, the job it starts, the TriggerEvent
    # fields the job reads — but the wiring itself is not something a
    # deployment can declare. The emulator exposes an emulator-native surface
    # (`…/reflexes/{id}/triggers`) and says so in its own parity table.
    #
    # So the trigger step is the one place this platform cannot be
    # target-neutral: against production the binding is a portal task and the
    # step says so rather than pretending. Naming the difference here keeps it
    # visible instead of buried in a step nobody re-reads.
    event_triggers_have_rest_api: bool
    # WHETHER AN ENGINE MAY REPORT ITS OWN LINEAGE. Real Fabric derives lineage
    # from the artifacts it manages and accepts no claim from a client, so
    # `POST …/lineage` is an emulator-native extension and the emulator's own
    # parity table records it as one.
    #
    # It exists because plenty of real movement is invisible to the service: an
    # interactive Spark session, a local script, a step that pulls from a
    # vendor API. Without a report the graph begins at a landed file and the
    # system that PUT it there cannot be named at all.
    lineage_can_be_reported: bool
    # WHAT THE WAREHOUSE IS CALLED ON ITS SQL ENDPOINT — the `database` half of
    # a connection, and the one thing about the Warehouse that differs.
    #
    # Real Fabric's SQL endpoint exposes a Warehouse under its DISPLAY NAME:
    # `Sql.Database("<endpoint>", "contoso_warehouse")`. The emulator resolves a
    # TDS database by ITEM ID instead — it calls `EnsureDatabase(ctx, it.ID)`
    # and keys its backing databases the same way — which is why compose sets
    # `DBT_DATABASE: "${WAREHOUSE_ID}"`, a GUID.
    #
    # It matters beyond dbt now that the semantic model carries partitions: the
    # M expression in a partition names this database, and a partition naming
    # the wrong one is a model that opens and loads nothing. dbt got away with
    # it because the value was handed to it through the environment and nobody
    # had to write it down.
    #
    # True here means "address it by id". A Fabric-shaped emitted artifact
    # therefore differs by this one string between targets, which is the honest
    # position until the emulator accepts the display name as an alias.
    warehouse_database_is_item_id: bool
    # Where secrets live. The azure-keyvault-emulator locally, the customer's
    # real vault in production — never the source tree, on either target.
    vault_url: str
    # Real Entra knows Azure SQL and Power BI as first-party resources, so a
    # token for those audiences needs no setup. The emulator's entra mints only
    # for audiences it has been told about, and exposes an admin API to do it.
    # None on the real target means "nothing to register".
    entra_admin_api: str | None

    @property
    def is_emulator(self) -> bool:
        return self.name == EMULATOR

    def token(self, audience: str) -> str:
        """A bearer token for `audience`, from whatever credential this target
        resolved. The audiences are the real Fabric ones on both targets — the
        emulator validates the same strings — so nothing above this line knows
        which identity answered, which is the whole point: `az login`, a
        service principal and a notebook's managed identity are all just a
        credential here.
        """
        return self.credential.get_token(f"{audience}/.default").token

    def warehouse_database(self, item_id: str, display_name: str) -> str:
        """The `database` a SQL client names to reach this Warehouse.

        A method rather than a bare flag at the call site, because the caller
        should not have to remember WHICH of the two it is holding — it has
        both, and this file is where the choice belongs.
        """
        return item_id if self.warehouse_database_is_item_id else display_name

    def delta_storage_options(self, tok: str) -> dict[str, str]:
        """What delta-rs needs to reach OneLake on this target.

        Kept here rather than at the call site so that every difference between
        the emulator and real Fabric is in ONE file — which is what makes
        "would this run on real Fabric?" answerable by reading, instead of by
        auditing each module.
        """
        opts = {
            # The account name is the literal `onelake` on both targets.
            "azure_storage_account_name": "onelake",
            "azure_storage_token": tok,
            "azure_endpoint": f"{self.onelake_url}/onelake"
            if self.is_emulator
            else self.onelake_url,
        }
        if not self.verify_tls:
            # object_store verifies by default and fails with `invalid peer
            # certificate: UnknownIssuer` — naming neither the emulator nor the
            # fix. Never set on the real target.
            opts["allow_invalid_certificates"] = "true"
        return opts


def resolve() -> Target:
    # Declared before the resolver runs, because real mode refuses to construct
    # without a workspace scope. An explicit default is not a workaround for
    # that rule — this platform genuinely owns one workspace, and saying so is
    # what the rule asks for. An operator pointing the platform at a
    # differently named workspace sets FABRIC_WORKSPACE and this leaves it be.
    os.environ.setdefault("FABRIC_WORKSPACE", WORKSPACE)

    # Endpoints, credentials and the seed guards come from the PUBLISHED
    # contract; everything below is this platform's own policy.
    ft = fabric_target.target(target(), fresh=True)
    # `fabric-target` hands back the versioned control plane; this platform
    # composes `/v1` per call, and xmla_probe needs the bare host.
    api_root = ft.api_root.removesuffix("/v1")

    if ft.is_real:
        vault = ft.vault_url
        if not vault:
            raise SystemExit(
                "FABRIC_TARGET=real needs AZURE_KEY_VAULT_URL — secrets come "
                "from the customer's own Key Vault, never the source tree."
            )
        # OPTIONAL, and only consulted when a workspace has to be CREATED.
        # An existing workspace already carries its capacity and the platform
        # adopts it, so a real run against an established workspace needs no
        # capacity configuration at all. Demanding it unconditionally was both
        # redundant and a trap: it invited a value that disagreed with reality,
        # and the code then "corrected" reality to match. Missing is checked
        # where it is needed, in capacity.for_new_workspace.
        capacity = os.environ.get("FABRIC_CAPACITY", "")
        return Target(
            name=REAL,
            api_root=api_root,
            tenant=ft.tenant,
            credential=ft.credential,
            onelake_url=ft.onelake_url,
            onelake_host_header=None,
            verify_tls=True,
            capacity_name=capacity,
            # None: never create billable infrastructure from a pipeline run.
            capacity_arm=None,
            # A Fabric Spark notebook supplies the session; nothing to connect to.
            spark_remote=os.environ.get("SPARK_REMOTE"),
            runs_notebooks_itself=True,
            clock_is_controllable=False,
            event_triggers_have_rest_api=False,
            lineage_can_be_reported=False,
            # Fabric's SQL endpoint exposes a Warehouse by display name.
            warehouse_database_is_item_id=False,
            vault_url=vault,
            entra_admin_api=None,
        )

    return Target(
        name=EMULATOR,
        api_root=api_root,
        tenant=ft.tenant,
        credential=ft.credential,
        onelake_url=ft.onelake_url,
        onelake_host_header="onelake.dfs.fabric.microsoft.com",
        # Off ONLY here, and not by this file's choice — the published contract
        # sets tls_verify False for the emulator, whose family serves
        # self-signed certificates a consumer has no CA for. The TLS path is
        # still exercised. On the real target it is True above, literally, and
        # no configuration reaches it.
        verify_tls=ft.tls_verify,
        capacity_name=CAPACITY,
        capacity_arm=ArmCapacity(
            url=os.environ.get("FABRIC_ARM_URL", "https://localhost:8445"),
            # arm-emulator's seeded subscription and a resource group this
            # platform makes for itself. In Azure both are the operator's.
            subscription=os.environ.get(
                "AZURE_SUBSCRIPTION_ID", "6082bfda-63d0-46f4-8272-ae9195139feb"
            ),
            resource_group="contoso-rg",
            location="westus",
            # The smallest F-SKU. Size is meaningless against the emulator and
            # the cheapest real answer, so nothing here encourages an expensive
            # copy-paste.
            sku="F2",
            admin="admin@contoso.com",
        ),
        spark_remote=os.environ.get("SPARK_REMOTE", "sc://localhost:50051"),
        runs_notebooks_itself=True,
        clock_is_controllable=True,
        event_triggers_have_rest_api=True,
        lineage_can_be_reported=True,
        warehouse_database_is_item_id=True,
        vault_url=ft.vault_url,
        entra_admin_api=ft.entra_url + "/admin/api/apps",
    )
