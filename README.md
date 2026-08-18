# contoso-data-product-fabric-notebook-pipelines

**Reserved.** This repository is a cell in the Contoso family matrix that has
not been built yet: **Fabric**, orchestrated by **notebooks + Data Pipelines**.

It holds nothing but this file and a LICENSE on purpose. A reserved repository
costs nothing and makes the shape of the family legible; a *witnessed* cell is
the expensive thing, and one is only built when it proves something no existing
cell proves.

## What will live here

Exactly what a team on this platform would write, and nothing else:

the notebooks and pipeline definitions a Fabric team would write.

## What will never live here

Transform SQL, an ODCS contract, or an expected number. Those live once, in
[contoso-data-product](https://github.com/calvinchengx/contoso-data-product),
and this leaf will depend on it **by tag**. A second copy of a gold model is
how "one data product, many engines" stops being true.

## Its platform

[fabric-platform-notebook-pipelines](https://github.com/calvinchengx/fabric-platform-notebook-pipelines)
stands up the infrastructure and runs this leaf. The platform carries no
Contoso name; this leaf carries no infrastructure.

## Where this fits

- [The family](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/00-family.md) — the matrix and the four tiers
- [The plan](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/01-plan.md) — where every cell stands, and what blocks this one
