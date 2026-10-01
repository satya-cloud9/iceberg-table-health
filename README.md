# iceberg-table-health

An Iceberg Table Health & Compaction Advisor, built on a fork of
[lakehouse-anywhere-a](https://github.com/satya-cloud9/lakehouse-anywhere-a)
(`feature/oidc`). This fork is **EKS-only** (Floci-emulated EKS, then real
EKS) and uses **Nessie** as the Iceberg catalog. See [ROADMAP.md](ROADMAP.md)
for milestones.

> The rest of this README is inherited from upstream and still describes
> the multi-provider layout; the gcp/azure/baremetal providers have been
> removed here and `PROVIDER` is pinned to `aws`.

---

# lakehouse-anywhere

A build/test rig for a lakehouse stack — Terraform, Kestra, Iceberg, Trino,
and observability — organized around a formal **provider contract** so the
same Kubernetes-and-up layer runs unmodified on bare metal, AWS, GCP, or
Azure, and around a **tenant workspace** module so a production-shaped
multi-tenant deployment (isolated namespace, own Trino/Postgres/storage/
Grafana folder per tenant) is the default shape, not an afterthought bolted
on later. See `docs/architecture.md` for the full reasoning; this file is
the how-to-run version.

## The three-stage structure

```
terraform/
  providers/<name>/   one real infra provisioner per cloud -- see providers/CONTRACT.md
    baremetal/          REAL: installs k3s over SSH onto your own box
    aws/                 contract-test double: floci + kind
    gcp/                  contract-test double: floci-gcp
    azure/                contract-test double: floci-az
  platform/             ONE shared control-plane layer: Nessie (Iceberg REST
                          catalog), Kestra (shared, standalone), the Shared
                          OLTP Service, the tenant-pool Postgres, observability
  tenants/
    _template/           reusable module: namespace + NetworkPolicy + Trino +
                          MinIO + Postgres (pool schema or dedicated instance,
                          by isolation_tier) + a per-tenant Grafana folder
    tenant-a/             root instantiation of _template -- adding a tenant
                          means a new directory like this one, never editing
                          _template
```

Each stage is a **separate Terraform state** (separate directory, separate
`tofu init`/`apply`). A provider module hands back exactly four outputs —
`kubeconfig_path`, `node_pool_refs`, `workload_identity_mechanism`,
`storage_class_name` (see `terraform/providers/CONTRACT.md`) — and nothing
above that line is allowed to read anything else from it or branch on which
provider produced them. That's what makes adding a new cloud provider later
mean "write one new module under `terraform/providers/`," not "touch
`terraform/platform` or `terraform/tenants`."

Outputs flow forward via captured JSON var-files, not Terraform's
same-directory auto-loading (these files live outside each stage's own
directory, so they're passed explicitly):

```
terraform/providers/<p>  --tofu output-->  terraform/generated/<p>.tfvars.json
                                                    |
                                                    v  -var-file
terraform/platform        --tofu output-->  terraform/generated/platform.tfvars.json
                                                    |
                                                    v  -var-file (both files)
terraform/tenants/<name>
```

## Which provider is "real" today

Only `baremetal/` provisions a real cluster (k3s over SSH). `aws/`, `gcp/`,
and `azure/` are **contract-test doubles**: they prove the Terraform HCL is
valid against each cloud's real API shape and prove the four-output
contract holds identically across providers, using the free/open
[floci](https://floci.io) emulator family (floci for AWS, floci-gcp,
floci-az) — no real cloud account needed to validate the plumbing. They do
not prove real EKS/GKE/AKS runtime behavior (their storage CSI driver, load
balancer controller, or IRSA/Workload-Identity token exchange against real
STS/Entra) — budget one real pass per cloud before trusting any of this in
production. See `terraform/providers/CONTRACT.md` for the full table and
per-provider caveats.

## Sizing

Everything running concurrently (platform layer + one tenant) needs
roughly **7-8 vCPU / 13-15 GB RAM** in requests. The reference target is
the mini PC in `docs/architecture.md` (Ryzen 7 8745HS / 32GB DDR5) —
comfortably above that floor, with headroom for a second tenant or the
node-group/dedicated isolation tiers later. `scripts/01-install-deps.sh`
detects your package manager (apt/dnf/yum) and CPU architecture (amd64/
arm64) at runtime, so it's not tied to a specific distro or provider's
default image.

## How this is meant to be run

None of this has been run end-to-end in the sandbox this was written in
(no Docker Hub / cloud API access there) — it's a first pass meant to be
run and iterated on *with you*. Run it phase by phase, not `make up` blind
on the first pass, and paste back the output of any phase that fails.
Every target takes `PROVIDER` (`baremetal` | `aws` | `gcp` | `azure`,
default `aws`) and `TENANT` (default `tenant-a`):

```
make preflight                          # phase 0: check the box meets the sizing assumptions
make install                             # phase 1: install docker, kind, kubectl, helm, tofu
make emulator-up PROVIDER=aws            # phase 2: start floci (skipped for baremetal)
make provider-apply PROVIDER=aws         # phase 3: provision the cluster, capture contract outputs
make platform-apply PROVIDER=aws         # phase 4: Nessie, Kestra, Shared OLTP, observability
make tenant-apply PROVIDER=aws TENANT=tenant-a   # phase 5: this tenant's Trino/MinIO/Postgres/Grafana
make flows                               # phase 6: register the example Kestra flow
make status PROVIDER=aws TENANT=tenant-a # print all service URLs / port-forward commands
```

Or, once you trust a given `PROVIDER`, `make up PROVIDER=baremetal` runs
phases 0-6 in sequence.

`make teardown` stops the emulator/local state only, leaving every
Terraform state untouched. `make destroy` additionally runs `tofu destroy`
at every applied stage (tenant, then platform, then provider) — see
`scripts/99-teardown.sh`.

## Adding a tenant

Copy `terraform/tenants/tenant-a/` to `terraform/tenants/tenant-b/`, change
`tenant_id` (and `isolation_tier` if it should differ) in its `main.tf`,
then `TENANT=tenant-b bash scripts/05-apply-tenant.sh`. Nothing in
`terraform/platform` or the other tenant's state needs to change — that
isolation is enforced by the per-namespace NetworkPolicy default-deny plus
each tenant's own Terraform state.

## Adding a cloud provider

Write a new `terraform/providers/<name>/` module producing the same four
contract outputs (`terraform/providers/CONTRACT.md` has the exact shape
and a worked list of what's deliberately *not* in the contract). Nothing
in `terraform/platform` or `terraform/tenants` changes.

## Trimming the footprint (8 GB boxes)

If you're testing on something smaller than the reference target:
- Skip Loki + Tempo initially (comment them out of `terraform/platform/observability.tf`).
- Cap Trino's JVM heap at 1.5-2 GB in `helm-values/trino-values.yaml`.
- Kestra's documented floor is 4 GB / 2 vCPU standalone — don't go below that.
- Use `isolation_tier = "pool"` (the default) for every tenant until you
  actually have spare node capacity — `node_group`/`dedicated` only mean
  something physically once there's more than one node.

## Repo layout

```
terraform/providers/<name>/   one infra provisioner per cloud (see CONTRACT.md)
terraform/platform/             shared control-plane: Nessie, Kestra, Shared OLTP, observability
terraform/tenants/_template/   reusable per-tenant workspace module
terraform/tenants/<name>/       root instantiation per tenant
scripts/                              setup scripts, run in numeric order (or via `make`)
helm-values/                        Helm values for MinIO, Trino, Kestra, observability
kestra/flows/                      example Kestra flow definitions
trino/catalog/                    reference Iceberg catalog properties (also generated as a ConfigMap per tenant)
docs/                                    architecture notes
```

## Getting this onto your GitHub repo

This directory is already a git repo with one commit per phase — extracting
the tarball is all the "cloning" you need to do; there's nothing further to
pull. From inside this extracted directory, wherever you have GitHub auth
set up:

```
git branch -m master main
git remote add origin git@github.com:<you>/<repo>.git
git push -u origin main
```

Create the GitHub repo first, as **completely empty** — do not let GitHub
auto-initialize it with a README/license/.gitignore. If it's not empty,
`git push` will be rejected because the two histories don't share a common
ancestor; either delete and recreate the repo empty, or
`git push -u origin main --force` if you're sure overwriting it is what you want.
