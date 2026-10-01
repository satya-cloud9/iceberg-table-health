# Architecture notes

A living reference diagram of everything in this document — the
provider/platform/tenant layering, cross-tenant data sharing, and where
the Shared OLTP Service sits — is published at:
https://claude.ai/code/artifact/609184c7-1be9-4aa7-a001-0141d95fc3c0

## The provider contract

`terraform/providers/<name>/` modules are the only place cloud-specific
Terraform lives. Each one does exactly one job — turn "a region on this
provider" into a working Kubernetes cluster — and hands back exactly four
outputs: `kubeconfig_path`, `node_pool_refs`, `workload_identity_mechanism`,
`storage_class_name`. Nothing in `terraform/platform/` or
`terraform/tenants/` reads anything else from a provider module or
branches on which provider produced these values. The full contract,
rationale for exactly these four, and what's deliberately excluded lives
in `terraform/providers/CONTRACT.md`.

This is the mechanism behind "deploy this on any cloud provider": adding
GCP or Azure support later means writing one new module that satisfies
this contract, not touching the shared platform layer or any tenant's
Terraform. The inverse also holds — the platform and tenant layers were
written once, against the contract, and never needed to change across
four very different backing implementations (real k3s-over-SSH, and three
different cloud-emulator families).

### Real provider vs. contract-test double

Only `baremetal/` provisions real infrastructure — it installs k3s over
SSH onto the box you point it at. `aws/`, `gcp/`, and `azure/` are
**contract-test doubles**, each backed by a member of the
[floci](https://floci.io) emulator family (floci for AWS on port 4566,
floci-gcp on port 4588, floci-az on port 4577 — all MIT-licensed, no auth
token, wire-compatible with their respective SDKs). A contract-test double
proves two things for free, before you own a single real cloud account:
that the Terraform HCL is valid against that cloud's real API shape, and
that the four-output contract holds identically no matter which provider
produced it. It does *not* prove real EKS/GKE/AKS runtime behavior — the
actual storage-class CSI driver, the real load-balancer controller,
Workload-Identity actually completing a token exchange against real Entra
ID. Budget one real pass per cloud against the genuine managed service
before calling any of it production-trusted.

**AWS is a partial exception to "does not prove real EKS runtime
behavior," and it's worth being precise about how partial.** `aws/cluster.tf`
used to deliberately use a `null_resource` + the `kind` CLI directly for
the cluster itself, on the reasoning that floci's own EKS emulation would
just be another layer of indirection over the same kind cluster, and
couldn't prove a real IRSA/Workload-Identity token exchange against STS
anyway. That stopped being true: floci's EKS emulation (a real k3s node
per cluster, not metadata-only) gained genuine OIDC-signed
service-account tokens and full EKS Pod Identity support
(floci-io/floci#3783), confirmed empirically against a `nightly` floci
image — `aws eks describe-cluster` returns a real `identity.oidc.issuer`,
not a placeholder. `aws/cluster.tf` now creates the cluster through
floci's own EKS API (`aws_eks_cluster`/`aws_eks_node_group`, the same
`hashicorp/aws` provider already depended on everywhere else in this
directory -- not a new, unverified community provider, so the original
"stay on solid ground" reasoning still holds, just via a different path
to it), and `aws/workload-identity.tf` builds real EKS Pod Identity
associations on top of that.

What this does and doesn't prove, precisely: a clean `tofu apply` proves
floci's EKS API accepts the HCL and that the Pod Identity association
exists as an EKS-API-level object. It does not yet prove a pod running on
floci's k3s node actually receives and uses working credentials from that
association end to end -- that's a real token exchange, genuinely
unverified until apply-tested, same as the GKE/AKS caveat above. See
`terraform/providers/CONTRACT.md`'s "Native federation (Option A)"
section for the current, precise status.

## Why bare metal is the first real target

The near-term hardware is a single mini PC (Ryzen 7 8745HS / 32GB DDR5 /
1TB NVMe) — plenty for the platform layer plus a tenant workspace at the
`pool` isolation tier, with headroom to add a second tenant or trial the
`node_group`/`dedicated` tiers once there's more than one node. Real
AWS/GCP/Azure get added later, specifically for portability and disaster
recovery (being able to stand the same stack up somewhere else), not for
running tenants simultaneously split across providers — that's a
meaningfully more complex problem (cross-provider networking, data
gravity, consistent identity) this repo doesn't take on. Because the
provider contract already exists, "add AWS later" is additive: a new
`terraform/providers/aws` apply, feeding the same `terraform/platform` and
`terraform/tenants` modules that already run against bare metal today.

## Multi-tenancy: the workspace boundary

Production multi-tenant architecture is respected from the start rather
than bolted on: each tenant gets its own **workspace** — a Kubernetes
namespace combining that tenant's Trino, Postgres (pool schema or
dedicated instance), object storage, and a Grafana folder — wrapped in an
isolation boundary. `terraform/tenants/_template/` is the reusable module;
`terraform/tenants/tenant-a/` is a thin root instantiation of it. Adding a
tenant is a new instantiation directory, never a copy of `_template`
itself or an edit to it.

The isolation boundary is a Kubernetes `NetworkPolicy` default-deny with
explicit allows for: same namespace, the shared `platform` namespace
(Kestra, Nessie, the Shared OLTP Service), and the `observability`
namespace. Combined with per-namespace `ResourceQuota`s, this is what
keeps "isolation tier" a variable rather than a forked codebase.

### Isolation tiers

`isolation_tier` (`pool` | `node_group` | `dedicated`) is a plain
Terraform variable on the tenant module, not a different code path:

- **pool** — the default, and the only tier meaningful on a single-node
  cluster. The tenant's Postgres-backed data lives as its own schema
  inside the shared `tenant-pool-postgres` instance in the platform
  layer (`terraform/platform/tenant-pool-postgres.tf`); Trino, MinIO,
  and everything else still run as the tenant's own pods in its own
  namespace.
- **node_group** — same as `pool`, but the tenant's pods get scheduled
  onto a dedicated node pool via node selectors/taints. Needs real
  additional node capacity to mean anything physically.
- **dedicated** — the tenant gets its own Postgres instance instead of a
  pool schema, in addition to node-level separation.

Implemented via `count = var.isolation_tier == "pool" ? 1 : 0` (and its
inverse) in `terraform/tenants/_template/postgres.tf` — toggling which
resources get created, not which module gets called.

## The platform layer: what's shared, and why

`terraform/platform/` is the one thing every tenant depends on and no
tenant owns: Nessie (the Iceberg REST catalog, with its own dedicated
Postgres for persistence — the in-memory default loses state on
restart), Kestra (still standalone mode; see below), the tenant-pool
Postgres, the Shared OLTP Service, and observability (kube-prometheus-stack,
Loki, Tempo).

### Why Nessie instead of Glue-via-floci

The original design used AWS Glue (via floci) as the Iceberg catalog,
specifically because it carried over to real AWS with the least change.
That stopped making sense the moment "any cloud provider" became a real
requirement — Glue is AWS-only, full stop, so a GCP or Azure tenant would
have needed a second catalog implementation and code above the provider
contract that branches on which cloud it's running on. Nessie is a REST
catalog: one implementation, reachable identically regardless of which
provider's Kubernetes cluster it's running on, and it happens to add
table branching/versioning semantics Glue never had. The cost is one more
service to run and operate, accepted specifically for the cross-provider
requirement.

### Why Kestra stays in standalone mode for now

Kestra 2.0 supports a Controller/Worker-Group split, which is the right
shape once there's a reason to isolate *execution* per tenant (a tenant's
flows shouldn't be able to starve another's, or you want per-tenant worker
scaling). On a single mini PC there's no such pressure yet — standalone
mode is simpler to operate and this is a deployment-topology decision, not
a flow-authoring one, so nothing about how flows are written changes when
that split eventually happens.

### Cross-tenant data sharing and the Shared OLTP Service

Two different sharing problems came up, and they're handled differently:

- **Iceberg tables one tenant wants to share with another** go through
  Nessie's own REST-catalog grants — no separate mechanism needed, since
  the catalog is already outside any tenant's boundary.
- **Postgres-resident data that's genuinely cross-tenant** — the case
  Iceberg historically handles badly here (row-level concurrent commits)
  and where plain Postgres was already the fallback — lives in the
  **Shared OLTP Service**, in the platform layer, *never* inside a
  tenant's namespace even when one tenant "owns" one side of the
  relationship. It uses Row-Level Security with a `current_setting('app.tenant_id', true)`
  policy plus a `shared_with` array column (example bootstrap SQL is in
  `terraform/platform/shared-oltp.tf`'s ConfigMap) so isolation is
  enforced at the query layer even though the data physically lives in
  one shared instance. This is a deliberate exception to "everything
  tenant-scoped lives inside the tenant's namespace" — the alternative
  (replicating shared rows into every tenant that needs them) trades a
  RLS policy for a distributed-consistency problem, which is worse.

## Cross-stage plumbing: how state stays separate but data still flows

Terraform state is separated per provider instance / platform / tenant
from the start, specifically so a new provider or a second tenant never
means migrating shared state. What crosses those boundaries is a handful
of plain values (a kubeconfig path, a catalog URI, a postgres host), not
shared state — carried via `tofu output -json` captured into
`terraform/generated/<stage>.tfvars.json` and passed forward with explicit
`-var-file` flags (see `scripts/03-apply-provider.sh` through
`scripts/05-apply-tenant.sh`). This is deliberately *not* Terraform's
same-directory `.auto.tfvars.json` auto-loading — these files live outside
each stage's own directory, so auto-loading would silently do nothing.

One real Kubernetes wrinkle this surfaces: a Job in a tenant's namespace
can't read a Secret that lives in the `platform` namespace via a pod's
`secretKeyRef` (that's namespace-scoped). The pool-tier schema-creation
Job in `terraform/tenants/_template/postgres.tf` works around this with a
Terraform `data "kubernetes_secret_v1"` data source instead — that reads
through the Kubernetes API at plan/apply time (not namespace-scoped the
way a pod's own secret mount is) and passes the value in as a literal env
value.

## Known gaps / things not yet exercised

- No ingress controller / TLS — everything is reached via
  `kubectl port-forward` (see `make status`). Fine for a single-operator
  test rig, not representative of how you'd expose these UIs in
  production (a real ingress controller + cert-manager, typically).
- No autoscaling (HPA/cluster autoscaler) — bare metal is a fixed-size
  box by nature; this becomes relevant once a cloud provider is added
  for real.
- IAM/identity policy correctness isn't validated against the emulator
  family the way real AWS/GCP/Azure would enforce it — treat the
  Terraform in `providers/aws|gcp|azure` as "does the resource graph and
  contract shape make sense," not "is this policy correct in production."
  `providers/gcp` and `providers/azure` also carry explicit comments
  flagging specific unverified mechanics (floci-gcp/floci-az's exact
  kubeconfig-retrieval and health-check-path behavior, and azurerm's
  endpoint-override mechanism for pointing at floci-az) — verify these
  empirically on first real `tofu apply`, don't assume the comments are
  fact.
- Trino resource-groups (per-tenant query-level quotas, beyond the
  namespace-level `ResourceQuota`) aren't wired up yet — flagged as a
  known gap in `terraform/tenants/_template/trino.tf`.
- Trino/Kestra metrics aren't wired into Prometheus yet (see the note at
  the bottom of `helm-values/kube-prometheus-stack-values.yaml`) — logs
  via Loki and dashboards via Grafana (including per-tenant folder
  discovery via the sidecar's `folderAnnotation`) work out of the box;
  metrics need a ServiceMonitor added per component once the base stack
  is confirmed running.

## Why MinIO *and* floci/floci-gcp/floci-az, not just one

Each emulator in the floci family has its own S3-ish object storage, so in
principle any one of them alone could serve as both the cloud
control-plane emulator and the data plane. Splitting them — the emulator
for control-plane APIs (IAM/STS/KMS/networking), MinIO for the actual
object storage Trino reads and writes — is the pattern most local Iceberg
lakehouse setups converge on: MinIO is a purpose-built, fast S3-compatible
store, while the emulators' own S3-ish implementations are fine for
control-plane-adjacent operations but aren't built for the volume of small
PUT/GET calls Iceberg's manifest-file-heavy layout generates. On bare
metal, MinIO is also just real storage, not an emulation of anything —
the split matters even more there than on the cloud contract-test doubles.

## Provisioning-tooling portability (below the Kubernetes API)

Separately from the provider-contract question above the Kubernetes API,
`scripts/01-install-deps.sh` detects the OS package manager (apt/dnf/yum)
and CPU architecture (amd64/arm64) at runtime rather than assuming a
specific distro, so the same install script runs unmodified whether it's
setting up the bare-metal mini PC, a cloud VM used to test one of the
contract-test-double providers, or an ARM box. This is a genuinely
separate concern from the provider contract itself — it's about the tool
installation step that happens once per machine, not about which cloud's
API anything talks to.

