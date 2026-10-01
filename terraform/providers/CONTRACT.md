# The provider contract

Every module under `terraform/providers/<name>/` does exactly one job:
turn "a region on this provider" into a working Kubernetes cluster, and
hand back four outputs in this exact shape. Nothing in `terraform/platform/`
or `terraform/tenants/` is allowed to read anything else from a provider
module, or to branch on which provider produced these values — that's
what keeps this repo reorganizable for a new provider without touching
the code above this line.

```hcl
output "kubeconfig_path" {
  description = "Local path to a kubeconfig file for the cluster this module created. Platform/tenant modules point their kubernetes/helm providers at this."
  value       = "..."
}

output "node_pool_refs" {
  description = "List of node-pool/node-group identifiers this module created (strings — provider-specific format is fine, this is opaque to everything above)."
  value       = ["..."]
}

output "workload_identity_mechanism" {
  description = "One of: pod-identity | workload-identity | azuread-workload-identity | static-secret. Tells the platform/tenant layer which pattern to use when wiring a pod's cloud credentials -- static-secret is the fallback for bare-metal/other (and AWS's own default today), where there's no federation active. See \"Known gap\" below for the current, real status of the other three."
  value       = "..."
}

output "storage_class_name" {
  description = "Name of the Kubernetes StorageClass this module's cluster provisions PersistentVolumes against by default."
  value       = "..."
}
```

## Why these four and not more

`kubeconfig_path` is the only thing that actually has to be true for
anything above this line to work at all. The other three exist because
we already found real places the platform/tenant layer needs to make a
provider-shaped decision (which identity pattern to wire a ServiceAccount
to, which storage class to request in a PVC) — and the fix is to make
that decision data the provider module hands over, not a per-provider
`if` inside `terraform/platform` or `terraform/tenants`. If a future
change needs a fifth thing from every provider, it goes here first,
before any provider module is touched.

## The object-storage outputs

Added after finding that `terraform/platform`'s Nessie could not boot
without a specific tenant's MinIO already existing — a real dependency
running the wrong direction (platform depending on tenant, not the other
way around). The fix: object storage is a provider-layer concern, the
same way compute and identity already are. Every provider module now
also hands back:

```hcl
output "object_storage_endpoint" {
  description = "S3-API-compatible endpoint. Platform and tenant layers point Nessie/Trino's S3 client at this and never create or own a storage server themselves."
  value       = "..."
}

output "object_storage_bucket" {
  description = "Name of the single shared bucket/container for this environment. Tenant isolation is by path prefix inside it (s3://bucket/tenant-a/, tenant-b/, ...), not by separate buckets or servers."
  value       = "..."
}

output "object_storage_access_key_id" {
  description = "Static credential, sensitive. Real per-tenant access scoping via workload_identity_mechanism is a separate, not-yet-wired piece of work -- see docs/lakehouse-series article 3. Until that lands, every tenant shares this one credential and prefix-level isolation is a convention, not an enforced boundary."
  value       = "..."
  sensitive   = true
}

output "object_storage_secret_access_key" {
  value     = "..."
  sensitive = true
}
```

For bare metal there's no cloud storage API to call, so the provider
module runs a single shared MinIO instance itself -- as a plain Docker
container on the host, over the same SSH connection this module already
uses to install k3s (see storage.tf), not as a Kubernetes workload. That
keeps this module's original "only touches the box itself, never the
cluster's own API" rule intact, and it's a closer match to how real
cloud object storage actually works anyway: an external service pods
reach over the network, not something running inside the same cluster
it's meant to be independent of. For AWS/GCP/Azure this is the real
bucket the provider module already created as a contract-test-double
"parity" resource -- promoted here from validated-but-unused to actually
load-bearing.

**Known limitation, not yet solved by this change**: Nessie's own
warehouse-to-tenant mapping is still static server config, not something
that can be registered at runtime without a restart (confirmed against
Nessie's own docs -- there is no live API for this today). So while
platform no longer *depends on a tenant existing to boot*, platform's
code still has to know a tenant's name to give it a queryable warehouse
entry, and a real new-tenant onboarding still means a one-line addition
to `terraform/platform/catalog.tf` plus a re-apply. That's a smaller,
config-time coupling, not the boot-time one this change removes -- worth
being precise about the difference rather than claiming this fully
decouples platform from tenant identity.

## Known gap: workload identity has no binding mechanism yet

`workload_identity_mechanism` above only *names* a pattern
(`irsa | workload-identity | azuread-workload-identity | static-secret`).
For most of this repo's life nothing above this line actually used that
name to bind a pod's ServiceAccount to a cloud identity on any provider —
object storage access went through the static, long-lived credential in
the section above instead. That's still true by default everywhere
today, AWS included — see "Native federation (Option A)" below for what's
actually wired, gated behind opt-in variables so nothing changes until
you turn it on.

The obvious next step — read a provider-specific extra output (a GCP
service-account email, an AWS IAM role ARN, an Azure managed-identity
client ID) and use it to wire a Kubernetes ServiceAccount's federation
directly — is exactly the per-provider branch this contract exists to
rule out above this line. `aws/iam.tf`, the most complete of the four
provider modules, already shows what that branch looks like unfinished:
its IAM role's trust policy is honestly a placeholder today, trusting
nothing more specific than "some EKS service," with no reference yet to
which Kubernetes namespace or ServiceAccount is even allowed to assume
it. That's not an AWS-specific shortfall — GCP's and Azure's modules have
the identical shape of gap.

The reason it isn't a small fix: the binding needs facts from both sides
of the apply boundary at once — the cloud identity, which only the
provider module knows, and the exact Kubernetes namespace and
ServiceAccount name, which only `terraform/platform` creates — and
platform applies strictly after the provider module, so neither side can
read the other's not-yet-existing resource. The direction under
consideration, not yet built on any provider: stop treating the
namespace/ServiceAccount name as something that has to cross the line at
apply time at all, and fix it instead as a plain naming convention both
phases already agree to independently. A provider module could then build
its entire trust binding unilaterally, in its own phase, using nothing
but its own inputs, and hand `terraform/platform` one new
provider-agnostic output — a map of ServiceAccount annotations, empty
where there's no federation to wire — instead of a cloud-specific value
platform would otherwise need to know how to interpret. That output would
be optional-with-fallback like `workload_identity_mechanism` itself
(bare metal has nothing to federate and would return an empty map), not
mandatory like the object-storage group above.

Separate, later, and explicitly not claimed here: whether real identity
federation actually works end to end, and whether the local emulators
involved (`kind`, floci-gcp, floci-az) can even prove a token exchange
against it at all. This gap was found by design review — trying to use
the contract for something it was heading toward — not by anything
breaking in production, which is the point of catching it here before any
platform/tenant code gets written against the wrong shape. See
`docs/ROADMAP.md` for where this sits relative to the resiliency/
multi-provider phases, and `scratch/lakehouse-series-04-what-the-contract-doesnt-promise.md`
for the fuller writeup.

A broker-based candidate -- a HashiCorp Vault instance sitting between
the provider's one bootstrap cloud identity and every tenant's own pods
-- used to be implemented and documented here for AWS
(`workload_identity_mechanism == "vault-broker"`). Removed from this
branch entirely (code, docs, and the enum value itself) -- kept on a
separate branch instead. The mechanism this repo is pursuing here going
forward is native federation, below.

## Native federation (Option A), and the new output it needed

The naming-convention fix floated in "Known gap" above — a provider-agnostic
map of ServiceAccount annotations, built unilaterally inside each provider
module, handed up instead of a raw cloud-specific value — is no longer just
floated. It's a fifth, optional output now: `service_account_annotations`,
`map(tenant_id -> map(string))`, declared on all four provider modules
(`aws/outputs.tf`, `gcp/outputs.tf`, `azure/outputs.tf`,
`baremetal/outputs.tf`), and read exactly once, in
`terraform/tenants/_template/trino.tf`'s `serviceAccount.annotations`, via
`lookup(var.service_account_annotations, var.tenant_id, {})`. Optional-with-
fallback the same way `workload_identity_mechanism` itself is: bare metal's
copy is hardcoded `{}` (nothing there to federate), and every other
provider's is empty per-tenant unless that tenant_id is actually in
`var.tenant_ids`.

**Status: wired end to end on all three cloud providers. AWS's HCL-level
wiring has been reviewed against a real empirical test of the emulator
(floci's DescribeCluster confirmed to return a real OIDC issuer — see
below); a full `tofu apply` chain (provider -> platform -> tenant) with
Option A on has not been run on any of the three yet.**
`gcp/workload-identity.tf` and `azure/workload-identity.tf` build the
per-tenant trust object this design needs — a `google_service_account` plus
a `roles/iam.workloadIdentityUser` binding, an
`azurerm_user_assigned_identity` plus an `azurerm_federated_identity_credential`
— entirely inside that provider's own directory, `for_each`-keyed over a new
`var.tenant_ids`, using nothing platform or tenant creates.

AWS's own `aws/workload-identity.tf` builds something differently shaped
now: EKS Pod Identity, not classic OIDC-federated IRSA. An earlier version
of this file built the classic design — an `aws_iam_openid_connect_provider`
plus a ServiceAccount-annotation-based trust condition — but that was
blocked on `kind` having no publicly-reachable OIDC discovery document, and
was never apply-tested. AWS's cluster is no longer built with `kind` at
all (see "Current implementations" below); once it moved to floci's own
EKS API, EKS Pod Identity turned out to be fully supported there
(`aws_eks_addon` for `eks-pod-identity-agent`, `aws_eks_pod_identity_association`
per tenant plus one platform-wide association for Nessie), which is both
simpler than classic IRSA (no OIDC provider, no thumbprint, no
ServiceAccount annotation for anything to inject from) and doesn't share
the JWKS-hosting blocker at all. `service_account_annotations` is
consequently always `{}` for AWS now — Pod Identity associations are made
through the EKS API directly, keyed on namespace + ServiceAccount name,
never through the pod spec. GCP and Azure's designs are still
annotation-based, so that output is still live for them.

Same contract-clean property either way: the per-tenant scoping object
gets created somewhere that already owns both halves of the pairing it
needs — the provider module itself, in its own apply — rather than needing
platform or tenant to hand it anything.

`var.tenant_ids` is discovered, not hand-maintained: `scripts/03-apply-provider.sh`
now scans `terraform/tenants/*` (excluding `_template`) before every
provider apply and writes the result to that provider directory's own
`tenant_ids.auto.tfvars.json` (gitignored, regenerated every run). This is
the one real new cost native federation has over the static shared
credential every provider defaults to: **onboarding a tenant now means a
provider re-apply first**, not just that tenant's own apply — `scripts/05-apply-tenant.sh`'s header comment
about adding a tenant being "another module call... never editing
terraform/tenants/_template" is still true, but it's no longer the whole
story once Option A is active on a given provider. `for_each` (never
`count`) keyed on tenant_id keeps that re-apply incremental rather than a
full re-plan — existing tenants' resources keep their same addresses,
onboarding tenant-c doesn't touch tenant-a or tenant-b's state.

AWS still gates behind an explicit opt-in --
`enable_native_workload_identity` (default `false`) -- but no longer needs
`oidc_issuer_url`/`oidc_issuer_thumbprint` the way the classic-IRSA design
did: EKS Pod Identity needs no OIDC issuer at all, and floci's EKS
emulation handles the signing internally. Leave the switch at its default
and `aws/workload-identity.tf` is a no-op, same as before. One real
external prerequisite now, though: this only works on a floci image that
includes the EKS Pod Identity/OIDC-signing work
(floci-io/floci#3783, merged 2026-09-18) -- confirmed missing from a
`floci/floci:latest` image built 2026-09-15, confirmed present on a
`floci/floci:nightly` image built 2026-09-21 (empirically, via
`aws eks describe-cluster` returning a real `identity.oidc.issuer`; see
`docker-compose.floci.yml`'s header comment). Not yet in a stable pinned
release as of this writing.

Also unverified: whether floci-gcp/floci-az's STS-equivalents actually
validate a federated token against a real, fetched signing key, or accept
the resource shape without enforcing it — and, on GCP and Azure, whether
their respective static scoping primitives (a GCP IAM Condition's
`resource.name.startsWith(...)`, an Azure ABAC condition requiring
Hierarchical Namespace per `azure/workload-identity.tf`'s own header
comment) are actually enforced by the emulator or only accepted as valid
HCL. On AWS specifically: floci's EKS emulation does validate some things
for real (confirmed empirically -- `CreateCluster` rejected a
non-existent subnet ID outright) but was lenient about others in the same
test (accepted a cluster IAM role ARN that didn't exist in its IAM state
at all), so "floci accepted the apply" and "floci actually enforces the
IAM/Pod-Identity boundary the same way real EKS/STS would" are still two
different claims -- only the first is confirmed. A clean `tofu apply`
across all three clouds would prove the HCL is valid and mutually
consistent with the four-output contract. It would not, by itself, prove
tenant isolation on any of them — the same overclaim this contract's own
discipline exists to catch everywhere else in this file.

## The storage-protocol fork, and the sixth output that keeps it out of `_template`

GCP's review above ("Also unverified...") and a later, real apply against
`gcp/workload-identity.tf` (see `docs/lakehouse-series` article 3's final
sections) turned up something Option A hadn't hit on any other provider
yet: a case where two clouds genuinely need different Trino/Nessie
*storage-connector configuration*, not just a different credential value
plugged into the same configuration. AWS, Azure, and bare metal all talk
to their object store through the same S3-compatible surface
(`fs.s3.enabled`, `s3.*` properties) -- Option A only ever changed which
`s3.aws-*-key` properties get omitted, per
`contains(["pod-identity", "irsa"], var.workload_identity_mechanism)` in
`_template/trino.tf` and the matching local in `platform/catalog.tf`.
GCP's real object store is GCS, and GCS's S3-compatible interoperability
API is HMAC-key-shaped by protocol design -- a fixed access-id/secret
pair, not a bearer token -- so no Workload-Identity-issued OAuth token can
ever authenticate through it, confirmed live: a real `tofu apply` against
`gcp/workload-identity.tf` produced a correctly-scoped per-tenant GSA, IAM
binding, and prefix-scoped Condition, and Nessie went `Ready` anyway using
nothing but the same static HMAC key every tenant already shares, because
that's the only credential shape the interop API accepts. Closing that gap
for real means Trino and Nessie authenticating to GCS through its native
JSON API (`fs.gs.enabled`, GSA-based auth via Workload Identity's ambient
credential chain) instead of the S3-compatible one -- a genuinely
different connector, not a different key plugged into the same one.

That's the first time this repo has needed `_template/trino.tf` or
`platform/catalog.tf` to configure a *different storage connector* per
provider, rather than a different credential inside an otherwise-identical
one -- exactly the branch CONTRACT.md's opening line rules out: "Nothing
in `terraform/platform/` or `terraform/tenants/` is allowed to... branch
on which provider produced these values." The fix isn't an exception to
that rule. It's the same fix "Known gap" and "Native federation (Option
A)" above already used for identity -- `service_account_annotations` --
applied a second time, to storage properties instead of ServiceAccount
annotations.

**The split.** `_template/trino.tf`'s `iceberg_catalog_properties` (and
`platform/catalog.tf`'s equivalent Nessie config) is really two kinds of
line, and they've just never needed to be told apart before:

- **Protocol-independent, about the REST catalog itself, not about
  storage.** `connector.name=iceberg`, `iceberg.catalog.type=rest`,
  `iceberg.rest-catalog.uri=...`, `iceberg.rest-catalog.warehouse=...`,
  `iceberg.file-format=PARQUET` -- five lines, identical on every provider
  today and identical under either storage connector, because they
  describe Trino's relationship to Nessie, not to object storage. These
  stay exactly where they are, owned by `_template`/`catalog.tf` directly,
  the same way the REST-catalog wiring always has been.
- **Storage-protocol-specific.** Everything that says how to reach and
  authenticate to the object store itself -- `fs.s3.enabled`/`s3.region`/
  `s3.aws-*-key`/`s3.endpoint`/`s3.path-style-access` today,
  `fs.gs.enabled`/`fs.gs.project-id`/`fs.gs.auth.type` on the day GCP
  actually needs it. This is the part that would have to branch on
  provider, so it's the part that moves.

**The output.** A sixth contract output,
`object_storage_catalog_properties` -- a single opaque string, mandatory
the same way `object_storage_endpoint` is (every provider must produce
one; there's no bare-metal-style empty-map fallback, since every provider
needs *some* storage connector configured), built entirely inside that
provider's own module, using nothing `_template` or `platform` provides.
`_template/trino.tf` and `platform/catalog.tf` interpolate it verbatim
into the block their own five universal lines already sit next to, and
never parse or branch on its contents -- the identical discipline
`service_account_annotations` already established, extended from "which
ServiceAccount annotation" to "which storage-connector properties."

**Why this changes nothing today.** Every provider module --
`aws/outputs.tf` (both `static-secret` and `irsa`), `gcp/outputs.tf`,
`azure/outputs.tf`, `baremetal/outputs.tf` -- would emit exactly the
string `_template/trino.tf` already hardcodes today: `fs.s3.enabled=true`
/ `s3.region=us-east-1` / (the `s3.aws-*-key` lines, or not, per that
provider's own `contains(["pod-identity", "irsa"], ...)` check, which
moves into the provider module along with everything else) /
`s3.endpoint=<that provider's own object_storage_endpoint>` /
`s3.path-style-access=true`. Relocating ownership of a block that's
identical across every provider today isn't itself the fix for GCP -- it's
what makes the eventual GCP-specific fix a one-file change instead of a
`_template`-level branch. The day Trino/Nessie's GCS work actually lands,
only `gcp/outputs.tf`'s own emitted string changes, to whatever `fs.gs.*`
properties that connector needs; `aws/outputs.tf`, `azure/outputs.tf`, and
`baremetal/outputs.tf` are untouched, and `_template/trino.tf`/
`platform/catalog.tf` don't change at all, because both already treat the
value as opaque.

**Status: design only, not yet implemented on any provider.**
`iceberg_catalog_properties` in `_template/trino.tf` and its counterpart
in `platform/catalog.tf` still build the full properties block themselves
today, S3-shaped, unconditionally. This section records the resolved
shape for `docs/testing-platform-tenant.md`'s GCP fork -- "worth a
CONTRACT.md discussion before patching around it locally" -- not a change
that's landed. Building it is mechanical (move existing lines from two
`_template`/`platform` locals into four provider `outputs.tf` files, add
one new output declaration to each, update two `lookup()`/interpolation
call sites) but genuinely not done, and GCP's actual native-GCS connector
work -- the thing that would ever make this output's GCP copy diverge from
the other three -- hasn't started either. Until both land, this output
would exist, be mandatory, and be identical across every provider, same as
`workload_identity_mechanism` was before Option A gave it somewhere to
diverge.

## What's deliberately NOT in the contract

Anything AWS/GCP/Azure/bare-metal-specific that only the provider module
itself needs — VPC CIDRs, IAM policy documents, the exact cluster
resource type (`aws_eks_cluster` vs `google_container_cluster` vs
`azurerm_kubernetes_cluster` vs a `null_resource` running a k3s install
script) — stays inside that provider's own directory and is never an
input to anything else.

## Current implementations

| Provider | Real or contract-test double? | Backend |
|---|---|---|
| `baremetal/` | Real | k3s installed over SSH onto the actual box |
| `aws/` | Contract-test double | [floci](https://floci.io)'s real EKS API (`aws_eks_cluster`/`aws_eks_node_group`), not `kind` directly — see `docs/architecture.md` for the history of that decision and why it changed. Needs a floci build that includes floci-io/floci#3783 (a `nightly` image as of this writing, not yet in a stable release — see "Native federation (Option A)" above). `workload_identity_mechanism = "static-secret"` by default, `"irsa"` once `var.enable_native_workload_identity` is set (default mechanism as of 2026-09-24, see `aws/variables.tf`'s `aws_workload_identity_mechanism`) — apply-tested clean against Nessie (real, short-lived IRSA credentials delivered to a running pod, hand-wired token volume + `AWS_ENDPOINT_URL_STS`, no static secret in state). `"pod-identity"` is still available, gated off by default: Terraform/IAM-side wiring confirmed correct, but its node-side credential relay is confirmed BLOCKED on the current floci build (re-confirmed 2026-09-24 against a known-clean cluster, same "No supported package manager found for IMDS proxy dependencies" failure, `eks-pod-identity-agent` addon pod not running) |
| `gcp/` | Contract-test double | [floci-gcp](https://floci.io/gcp/) — its GKE emulation is documented as backed by a real local k3s cluster, so this module reads the kubeconfig floci-gcp hands back rather than standing up a separate `kind`/`k3s` cluster itself. Native federation (`workload-identity.tf`) reviewed against the AWS IRSA pattern (2026-09-24, code review only — no apply chain has run against `floci-gcp` yet, see `docs/lakehouse-series` article 3): the per-tenant GSA/binding/IAM-Condition HCL is sound, but currently inert — `trino.tf`/`catalog.tf` talk to GCS only through the S3-compatible interop API (static HMAC key), which nothing Workload-Identity-issued can authenticate to, so Option A changes nothing about which credential is actually used until Trino/Nessie gain a native GCS connector. See `workload-identity.tf`'s own header comment |
| `azure/` | Contract-test double | [floci-az](https://floci.io/az/) — same pattern as `gcp/`, verify the exact kubeconfig-retrieval mechanism empirically when you first `tofu apply` this, since floci-az's docs describe AKS coverage without spelling out that mechanism |

"Contract-test double" means: these prove the Terraform HCL is valid
against that cloud's real API shape, and prove the four-output contract
holds identically across three different providers, all for free and
before you own a single real cloud account. They do not prove real
EKS/GKE/AKS runtime behavior (their actual storage-class CSI driver,
their real load-balancer controller, IRSA/Workload Identity completing a
token exchange against real STS/Entra) — budget one real pass per cloud
against the genuine managed service before calling any of this
production-trusted.

