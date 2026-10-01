# Testing platform and tenant

A consolidated verification pass for anything running above the provider
layer -- see `terraform/providers/CONTRACT.md`. Run this after
`scripts/04-apply-platform.sh` and `scripts/05-apply-tenant.sh` both report
success.

Split below by provider (`aws`, `gcp`, `azure`; `baremetal` follows AWS's
section verbatim -- it also implements the full `object_storage_*`
contract), because right now that split tells you something real: AWS is
fully wired and every step below was verified live against it; Azure isn't
wired up at the provider layer at all yet, so platform/tenant can't be
applied against it today; GCP is in between -- its `object_storage_*`
outputs exist now, but whether they actually work has never been checked
against a real apply. Each section says exactly which of those is true
instead of describing steps that were never run.

**Steps 1-4 and 6 are identical across providers by design** -- they
exercise platform/tenant's own contract (`terraform/providers/CONTRACT.md`),
not anything provider-specific, so they're written out in full once, under
AWS, and every other provider's section just points back at them rather
than duplicating the text. If one of those steps ever needs a
provider-specific tweak, that's the contract leaking and worth fixing at
the source (or at minimum applying identically to every provider's copy),
not silently forking this doc. Step 5 (object storage) is the one
deliberate exception -- AWS's contract-test double exposes an
S3-compatible API, and Azure Blob's native API is a genuinely different
shape, so that step was never going to be shareable text even once GCP and
Azure catch up.

Every AWS step here was worked out empirically, including two real dead
ends (wrong Nessie REST paths, a misdiagnosed emulator race) -- the
corrected version is what's below; the wrong turns aren't repeated here,
only the destination.

## AWS (and bare metal)

### 0. Prerequisites

```bash
export KUBECONFIG=$(cd terraform/providers/aws && tofu output -raw kubeconfig_path)
```

For bare metal, replace `aws` with `baremetal`. Every other command below
assumes this is already exported.

### 1. Pod health sweep

Quick, provider-agnostic first pass before checking anything specific:

```bash
kubectl get pods -A | grep -v Running
```

Should return nothing, or only completed Jobs (e.g. a pool-tier tenant's
schema-creation Job shows `0/1 Completed`, which is correct, not a
failure). Anything else needs a look before continuing.

**Known false positive, not platform/tenant's fault**: on floci-gcp,
`prometheus-node-exporter` sits in `CreateContainerError` -- a DaemonSet
hostPath/privileged-mount issue specific to the nested k3s environment,
unrelated to anything in this doc. Confirm it's *that* pod specifically
before assuming a real problem. (Listed here because it was first hit on
GCP's nested k3s box, not because GCP is otherwise in scope yet -- see the
GCP section below.)

### 2. Nessie's catalog actually reachable

Not just the pod Running -- the REST API itself, and specifically the
Iceberg REST catalog surface Trino depends on.

```bash
kubectl port-forward -n platform svc/nessie 19120:19120 &
PF_PID=$!
sleep 3
curl -s "http://localhost:19120/iceberg/v1/config?warehouse=tenant-a" | python3 -m json.tool
kill $PF_PID
```

**Path gotcha, confirmed the hard way -- supersedes this doc's own
previous guidance below**: the Iceberg REST *config* endpoint is not
ref-scoped in the path at all. It's `/iceberg/v1/config`, with the
warehouse selected via a `?warehouse=<name>` query parameter, matching
one of the `catalog.iceberg.warehouses[].name` entries
`terraform/platform/catalog.tf` configures (`default`, `tenant-a`, ...).
The previously-documented `/iceberg/v1/main/config` 404s silently with an
empty body -- confirmed live against a running Nessie 0.108.4's own
access log (`GET /iceberg/v1/main/config HTTP/1.1" 404 -`) sitting right
next to earlier *successful* traffic through the correct path, so a 404
here does not mean "catalog not applied yet"; check the path first.

The config response's `defaults.prefix` field is what every other
endpoint actually needs, and it is **not** just the branch name --
Nessie's multi-warehouse convention (added along with `catalog.tf`'s
`warehouses` list) combines branch and warehouse as `{branch}|{warehouse}`
(`main|tenant-a` for the example above). Nessie returns this field
**already URL-encoded** -- the literal JSON string is `main%7Ctenant-a`,
not a raw pipe character -- confirmed live against Nessie's own access
log, which recorded exactly that encoded form in real Trino/dbt traffic.
Pull it out and use it verbatim; guessing `main` alone (this doc's old
advice) 404s, and re-encoding an already-encoded value double-encodes the
`%` itself (`main%257Ctenant-a`) and 400s with "Reference name must start
with a letter..." -- confirmed live, both real mistakes, don't repeat
either one:

```bash
kubectl port-forward -n platform svc/nessie 19120:19120 &
PF_PID=$!
sleep 3
PREFIX=$(curl -s "http://localhost:19120/iceberg/v1/config?warehouse=tenant-a" \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['defaults']['prefix'])")
echo "prefix: $PREFIX"   # main%7Ctenant-a, for the tenant-a warehouse on the main branch -- already encoded, do not re-encode
curl -s "http://localhost:19120/iceberg/v1/${PREFIX}/namespaces" | python3 -m json.tool
curl -s "http://localhost:19120/iceberg/v1/${PREFIX}/namespaces/default/tables" | python3 -m json.tool
kill $PF_PID
```

### 3. A real flow, verified by querying the table directly

Never trust Kestra's own execution-status badge as proof -- query the
actual table:

```bash
kubectl get svc -n tenant-a   # confirm the real service/deployment names -- don't assume they match another provider's naming
kubectl exec -it -n tenant-a deploy/trino-coordinator -- trino --catalog iceberg --schema default --execute "SHOW TABLES;"
kubectl exec -it -n tenant-a deploy/trino-coordinator -- trino --catalog iceberg --schema default --execute "SELECT count(*) FROM <table>;"
```

A table appearing in `SHOW TABLES` proves the catalog commit succeeded --
it does **not** prove rows exist. Check the count, and if you need to go
deeper, pull the table's full metadata directly from Nessie. Reuse the
`$PREFIX` from step 2 (`main%7C<tenant_id>`, not just `main` -- see that
step's path gotcha):

```bash
kubectl port-forward -n platform svc/nessie 19120:19120 &
PF_PID=$!
sleep 3
curl -s "http://localhost:19120/iceberg/v1/${PREFIX}/namespaces/default/tables/<table>" | python3 -m json.tool
kill $PF_PID
```

The `snapshots[].summary` block tells you what actually happened on the
last write -- `total-records`, `total-data-files`, and `manifests-created`
being `"0"` means a snapshot committed successfully but wrote no data,
which is a materially weaker claim than "a real row landed," even though
`SHOW TABLES` and a clean count query would both look identical either
way at a glance.

If a query against an existing table fails with a filesystem-level error
(a `NullPointerException` in `S3InputFile.length()` was the real one hit
here), that's a sign the object-storage layer and the catalog have
drifted -- go to step 5 before assuming it's a Trino or Nessie bug.

### 4. Custom image pull actually happened over the network

```bash
kubectl get pod -n platform -l app.kubernetes.io/name=kestra -o jsonpath='{.items[0].status.containerStatuses[0].imageID}'
kubectl get events -n platform | grep -i pull
```

A real `ghcr.io/...@sha256:...` digest plus a fresh `Pulling`/`Pulled`
event pair (not just an already-cached image sitting on the node) is the
same standard AWS's pipeline proof used.

### 5. Object storage actually has what the catalog thinks it has

**Corrected below -- supersedes this doc's own previous guidance, which
was never actually run against AWS.** floci (`aws_emulator_endpoint`) is a
real S3-compatible emulator, not a GCS-shaped one -- it speaks the actual
S3 REST API, not the GCS JSON API's `/storage/v1/b/<bucket>/o` shape this
doc used to point at here. That shape was only ever validated against a
different, GCS-flavored double this doc was originally written against
(see the top of this doc); it 404s or returns an S3 XML error body against
floci, which `python3 -m json.tool` then fails to parse.

Path shape isn't the only thing wrong with a bare curl here, either:
`terraform/providers/aws/s3.tf`'s `aws_s3_bucket_public_access_block` and
SSE-KMS, plus `iam.tf`'s real scoped `aws_iam_user.object_storage`, mean
this bucket rejects unauthenticated requests outright -- confirmed live,
an unsigned request 403s (`AccessDenied`) no matter which API shape you
hit it with. The request has to be SigV4-signed with the
`object_storage_access_key_id`/`object_storage_secret_access_key`
credentials, which is what the `aws` CLI does for you:

```bash
ENDPOINT=$(tofu output -raw object_storage_endpoint)    # run from terraform/providers/aws
BUCKET=$(tofu output -raw object_storage_bucket)
export AWS_ACCESS_KEY_ID=$(tofu output -raw object_storage_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(tofu output -raw object_storage_secret_access_key)
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url "$ENDPOINT" s3api list-objects-v2 --bucket "$BUCKET" | python3 -m json.tool
```

Cross-check a specific object's existence and size against a
`metadata-location` or `manifest-list` path pulled from step 3's Nessie
response -- pass it verbatim as `--key`, no URL-encoding needed (that
`%2F`-encoding advice was also leftover from the GCS-shaped path scheme
above and doesn't apply to the S3 API):

```bash
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key "<metadata-location or manifest-list path from step 3>"
```

A `ContentLength` mismatch against what Nessie's manifest reports, or an
outright 404 (`An error occurred (404) when calling the HeadObject
operation`) for an object the catalog is confident exists, is the real
signature of a storage/catalog drift bug, not something query-side
retrying will fix.

### 6. The tenant task-runner plugin's own disposable Pod

Proves per-tenant execution isolation, not just that Kestra itself runs:

```bash
kubectl get pods -n tenant-a --sort-by=.metadata.creationTimestamp | tail -5
kubectl logs -n tenant-a <that-pod-name>
```

Confirms the custom plugin created and ran a Pod *inside the tenant's own
namespace* (not platform's), running dbt against Trino and the catalog.

### 7. Tenant prefix isolation -- does a tenant's own credential actually stay inside its own prefix?

This section used to walk through pulling each tenant's Vault-minted,
prefix-scoped AWS session credential off its running Trino pod and
probing both tenants' prefixes with each one, to check whether the
credential really stayed inside its own tenant's `s3://bucket/<tenant_id>/`
path. That whole mechanism (the Vault-broker design --
`workload_identity_mechanism == "vault-broker"`, `vault_aws_secret_backend_role.tenant`'s
inline session policy) is removed from this branch; see
`terraform/providers/CONTRACT.md`'s "Native federation (Option A)"
section for what replaced it.

`terraform/tenants/tenant-b` still exists as a second, independent
tenant for exactly this kind of cross-tenant test -- apply it the same
way tenant-a was:

```bash
PROVIDER=aws TENANT=tenant-b bash scripts/05-apply-tenant.sh
```

But there's nothing meaningful to test here yet on any provider. Every
provider defaults to the one shared, bucket-wide static
`object_storage_*` credential today (`workload_identity_mechanism ==
"static-secret"`) -- both tenants share it, so there's no per-tenant
boundary to probe. Real per-tenant scoping is Option A
(`workload-identity.tf` on each provider), and on AWS specifically it
isn't even apply-testable yet without a real OIDC issuer (see
`aws/workload-identity.tf`'s own header comment) -- this section is owed
a rewrite once that's in place.

## GCP

**No longer blocked on missing outputs, but not yet apply-tested either --
treat everything below as "wired, first real `tofu apply` still owed."**
`terraform/providers/gcp/iam.tf` and `outputs.tf` now mint a
`google_storage_hmac_key` for a dedicated `object_storage` service account
and expose all four `object_storage_*` contract outputs, the same shape
AWS's `aws_iam_user.object_storage`/`aws_iam_access_key.object_storage`
promotion took. `terraform/platform` can be pointed at this provider now
without erroring on a missing variable.

That's a narrower claim than AWS gets, though, and `iam.tf`'s own header
comment on `google_storage_hmac_key` is explicit about the gap: an HMAC
key is GCS's interoperability (XML) API credential shape, a genuinely
separate API surface from the JSON API (`storage/v1/`) floci-gcp is
actually confirmed to serve (see `versions.tf`'s header comment). Nothing
here confirms floci-gcp answers S3-shaped requests against that HMAC key
at all -- a clean `tofu apply` proves the JSON API accepted the
key-management call, not that Trino's S3 filesystem client (which is what
`tenants/_template/trino.tf` actually uses against
`object_storage_endpoint`) can authenticate against it. `outputs.tf`'s
`object_storage_endpoint` is flagged the same way AWS's
`aws_emulator_pod_endpoint` was ("UNVERIFIED -- check this on first
apply"), except GCP's open question is bigger: AWS's was "is this specific
host:port reachable," GCP's is "does this API surface exist here at all."

So the real next step here isn't writing this doc's steps 1-6 for GCP --
it's a real `tofu apply` against `terraform/providers/gcp` followed by
this doc's step 5 (AWS's corrected `aws` CLI version, pointed at GCP's
`object_storage_*` outputs) as the very first thing to try, specifically
to answer that open question. Two outcomes, both worth writing down
in this doc when either happens:

- **It works** -- floci-gcp does speak the XML/interoperability API.
  Copy AWS's steps 1-6 in here verbatim (step 0's `<provider>` becomes
  `gcp`), and this section becomes as real as AWS's.
- **It doesn't** -- floci-gcp only implements the JSON API. That's not a
  doc fix, it's a design one: `tenants/_template/trino.tf` would need
  Trino's native GCS connector (`fs.gs.enabled`, GSA-based auth) instead
  of the S3 one for this provider, which is provider-specific branching
  the platform/tenant layer doesn't have today and CONTRACT.md's
  provider-agnostic contract doesn't currently accommodate cleanly --
  worth a CONTRACT.md discussion before patching around it locally.

## Azure

**Blocked at the provider layer -- platform/tenant cannot be applied
against Azure yet**, for the same reason as GCP.
`terraform/providers/azure/storage.tf`'s header comment:

> Originally a parity-only resource, same role as providers/aws/s3.tf and
> providers/gcp/storage.tf. AWS's provider module has since been promoted
> to use its equivalent bucket as the real, shared object store (see
> CONTRACT.md's "object-storage outputs" section and aws/s3.tf) -- this
> module hasn't had that same treatment yet (no object_storage_* outputs
> below), so don't point platform/tenant at Azure until it does.

Concretely: `azurerm_storage_account.parity` and
`azurerm_storage_container.parity` exist and `tofu apply` against
`terraform/providers/azure` succeeds, but again none of the
`object_storage_*` contract outputs are exposed, so `terraform/platform`
can't consume this provider yet. As with GCP, there's nothing beyond the
bare storage account to verify today.

Once that gap closes, reuse the AWS section's steps 1-6 verbatim (step
0's `<provider>` becomes `azure`). Step 5 will need real work rather than
a copy-paste, though: Azure Blob's native API is a genuinely different
shape than S3's, so don't reuse AWS's `aws` CLI-based step 5 and assume it
degrades gracefully -- write and confirm an Azure-native equivalent
explicitly when the time comes.

## What "all green" here actually proves, and doesn't

Passing every step in the AWS section proves the same thing article two's
pipeline update proved for AWS: platform and tenant, unmodified, work
against this provider's contract outputs, and a real flow can write
through Trino into Iceberg against this provider's real object storage.
It does not prove this against real managed services (still a
contract-test double underneath), and it does not prove multi-provider
operation -- both caveats article two already holds itself to, and this
doc inherits rather than restates. Azure doesn't get to claim even that
much yet: there is no green to report until its provider layer exists at
all. GCP is closer but still can't claim it either -- its provider layer
now hands back a complete contract, but "the outputs exist" and "a real
flow ran green against them" are different claims, and only the first one
is true until someone actually runs GCP's step 5 for the first time.

