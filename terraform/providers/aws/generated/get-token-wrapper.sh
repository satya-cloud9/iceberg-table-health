#!/usr/bin/env bash
# kubectl exec-credential wrapper around `aws eks get-token`, installed
# into the kubeconfig by cluster.tf's null_resource.kubeconfig instead of
# calling `aws` directly. Root cause this exists for, confirmed against
# real apply output, not assumed: `aws eks get-token`'s ExecCredential
# reports its own token as valid for a long stretch (mirroring real
# aws-iam-authenticator's ~15-minute convention), so client-go caches and
# reuses the SAME SigV4-signed token for the whole life of a
# `terraform apply` rather than re-minting it per call. floci's actual
# enforcement window is evidently shorter than that self-reported
# validity (floci's own docs claim 15 minutes; floci-io/floci#2912's own
# description separately mentions a 60-second expiry check -- the two
# don't agree, and only live behavior settled it): a real apply against
# terraform/platform showed early resources (namespaces, two Deployments)
# succeed, then Unauthorized starts appearing specifically on resources
# declared after two slow Helm installs (kube_prometheus_stack, loki) --
# i.e. resources reached late enough into the apply's wall-clock time,
# not any particular resource type. That's a token-staleness signature,
# not an identity/RBAC one.
#
# Fix: don't trust aws eks get-token's own expirationTimestamp at all.
# Call it for a real, validly-signed token (nothing about the signature
# itself changes), then overwrite the ExecCredential's expiry to a few
# seconds out so client-go is forced to re-invoke this wrapper -- and
# mint a freshly-signed SigV4 request -- well inside whatever floci's
# real window turns out to be, instead of once per apply.
#
# Usage (matches how cluster.tf's null_resource.kubeconfig configures the
# kubeconfig's exec block): $1 is the cluster name; AWS_ENDPOINT_URL,
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY come from the exec block's own
# --exec-env entries, the same real (non-test/test) eks_admin identity
# used everywhere else in this directory -- see iam.tf's
# aws_iam_user.eks_admin header comment for why it has to be real.
set -euo pipefail

CLUSTER_NAME="$1"

RAW=$(aws --endpoint-url "$AWS_ENDPOINT_URL" eks get-token --cluster-name "$CLUSTER_NAME" --output json)

# GNU date (Linux/WSL2, the expected environment here) first; BSD date
# (macOS) as a fallback if that fails. 30s is a deliberately conservative
# guess given floci's own docs and PR description disagree on the real
# number (15 minutes vs. 60 seconds) -- comfortably under either one.
# Tune down further only if you've confirmed (not guessed) floci's actual
# window is tighter than this.
NEW_EXPIRY=$(date -u -d '+30 seconds' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+30S +%Y-%m-%dT%H:%M:%SZ)

echo "$RAW" | jq --arg exp "$NEW_EXPIRY" '.status.expirationTimestamp = $exp'
