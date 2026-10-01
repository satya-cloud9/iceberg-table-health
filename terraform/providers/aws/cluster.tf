# The cluster itself, via floci's real EKS API (aws_eks_cluster +
# aws_eks_node_group), not `kind` directly. This used to be plain `kind`
# CLI (see versions.tf's header comment for why, and why that reasoning
# stopped holding once floci's EKS emulation gained real OIDC-signed
# service-account tokens and EKS Pod Identity support -- confirmed
# empirically, not assumed, against a `nightly` floci image: see
# docker-compose.floci.yml's header comment). Node groups and cluster
# lifecycle are real, non-metadata resources in floci's EKS emulation (its
# own docs: "store every member, so CreateNodegroup and DescribeNodegroup
# both return exactly what was supplied") -- reviewed, not yet apply-tested
# end to end in this repo.
#
# aws_eks_cluster.this below uses provider = aws.eks_admin, not the default
# test/test provider every other resource in this directory uses. That's
# not cosmetic -- see iam.tf's aws_iam_user.eks_admin header comment for
# the root-caused reason: floci-io/floci#2912 hardened the EKS
# token-authentication webhook to reject the test/test credential pair
# specifically, so bootstrap_cluster_creator_admin_permissions below has to
# be granted to a real identity or it grants cluster-admin to nobody the
# webhook will ever authenticate.

resource "aws_eks_cluster" "this" {
  provider = aws.eks_admin
  name     = var.cluster_name
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.kubernetes_version

  vpc_config {
    subnet_ids = concat(aws_subnet.public[*].id, aws_subnet.private[*].id)
  }

  # An earlier version of this block also set authentication_mode =
  # API_AND_CONFIG_MAP and paired it with explicit aws_eks_access_entry /
  # aws_eks_access_policy_association resources, on the theory that
  # floci's CONFIG_MAP-only default might not actually replicate real
  # EKS's aws-auth-ConfigMap auto-bootstrap. Both pieces of that turned
  # out to be dead ends against this floci build, confirmed empirically,
  # not assumed:
  #   - authentication_mode is only changeable via UpdateClusterConfig,
  #     which floci doesn't implement at all (404 UnknownOperationException)
  #     -- meaning it has to be right at CreateCluster time or you're stuck
  #     rebuilding the cluster to change it, same as the value below.
  #   - The identity behind the test/test credentials resolves to the bare
  #     account root ARN in floci's STS emulation (no real IAM user/role
  #     backs those credentials) -- and EKS correctly refuses to create an
  #     access entry for the root ARN, that's real AWS behavior, not a
  #     floci bug. aws_eks_access_policy_association also just 404s
  #     outright (AssociateAccessPolicy isn't implemented), so even a
  #     valid access entry couldn't have been granted a policy this way.
  #
  # Back to relying on bootstrap_cluster_creator_admin_permissions alone,
  # with authentication_mode left unset (floci's own default, confirmed
  # via an earlier probe cluster: CONFIG_MAP).
  #
  # UPDATE, root cause found: the 401s that motivated the
  # authentication_mode/access-entry detour above, and that persisted even
  # after reverting to this simpler block, were NOT an access_config
  # problem at all -- confirmed via floci's own docs and source
  # (floci-io/floci#2912): its EKS token-authentication webhook
  # deliberately, unconditionally rejects the test/test credential pair
  # ("because the webhook grants cluster-admin access"). The cluster
  # creator and the `aws eks get-token` exec-plugin caller being "the same
  # identity" was never in question -- the problem was that shared
  # identity was test/test, which floci's webhook refuses regardless of
  # what access_config says. Fixed by making that identity real: see
  # `provider = aws.eks_admin` above and iam.tf's aws_iam_user.eks_admin
  # header comment.
  access_config {
    bootstrap_cluster_creator_admin_permissions = true
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
  ]
}

resource "aws_eks_node_group" "workers" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-workers"
  node_role_arn   = aws_iam_role.eks_node.arn
  # Private subnets -- matches the real-EKS convention the vpc.tf comments
  # already assume (the public subnets are tagged kubernetes.io/role/elb
  # for a load-balancer controller, not node placement).
  subnet_ids = aws_subnet.private[*].id

  scaling_config {
    desired_size = var.node_group_desired_size
    min_size     = 1
    max_size     = var.node_group_desired_size + 1
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_node_worker_policy,
    aws_iam_role_policy_attachment.eks_node_cni_policy,
    aws_iam_role_policy_attachment.eks_node_ecr_policy,
  ]
}

# kubeconfig generation, and two floci-emulator-only patches on top of it --
# neither needed against real EKS, both UNVERIFIED against floci (reviewed,
# not yet apply-tested):
#
# 1. The exec-based auth plugin `aws eks update-kubeconfig` writes calls
#    `aws eks get-token` with no way to tell it (via any update-kubeconfig
#    flag) to talk to floci instead of real AWS STS -- so this patches
#    AWS_ENDPOINT_URL (plus the same test/test credentials the provider
#    block in versions.tf uses) into that exec entry's own environment via
#    `kubectl config set-credentials --exec-env`, rather than assuming
#    whatever shell later runs `tofu apply`/`kubectl` already has those set.
# 2. The probe that confirmed OIDC support also showed
#    `certificateAuthority.data` coming back as "" from DescribeCluster --
#    no real CA to trust yet as of that test (run without
#    FLOCI_TLS_ENABLED). This unconditionally sets
#    insecure-skip-tls-verify=true rather than assuming FLOCI_TLS_ENABLED=true
#    (docker-compose.floci.yml) actually populates real CA data once it's on.
#    Confirm on first apply whether that assumption was even necessary --
#    if floci now returns a real CA, this patch is just redundant, not wrong.
resource "null_resource" "kubeconfig" {
  # Deliberately re-runs on every apply (always_run = timestamp()), not
  # just once. An earlier version triggered only on var.cluster_name/
  # var.aws_emulator_endpoint -- both static config values that don't
  # change across a floci container restart, an image-tag swap
  # (docker-compose.floci.yml), or floci losing track of a cluster's live
  # state while its own metadata still claims the cluster exists. Any of
  # those can leave the on-disk kubeconfig stale (missing the
  # insecure-skip-tls-verify patch below, or pointed at a k3s API server
  # port that no longer matches what's actually running) with nothing to
  # tell Terraform to regenerate it. Confirmed this happened in practice --
  # not a hypothetical. Re-running `aws eks update-kubeconfig` + the
  # `kubectl config` patches every apply is cheap and idempotent, so the
  # safety margin is worth the few extra seconds per apply.
  # Expected side effect: `tofu plan` will always show this resource as
  # needing replacement (timestamp() changes every run) -- that's the
  # trade-off for "always re-run this," a standard Terraform idiom, not a
  # sign anything's wrong. The provisioner itself is idempotent either way.
  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      mkdir -p "${path.module}/generated"
      exec >"${path.module}/generated/debug.log" 2>&1
      set -x
      # Real (non-test/test) identity, not the shared provider default --
      # see iam.tf's aws_iam_user.eks_admin header comment. This has to
      # match the identity that created the cluster (aws_eks_cluster.this's
      # provider = aws.eks_admin above), since that's the one
      # bootstrap_cluster_creator_admin_permissions actually granted
      # cluster-admin to.
      export AWS_ACCESS_KEY_ID="${var.eks_admin_access_key_id}"
      export AWS_SECRET_ACCESS_KEY="${var.eks_admin_secret_access_key}"
      export AWS_DEFAULT_REGION="${var.aws_region}"

      #mkdir -p "${path.module}/generated"
      KUBECONFIG_PATH="${path.module}/generated/kubeconfig"

      aws --endpoint-url "${var.aws_emulator_endpoint}" eks update-kubeconfig \
        --name "${var.cluster_name}" --alias "${var.cluster_name}" \
        --kubeconfig "$KUBECONFIG_PATH"
      # Point the exec plugin at get-token-wrapper.sh instead of `aws`
      # directly -- see that script's own header comment for the full
      # story (token-staleness confirmed against a real terraform/platform
      # apply: early resources fine, resources reached later in wall-clock
      # time failing with genuine 401s regardless of resource type). Same
      # underlying auth (still literally `aws eks get-token`, same
      # eks_admin identity), just with the returned token's self-reported
      # expiry clamped down so client-go refreshes far more often.
      cp "${path.module}/get-token-wrapper.sh" "${path.module}/generated/get-token-wrapper.sh"
      chmod +x "${path.module}/generated/get-token-wrapper.sh"
      WRAPPER_ABS_PATH="$(cd "${path.module}/generated" && pwd)/get-token-wrapper.sh"

      # Patch the kubeconfig directly as JSON via jq, NOT via
      # `kubectl config set-cluster`/`set-credentials`. Root-caused via a
      # scoped `set -x` trace (piped to this same debug.log, since
      # Terraform blanket-redacts provisioner stdout/stderr whenever a
      # sensitive-marked value is in scope -- see the comment at the top
      # of this heredoc): `kubectl config set-credentials
      # --exec-command=<absolute path>` was confirmed to receive the
      # correct, fully-expanded absolute path as a single argv token --
      # no bash/Terraform quoting or interpolation bug on our end -- and
      # still silently wrote only the basename into
      # users[].user.exec.command. That's a real bug/quirk in whatever
      # kubectl build this runs against, not anything in this script.
      # Bypassing that flag entirely sidesteps it: a kubeconfig file is
      # loaded as YAML, and valid JSON is also valid YAML, so writing the
      # file back as jq-produced JSON is something kubectl (or any other
      # YAML-based client) reads identically to hand-written YAML -- no
      # new dependency either, jq is already required throughout these
      # scripts. Also replaces the earlier separate
      # `set-cluster --insecure-skip-tls-verify` /
      # `unset certificate-authority-data` calls with the same jq pass.
      kubectl config view --kubeconfig "$KUBECONFIG_PATH" --raw -o json \
        | jq \
            --arg wrapper "$WRAPPER_ABS_PATH" \
            --arg cluster_name "${var.cluster_name}" \
            --arg endpoint "${var.aws_emulator_endpoint}" \
            --arg akid "${var.eks_admin_access_key_id}" \
            --arg secret "${var.eks_admin_secret_access_key}" \
            --arg region "${var.aws_region}" \
            '.clusters[0].cluster["insecure-skip-tls-verify"] = true
             | del(.clusters[0].cluster["certificate-authority-data"])
             | .users[0].user = {
                 exec: {
                   apiVersion: "client.authentication.k8s.io/v1beta1",
                   command: $wrapper,
                   args: [$cluster_name],
                   env: [
                     {name: "AWS_ENDPOINT_URL", value: $endpoint},
                     {name: "AWS_ACCESS_KEY_ID", value: $akid},
                     {name: "AWS_SECRET_ACCESS_KEY", value: $secret},
                     {name: "AWS_REGION", value: $region},
                     {name: "AWS_DEFAULT_REGION", value: $region}
                   ]
                 }
               }' \
        > "$KUBECONFIG_PATH.tmp"
      mv "$KUBECONFIG_PATH.tmp" "$KUBECONFIG_PATH"

      # TEMPORARY: read straight back, in this same process, immediately
      # after the write above -- see the "set -x" comment near the top of
      # this heredoc. If this still prints the bare filename, the bug is in
      # this write itself (WRAPPER_ABS_PATH or kubectl's handling of it);
      # if THIS prints the correct absolute path but a later `kubectl get`
      # (03b-verify-provider.sh, or a plain manual check) still shows the
      # bare filename, something after this provisioner is the culprit
      # instead (a second writer, or a stale file being read).
      echo "DEBUG WRAPPER_ABS_PATH=[$WRAPPER_ABS_PATH]"
      echo "DEBUG exec.command immediately after write:"
      kubectl config view --kubeconfig "$KUBECONFIG_PATH" --raw -o jsonpath='{.users[0].user.exec.command}'
      echo ""

    EOT
  }

  depends_on = [
    aws_eks_cluster.this,
    aws_eks_node_group.workers,
  ]
}

