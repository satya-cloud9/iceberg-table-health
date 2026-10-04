#!/usr/bin/env bash
# Tears down the emulated AWS provider (Floci + emulated EKS), then stops
# the emulator container if one is running. Leaves Terraform state and everything in git untouched
# by default -- pass DESTROY=1 to actually run `tofu destroy` at each
# stage instead of just stopping the emulator/local cluster.
#
# Usage: DESTROY=1 WIPE_STATE=1 bash scripts/99-teardown.sh
#
# WIPE_STATE=1: on top of stopping the emulator, also deletes the local
# Terraform state for the provider layer (.terraform/, terraform.tfstate*, generated/, *.auto.tfvars.json)
# so the next 02/03 script run starts genuinely from scratch. This is a
# DIFFERENT thing from DESTROY=1 and the two answer different situations,
# don't reach for both together without thinking about why:
#   - DESTROY=1 assumes the emulator/cluster is still alive and healthy
#     enough for `tofu destroy` to talk to it and clean up remotely.
#   - WIPE_STATE=1 assumes the opposite -- the emulator/cluster state is
#     already suspect or being thrown away anyway (this is exactly what a
#     manual "docker compose down -v" + `rm -rf .terraform terraform.tfstate*
#     generated/` sequence had to be run by hand for, more than once, during
#     this project's AWS/floci debugging -- see the IRSA workload-identity
#     work and its "stale/duplicate node registration" incidents). Trying to
#     `tofu destroy` against a cluster you're about to nuke either hangs
#     waiting on a dead endpoint or is pointless work -- WIPE_STATE=1 skips
#     straight to deleting the local state instead, on the reasoning that
#     the emulator itself is disposable dev infra with no real resources to
#     clean up behind it.
set -uo pipefail

cd "$(dirname "$0")/.."

PROVIDER="${PROVIDER:-aws}"
DESTROY="${DESTROY:-0}"
WIPE_STATE="${WIPE_STATE:-0}"

if [ "$DESTROY" = "1" ] && [ "$WIPE_STATE" = "1" ]; then
  echo "WARNING: both DESTROY=1 and WIPE_STATE=1 are set. Running 'tofu destroy'" >&2
  echo "against infra you're about to wipe the state for is usually not what you" >&2
  echo "want (see this script's header comment). Continuing anyway -- destroy" >&2
  echo "runs first, then the wipe, in case that combination really is intended." >&2
  echo "" >&2
fi

# Removes any container still attached to a docker-compose project's default
# network, then removes the network itself. Needed because floci spins some
# of its own containers up directly (e.g. its ECR-emulation helper,
# floci-ecr-registry) rather than through docker compose, so they carry none
# of compose's own labels -- `docker compose down`, even with
# --remove-orphans, does not know about them and leaves them attached,
# which blocks the network from ever being removed ("Resource is still in
# use") and leaves exactly the kind of leftover state that caused this
# project's stale/duplicate k3s node registration bug to recur after a
# plain restart instead of a real teardown. --remove-orphans only catches
# containers compose itself once created and later stopped tracking; it
# does not catch containers a *different* process created on the same
# network, which is what this actually is.
cleanup_compose_network() {
  local compose_file="$1"
  local project network leftover name

  project="$(docker compose -f "$compose_file" config --format json 2>/dev/null | jq -r '.name // empty' 2>/dev/null)"
  if [ -z "$project" ]; then
    # Fallback: docker compose's own default project-name algorithm is
    # "basename of the compose file's directory, lowercased, with anything
    # that isn't [a-z0-9_-] turned into nothing". Approximated here for the
    # case `docker compose config` itself isn't available/working (e.g. jq
    # missing) -- if this fallback ever produces the wrong name, the network
    # inspect below just no-ops (network won't exist under that name) and
    # this function silently does nothing, so it's safe to have wrong.
    project="$(basename "$(pwd)" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '-')"
  fi
  network="${project}_default"

  if ! docker network inspect "$network" >/dev/null 2>&1; then
    return 0
  fi

  leftover="$(docker network inspect "$network" --format '{{range $id, $c := .Containers}}{{$c.Name}} {{end}}' 2>/dev/null)"
  if [ -n "$leftover" ]; then
    echo "  Found leftover container(s) still attached to '$network' that" \
         "docker compose down didn't remove: $leftover"
    for name in $leftover; do
      echo "  Force-removing $name..."
      docker rm -f "$name" >/dev/null 2>&1 || true
    done
  fi

  if docker network rm "$network" >/dev/null 2>&1; then
    echo "  Network '$network' removed."
  elif docker network inspect "$network" >/dev/null 2>&1; then
    echo "  WARNING: network '$network' still exists after cleanup -- something" >&2
    echo "  else is still attached. Run 'docker network inspect $network' to see" >&2
    echo "  what, and remove it by hand before rebuilding, or the rebuild may" >&2
    echo "  inherit stale state again." >&2
  fi
}

# Removes any Docker volume floci created itself, outside the compose
# file's own `volumes:` block. `docker compose down -v` only removes what
# the compose file declares (floci-data) -- floci additionally creates its
# own volumes dynamically per-service/per-cluster (confirmed empirically,
# 2026-09-24: floci-ecr-registry-data for its ECR emulation, and
# floci-eks-<cluster-name> for each EKS cluster it emulates -- the latter
# is where the k3s node's own etcd/node-registration state actually lives).
# Leaving these behind is exactly what let stale/duplicate k3s node
# registrations keep surviving what looked like full teardowns, repeatedly,
# working through this project's AWS/floci IRSA debugging -- `docker
# compose down -v` alone reported success and removed floci-data every
# time, but the node that mattered was registered in a volume compose
# never knew existed. Must run AFTER cleanup_compose_network (or otherwise
# after the containers using these volumes are gone) -- Docker refuses to
# remove a volume still mounted by a running container.
cleanup_floci_volumes() {
  local vols v

  vols="$(docker volume ls -q --filter name=floci)"
  if [ -z "$vols" ]; then
    return 0
  fi

  echo "  Found floci-created volume(s) docker compose doesn't track: $vols"
  for v in $vols; do
    echo "  Removing volume $v..."
    docker volume rm "$v" >/dev/null 2>&1 \
      || echo "  WARNING: could not remove volume $v -- still in use by something? Check 'docker ps -a' for a container still holding it." >&2
  done
}

# Deletes local Terraform state for the provider (never git-tracked) so the next apply chain starts from nothing rather
# than reusing state that may reference resources on a now-gone emulator.
wipe_local_state() {
  local provider="$1"

  echo "=== Wiping local Terraform state (provider=${provider}) ==="

  echo "  terraform/providers/${provider}/{.terraform,terraform.tfstate*,generated/,*.auto.tfvars.json}"
  rm -rf \
    "terraform/providers/${provider}/.terraform" \
    "terraform/providers/${provider}"/terraform.tfstate* \
    "terraform/providers/${provider}/generated" \
    "terraform/providers/${provider}"/*.auto.tfvars.json

  echo "  terraform/generated/ (captured provider outputs; scripts/env.sh reads the kubeconfig path)"
  rm -rf terraform/generated/

  echo "  Done. Run 'make gl-up' to rebuild from scratch."
}

if [ "$DESTROY" = "1" ]; then
  echo "DESTROY=1 -- running 'tofu destroy' on terraform/providers/${PROVIDER}."
  echo "(Ctrl-C now if that's not what you meant.)"
  echo ""
  (cd "terraform/providers/${PROVIDER}" && tofu destroy -auto-approve) \
    || echo "(destroy failed or nothing to destroy -- continuing)"
else
  echo "DESTROY not set -- leaving Terraform state alone (this only stops the"
  echo "local emulator and cluster below). Re-run with DESTROY=1 to tear down"
  echo "the provider resources too."
fi

echo ""
echo "=== Stopping floci ==="
  # -v --remove-orphans, not a bare `down`: leaving floci's own persisted
  # volume in place across a restart is exactly what let its embedded k3s
  # node's stale/duplicate registration survive a "just restart it" cycle
  # instead of actually resetting -- confirmed the hard way more than once
  # working through the IRSA/workload-identity path on this provider.
  # There's no real data worth preserving in a disposable dev emulator, so
  # -v is the default here now, not opt-in.
  #
  # No `2>/dev/null` here on purpose (removed 2026-09-24) -- it was
  # blanket-swallowing this command's own stderr, which would hide a real
  # partial failure (e.g. the volume removal itself failing because
  # something still had it mounted) behind the exact same "(not running)"
  # fallback text a genuinely-nothing-to-tear-down case prints. Better to
  # let the real output through and see it.
  docker compose -f docker-compose.floci.yml down -v --remove-orphans || echo "(not running, or partial failure above -- read the output)"
  cleanup_compose_network docker-compose.floci.yml
  cleanup_floci_volumes


if [ "$WIPE_STATE" = "1" ]; then
  echo ""
  wipe_local_state "$PROVIDER"
fi

echo ""
echo "Done. Terraform state and git history are untouched unless DESTROY=1 or" \
     "WIPE_STATE=1 was set."
