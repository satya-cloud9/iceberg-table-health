#!/usr/bin/env bash
# Phase 0: sanity-check the box before installing anything.
# Prints a report and exits non-zero only on hard blockers (not enough RAM
# to run anything useful, or an architecture/OS we haven't accounted for).
set -uo pipefail

pass() { echo "  [OK]   $1"; }
warn() { echo "  [WARN] $1"; }
fail() { echo "  [FAIL] $1"; HARD_FAIL=1; }

HARD_FAIL=0

echo "=== lakehouse-anywhere preflight ==="
echo ""

# --- OS ---
if [ -f /etc/os-release ]; then
  . /etc/os-release
  echo "OS: $PRETTY_NAME"
  case "${ID:-}" in
    ubuntu)
      case "${VERSION_ID:-}" in
        22.04) pass "Ubuntu 22.04 — this is what the scripts were originally written against." ;;
        *) pass "Ubuntu ${VERSION_ID} — scripts/01-install-deps.sh detects apt automatically, should work fine." ;;
      esac
      ;;
    debian) pass "Debian — scripts/01-install-deps.sh detects apt automatically." ;;
    fedora|rhel|rocky|almalinux|centos)
      pass "${PRETTY_NAME} — scripts/01-install-deps.sh detects dnf/yum automatically."
      ;;
    *)
      warn "Unrecognized distro (${ID:-unknown}) — scripts/01-install-deps.sh only knows apt/dnf/yum. It will exit with a clear error rather than silently doing the wrong thing if yours isn't one of those."
      ;;
  esac
else
  warn "Could not detect OS (no /etc/os-release)."
fi
echo ""

# --- Architecture ---
ARCH=$(uname -m)
echo "Architecture: $ARCH"
if [ "$ARCH" = "x86_64" ]; then
  pass "x86_64 — all images/binaries in this repo target this."
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
  warn "ARM64 — most images used here (floci, MinIO, Trino, Kestra, Prometheus/Grafana/Loki/Tempo) publish multi-arch builds, but this hasn't been verified on ARM. Flag any 'no matching manifest' pull errors."
else
  fail "Unrecognized architecture: $ARCH"
fi
echo ""

# --- CPU ---
CPUS=$(nproc)
echo "vCPUs: $CPUS"
if [ "$CPUS" -ge 8 ]; then
  pass "8+ vCPUs — comfortable for the full stack."
elif [ "$CPUS" -ge 4 ]; then
  warn "4-7 vCPUs — workable, but the full stack (kind + floci + Trino + Kestra + observability) may feel sluggish under load. CPU overcommit is generally fine for this workload since services are mostly idle/bursty."
else
  fail "$CPUS vCPUs — likely too little to run kind plus the rest of the stack concurrently."
fi
echo ""

# --- RAM ---
RAM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
RAM_GB=$((RAM_MB / 1024))
echo "RAM: ${RAM_GB} GB (${RAM_MB} MB)"
if [ "$RAM_MB" -ge 15000 ]; then
  pass "15+ GB — enough for the full stack including Loki/Tempo, per the sizing in README.md."
elif [ "$RAM_MB" -ge 8000 ]; then
  warn "8-15 GB — the full stack will be tight. See 'Trimming the footprint' in README.md (drop Loki/Tempo, cap Trino's heap)."
else
  fail "${RAM_GB} GB — below Kestra's own documented minimum (4 GB) once you account for the OS, Docker, and everything else. You'll need a bigger box."
fi
echo ""

# --- Disk ---
DISK_AVAIL_GB=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
echo "Disk available on /: ${DISK_AVAIL_GB} GB"
if [ "$DISK_AVAIL_GB" -ge 40 ]; then
  pass "40+ GB free — plenty for container images + Terraform state + Iceberg test data."
elif [ "$DISK_AVAIL_GB" -ge 20 ]; then
  warn "20-40 GB free — should be enough, but keep an eye on 'docker system df' as images accumulate."
else
  fail "${DISK_AVAIL_GB} GB free — likely not enough once you've pulled the floci, MinIO, Trino, Kestra, and observability images (that's typically 5-8 GB of images alone)."
fi
echo ""

# --- Docker present? ---
if command -v docker >/dev/null 2>&1; then
  DOCKER_VER=$(docker --version 2>/dev/null || echo "unknown")
  pass "Docker CLI present: $DOCKER_VER"
  if docker info >/dev/null 2>&1; then
    pass "Docker daemon is reachable."
  else
    warn "Docker CLI present but daemon not reachable — scripts/01-install-deps.sh will (re)install/start it."
  fi
else
  warn "Docker not found — scripts/01-install-deps.sh will install it."
fi
echo ""

# --- Outbound network check (the thing that broke in the sandbox this was built in) ---
echo "Checking outbound access to the registries/APIs we need..."
# registry-1.docker.io covers both floci/floci and every other image this
# stack pulls (Trino, Kestra, MinIO, Prometheus, Grafana, Loki, Tempo).
for host in "registry-1.docker.io" "get.opentofu.org" "dl.k8s.io" "github.com"; do
  if timeout 5 bash -c "cat < /dev/null > /dev/tcp/${host}/443" 2>/dev/null; then
    pass "Can reach $host:443"
  else
    fail "Cannot reach $host:443 — this box has the same kind of network restriction the build sandbox did. Everything downstream will fail until this is fixed."
  fi
done
echo ""

echo "=== Summary ==="
if [ "$HARD_FAIL" -eq 1 ]; then
  echo "One or more hard failures above. Fix those before continuing, or paste this output back for help sizing/adjusting."
  exit 1
else
  echo "No hard blockers. Warnings above are worth reading but not blocking. Run: make install"
fi
