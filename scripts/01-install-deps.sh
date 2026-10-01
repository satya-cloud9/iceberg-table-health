#!/usr/bin/env bash
# Phase 1: install Docker, kind, kubectl, helm, OpenTofu, and awscli-local.
#
# Distro/arch-agnostic: detects the package manager (apt/dnf/yum) and CPU
# architecture (amd64/arm64) instead of assuming Ubuntu on x86_64, so the
# same script runs unmodified on any Linux box — an AWS EC2 instance, a
# DigitalOcean droplet, an Oracle Cloud Ampere (ARM) instance, bare metal,
# whatever. Nothing in this stack talks to a real cloud's API (that's the
# whole point of floci + kind), so the only things that actually vary
# across "which cloud is this VM on" are the OS package manager and the
# CPU architecture — this script handles both explicitly instead of
# silently assuming Ubuntu/apt/amd64.
#
# Idempotent — safe to re-run.
set -euo pipefail

# --- Detect package manager ---
PKG_MGR=""
if command -v apt-get >/dev/null 2>&1; then
  PKG_MGR="apt"
elif command -v dnf >/dev/null 2>&1; then
  PKG_MGR="dnf"
elif command -v yum >/dev/null 2>&1; then
  PKG_MGR="yum"
else
  echo "ERROR: no supported package manager found (looked for apt-get, dnf, yum)." >&2
  echo "This script supports Debian/Ubuntu (apt), Fedora/RHEL9+/Rocky/Alma (dnf)," >&2
  echo "and RHEL7/8/CentOS7 (yum). Alpine/SUSE aren't covered — Docker's own" >&2
  echo "install script (get.docker.com) doesn't officially support Alpine either." >&2
  exit 1
fi
echo "Detected package manager: $PKG_MGR"

# --- Detect architecture, map to the Go-style arch names these tools use ---
UNAME_M=$(uname -m)
case "$UNAME_M" in
  x86_64) GOARCH="amd64" ;;
  aarch64|arm64) GOARCH="arm64" ;;
  *)
    echo "ERROR: unrecognized architecture '$UNAME_M' — this stack targets amd64/arm64 only." >&2
    exit 1
    ;;
esac
echo "Detected architecture: $UNAME_M -> $GOARCH"
echo ""

echo "=== Installing base packages ==="
case "$PKG_MGR" in
  apt)
    sudo apt-get update -y
    sudo apt-get install -y ca-certificates curl gnupg lsb-release jq unzip apt-transport-https software-properties-common python3-pip
    ;;
  dnf)
    sudo dnf install -y ca-certificates curl gnupg2 jq unzip python3-pip
    ;;
  yum)
    sudo yum install -y ca-certificates curl gnupg2 jq unzip python3-pip
    ;;
esac

# --- Docker Engine (official convenience script — handles Debian/Ubuntu/
#     RHEL/CentOS/Fedora/SLES distro detection itself, so no branching needed here) ---
if ! command -v docker >/dev/null 2>&1; then
  echo "=== Installing Docker Engine ==="
  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
  sudo sh /tmp/get-docker.sh
  sudo usermod -aG docker "$USER"
  echo "Added $USER to the docker group — you'll need to log out/in (or run 'newgrp docker') for this to take effect without sudo."
else
  echo "Docker already installed: $(docker --version)"
fi

sudo systemctl enable docker >/dev/null 2>&1 || true
sudo systemctl start docker >/dev/null 2>&1 || true

# Docker Compose plugin (usually bundled by get-docker.sh, but double-check per package manager)
if ! docker compose version >/dev/null 2>&1; then
  echo "=== Installing docker-compose-plugin ==="
  case "$PKG_MGR" in
    apt) sudo apt-get install -y docker-compose-plugin ;;
    dnf) sudo dnf install -y docker-compose-plugin ;;
    yum) sudo yum install -y docker-compose-plugin ;;
  esac
fi

# --- kind ---
if ! command -v kind >/dev/null 2>&1; then
  echo "=== Installing kind ($GOARCH) ==="
  KIND_VERSION=$(curl -fsSL https://api.github.com/repos/kubernetes-sigs/kind/releases/latest | jq -r .tag_name 2>/dev/null || echo "")
  if [ -z "$KIND_VERSION" ] || [ "$KIND_VERSION" = "null" ]; then
    KIND_VERSION="v0.27.0"  # fallback pin — update if this is stale
    echo "Could not resolve latest kind release, falling back to $KIND_VERSION"
  fi
  curl -Lo /tmp/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${GOARCH}"
  chmod +x /tmp/kind
  sudo mv /tmp/kind /usr/local/bin/kind
  echo "Installed kind $KIND_VERSION"
else
  echo "kind already installed: $(kind version)"
fi

# --- kubectl ---
if ! command -v kubectl >/dev/null 2>&1; then
  echo "=== Installing kubectl ($GOARCH) ==="
  KUBECTL_VERSION=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
  curl -Lo /tmp/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${GOARCH}/kubectl"
  chmod +x /tmp/kubectl
  sudo mv /tmp/kubectl /usr/local/bin/kubectl
  echo "Installed kubectl $KUBECTL_VERSION"
else
  echo "kubectl already installed: $(kubectl version --client --short 2>/dev/null || kubectl version --client)"
fi

# --- helm (its own get-helm-3 script already detects OS/arch itself) ---
if ! command -v helm >/dev/null 2>&1; then
  echo "=== Installing helm ==="
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 -o /tmp/get-helm.sh
  chmod +x /tmp/get-helm.sh
  /tmp/get-helm.sh
else
  echo "helm already installed: $(helm version --short)"
fi

# --- OpenTofu (Terraform-compatible, MPL-licensed) ---
# Its install script supports --install-method deb/rpm/standalone; pick the
# one matching the detected package manager instead of assuming deb.
if ! command -v tofu >/dev/null 2>&1; then
  echo "=== Installing OpenTofu ==="
  curl -fsSL https://get.opentofu.org/install-opentofu.sh -o /tmp/install-opentofu.sh
  chmod +x /tmp/install-opentofu.sh
  case "$PKG_MGR" in
    apt) sudo /tmp/install-opentofu.sh --install-method deb ;;
    dnf|yum) sudo /tmp/install-opentofu.sh --install-method rpm ;;
  esac
else
  echo "OpenTofu already installed: $(tofu version)"
fi

# --- awslocal (awscli-local) — thin wrapper for ad-hoc `aws` calls against
#     floci (same wrapper LocalStack used, still works since floci is
#     wire-compatible). Pure Python — no arch/distro branching needed. ---
pip3 install --quiet --break-system-packages awscli-local awscli 2>&1 | tail -5 || \
  pip3 install --quiet --user awscli-local awscli 2>&1 | tail -5

echo ""
echo "=== Versions ==="
docker --version || true
docker compose version || true
kind version || true
kubectl version --client 2>/dev/null || true
helm version --short || true
tofu version || true
awslocal --version 2>/dev/null || echo "awslocal: check 'pip3 show awscli-local' if this didn't print"

echo ""
echo "If this is the first install of Docker, run 'newgrp docker' (or log out/in) before continuing to 'make emulator-up'."
