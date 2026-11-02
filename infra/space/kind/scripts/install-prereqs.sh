#!/usr/bin/env bash
# install-prereqs.sh — install the host-side tools the kind bootstrap needs.
#
# Idempotent: skips a tool that's already on PATH at the pinned version (or
# better). Downloads pinned-version static binaries from upstream releases and
# drops them under $INSTALL_DIR (default: /usr/local/bin; falls back to
# $HOME/.local/bin if /usr/local/bin isn't writable).
#
# Usage:
#   sudo bash infra/space/kind/scripts/install-prereqs.sh     # system-wide
#   bash infra/space/kind/scripts/install-prereqs.sh          # ~/.local/bin
#   INSTALL_DIR=/opt/bin bash ...                             # explicit dir
#
# Pinned versions are picked to match upstream stable as of the last update of
# this script. Bump them in a single PR (so kind/kubectl/helm stay roughly in
# sync) when you want to upgrade.
set -euo pipefail

KIND_VERSION="${KIND_VERSION:-v0.30.0}"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.32.0}"
HELM_VERSION="${HELM_VERSION:-v3.16.4}"
GRPCURL_VERSION="${GRPCURL_VERSION:-v1.9.3}"
TILT_VERSION="${TILT_VERSION:-0.37.5}"
GO_VERSION="${GO_VERSION:-1.25.3}"

# Choose install dir: prefer /usr/local/bin (system-wide) if writable, else
# fall back to ~/.local/bin (caller doesn't need root).
INSTALL_DIR="${INSTALL_DIR:-}"
if [[ -z "$INSTALL_DIR" ]]; then
  if [[ -w /usr/local/bin ]]; then
    INSTALL_DIR=/usr/local/bin
  else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
  fi
fi

case "$(uname -s)" in
  Linux)  OS=linux ;;
  Darwin) OS=darwin ;;
  *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac

log() { printf '[install-prereqs] %s\n' "$*"; }

# Prefix for commands that need root. Empty when already root, `sudo -n` when
# that works, and empty again when neither — each caller decides what to do with
# "no privilege available" rather than this probe deciding for them.
sudo_prefix() {
  [[ $EUID -eq 0 ]] && return 0
  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    printf 'sudo -n\n'
  fi
}

# Run a command with whatever privilege is available, or fail if it needs more
# than this run has.
sudo_prefix_run() {
  # shellcheck disable=SC2207
  local prefix=($(sudo_prefix))
  if [[ ${#prefix[@]} -eq 0 && $EUID -ne 0 ]]; then
    return 1
  fi
  "${prefix[@]}" "$@"
}

# Use the same prerequisite list that the smoke-suite bootstrap verifies.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
# shellcheck source=../../../../scripts/lib/space-smoke-prereqs.sh
source "$REPO_ROOT/scripts/lib/space-smoke-prereqs.sh"

want_version() {
  # want_version <bin> <expected version substring>
  command -v "$1" >/dev/null 2>&1 || return 1
  "$1" version --short 2>/dev/null | grep -q "$2" && return 0
  "$1" --version 2>/dev/null     | grep -q "$2" && return 0
  return 1
}

download() {
  # download <url> <dest>
  # GitHub release downloads (grpcurl, tilt) can answer 500 for longer than
  # fixed 2 s retries last: four attempts over ~6 s have all failed. Match
  # install-fc-vendor.sh: curl's doubling backoff spreads seven attempts over
  # about a minute, and --retry-all-errors also retries a reset connection.
  # A transfer that stalls is an error too: one attempt that averaged under
  # 10 KB/s for 30 s is dropped and retried, where a hung HTTP/2 stream once
  # held a CI shard for 39 minutes.
  curl -fsSL --retry 6 --retry-all-errors \
    --connect-timeout 20 --speed-limit 10240 --speed-time 30 "$1" -o "$2"
}

install_kind() {
  if want_version kind "$KIND_VERSION"; then
    log "kind $KIND_VERSION already installed ($(command -v kind))"
    return
  fi
  log "downloading kind $KIND_VERSION → $INSTALL_DIR/kind"
  download "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-${OS}-${ARCH}" \
    "$INSTALL_DIR/kind"
  chmod +x "$INSTALL_DIR/kind"
}

install_kubectl() {
  if want_version kubectl "$KUBECTL_VERSION"; then
    log "kubectl $KUBECTL_VERSION already installed ($(command -v kubectl))"
    return
  fi
  log "downloading kubectl $KUBECTL_VERSION → $INSTALL_DIR/kubectl"
  download "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${OS}/${ARCH}/kubectl" \
    "$INSTALL_DIR/kubectl"
  chmod +x "$INSTALL_DIR/kubectl"
}

install_helm() {
  if want_version helm "$HELM_VERSION"; then
    log "helm $HELM_VERSION already installed ($(command -v helm))"
    return
  fi
  log "downloading helm $HELM_VERSION → $INSTALL_DIR/helm"
  local tmp
  tmp=$(mktemp -d)
  trap "rm -rf $tmp" RETURN
  download "https://get.helm.sh/helm-${HELM_VERSION}-${OS}-${ARCH}.tar.gz" \
    "$tmp/helm.tar.gz"
  tar -xzf "$tmp/helm.tar.gz" -C "$tmp"
  install -m 0755 "$tmp/${OS}-${ARCH}/helm" "$INSTALL_DIR/helm"
}

install_grpcurl() {
  if want_version grpcurl "${GRPCURL_VERSION#v}"; then
    log "grpcurl $GRPCURL_VERSION already installed ($(command -v grpcurl))"
    return
  fi
  log "downloading grpcurl $GRPCURL_VERSION → $INSTALL_DIR/grpcurl"
  local tmp grpc_arch
  tmp=$(mktemp -d)
  trap "rm -rf $tmp" RETURN
  # grpcurl release tarballs use x86_64/arm64 (not amd64/arm64).
  case "$ARCH" in
    amd64) grpc_arch=x86_64 ;;
    arm64) grpc_arch=arm64 ;;
  esac
  local capitalized_os
  case "$OS" in
    linux)  capitalized_os=linux ;;
    darwin) capitalized_os=osx ;;
  esac
  download "https://github.com/fullstorydev/grpcurl/releases/download/${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION#v}_${capitalized_os}_${grpc_arch}.tar.gz" \
    "$tmp/grpcurl.tar.gz"
  tar -xzf "$tmp/grpcurl.tar.gz" -C "$tmp"
  install -m 0755 "$tmp/grpcurl" "$INSTALL_DIR/grpcurl"
}

install_tilt() {
  if want_version tilt "$TILT_VERSION"; then
    log "tilt $TILT_VERSION already installed ($(command -v tilt))"
    return
  fi
  log "downloading tilt $TILT_VERSION → $INSTALL_DIR/tilt"
  local tmp tilt_os tilt_arch
  tmp=$(mktemp -d)
  # shellcheck disable=SC2064 # capture this function-local path now
  trap "rm -rf $tmp" RETURN
  case "$OS" in
    linux)  tilt_os=linux ;;
    darwin) tilt_os=mac ;;
  esac
  case "$ARCH" in
    amd64) tilt_arch=x86_64 ;;
    arm64) tilt_arch=arm64 ;;
  esac
  download "https://github.com/tilt-dev/tilt/releases/download/v${TILT_VERSION}/tilt.${TILT_VERSION}.${tilt_os}.${tilt_arch}.tar.gz" \
    "$tmp/tilt.tar.gz"
  tar -xzf "$tmp/tilt.tar.gz" -C "$tmp" tilt
  install -m 0755 "$tmp/tilt" "$INSTALL_DIR/tilt"
}

ensure_path() {
  case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *)
      log "note: $INSTALL_DIR is not on \$PATH — add 'export PATH=\"$INSTALL_DIR:\$PATH\"' to your shell rc"
      ;;
  esac
}

install_go() {
  # Go 1.25+ is required to build the SPACE containerd shim (shim/go.mod says
  # `go 1.25.0`). Debian trixie's `golang-go` apt package is 1.24 at the time
  # of writing, so we fetch the official tarball.
  #
  # As root we install to the system-wide /usr/local/go so multiple users see
  # the same go. Without root we drop it under $HOME/.local/go and symlink the
  # binary into $INSTALL_DIR/go (which is on PATH); functionally equivalent
  # for a single dev, and the only option on sudo-less devboxes like the
  # devbox platform's default user.
  local go_prefix
  if [[ $EUID -eq 0 ]]; then
    go_prefix=/usr/local
  else
    go_prefix="$HOME/.local"
  fi

  if command -v "${go_prefix}/go/bin/go" >/dev/null 2>&1; then
    local installed
    installed=$("${go_prefix}/go/bin/go" version 2>/dev/null | awk '{print $3}' | sed 's/^go//')
    if [[ "$installed" == "$GO_VERSION" ]]; then
      log "go $GO_VERSION already installed (${go_prefix}/go)"
      ln -sf "${go_prefix}/go/bin/go" "$INSTALL_DIR/go" 2>/dev/null || true
      return
    fi
  fi
  if [[ "$OS" != "linux" ]]; then
    log "go install skipped (not Linux); install go $GO_VERSION manually"
    return
  fi

  log "downloading go $GO_VERSION → ${go_prefix}/go"
  local tmp
  tmp=$(mktemp -d)
  trap "rm -rf $tmp" RETURN
  download "https://go.dev/dl/go${GO_VERSION}.${OS}-${ARCH}.tar.gz" \
    "$tmp/go.tar.gz"
  install -d -m 0755 "$go_prefix" 2>/dev/null || true
  rm -rf "${go_prefix}/go"
  tar -C "$go_prefix" -xzf "$tmp/go.tar.gz"
  # Symlink into the user-writable bin so PATH/INSTALL_DIR picks it up.
  ln -sf "${go_prefix}/go/bin/go" "$INSTALL_DIR/go"
}

install_apt_packages() {
  # Two package groups need apt: the smoke suites' apt-carried prerequisites,
  # read from the shared list used by the bootstrap gate, and docker.io
  # (only when start-dind.sh is also
  # needed — host docker on a constrained partition; not the common case). The
  # AWS CLI is installed separately, from Amazon's bundle; the shared list says
  # why apt cannot carry it on this image.
  local pkg
  if [[ "$OS" != "linux" ]] || ! command -v apt-get >/dev/null 2>&1; then
    log "apt-get not available; install the packages in"
    log "       scripts/lib/space-smoke-prereqs.sh (plus docker.io) manually"
    return
  fi

  # Build the missing-package list once, regardless of whether we're root. Asked
  # per package rather than from the missing-prerequisite set, because the
  # support packages (unzip) are not prerequisites and `aws` is not apt-carried.
  local pkgs=()
  while IFS= read -r pkg; do
    command -v "$pkg" >/dev/null 2>&1 || pkgs+=("$pkg")
  done < <(space_smoke_prereq_packages)
  # docker.io is only required when start-dind.sh is the chosen path; on the
  # default devbox shape there's already a host docker daemon at
  # tcp://localhost:2375, so don't drag in docker.io unless dockerd is
  # genuinely missing AND we have no working DOCKER_HOST.
  if [[ ! -x /usr/sbin/dockerd ]] && ! docker info >/dev/null 2>&1; then
    pkgs+=(docker.io)
  fi
  if (( ${#pkgs[@]} == 0 )); then
    log "the shared prerequisite packages are already present (plus docker.io if needed)"
    return
  fi

  # If we're not root but sudo is available, use it. Many devbox shapes
  # ship a passwordless sudo for the dev user.
  local apt_prefix=()
  if [[ $EUID -ne 0 ]]; then
    # shellcheck disable=SC2207
    apt_prefix=($(sudo_prefix))
    if (( ${#apt_prefix[@]} == 0 )); then
      # Fatal, not a warning: these are the suites' prerequisites (see the
      # shared list for what each one is load-bearing for), so a box that
      # continues from here looks provisioned and silently isn't.
      log "ERROR: apt packages missing (${pkgs[*]}) and this run cannot install them"
      log "       (not root, and no passwordless sudo). Rerun with sudo, or install"
      log "       them by hand: apt-get install ${pkgs[*]}"
      exit 1
    fi
  fi
  log "installing apt packages: ${pkgs[*]}"
  # Both apt calls are guarded, and the check below is the arbiter instead of
  # apt's exit code. On this image apt exits non-zero while configuring packages
  # this script never asked for — the `python3` chain the shared prerequisite
  # list describes — and under `set -e` that would abort the bring-up here,
  # before `install_aws_cli`, leaving absent the very prerequisite this script
  # exists to provide.
  #
  # force-confold: the system containerd may have a hand-modified
  # /etc/containerd/config.toml; preserve existing runtime plugin settings
  # during the package install.
  "${apt_prefix[@]}" env DEBIAN_FRONTEND=noninteractive apt-get update -qq \
    || log "WARNING: apt-get update exited non-zero; continuing to the install"
  "${apt_prefix[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    -o Dpkg::Options::="--force-confold" \
    "${pkgs[@]}" \
    || log "WARNING: apt-get install exited non-zero; verifying the requested commands"

  # A successful install is not the same as a usable command: a package that
  # puts nothing on PATH under the name we expect, or a command clobbered by the
  # install itself, would otherwise leave the caller believing the box is
  # provisioned. docker.io is out of scope here — its `docker` client
  # legitimately comes from elsewhere on the default devbox shape.
  local unusable=()
  while IFS= read -r pkg; do
    command -v "$pkg" >/dev/null 2>&1 || unusable+=("$pkg")
  done < <(space_smoke_prereq_packages)
  # docker.io too, when the fallback asked for it. Verified by its daemon and not
  # by the `docker` client, because the client legitimately comes from elsewhere
  # on the default devbox shape — that is exactly why its absence is not what we
  # asked apt to fix. Skipping this would let a tolerated apt failure report
  # success and leave start-dind.sh with no dockerd.
  local requested
  for requested in "${pkgs[@]}"; do
    [[ "$requested" == "docker.io" ]] || continue
    [[ -x /usr/sbin/dockerd ]] || command -v dockerd >/dev/null 2>&1 \
      || unusable+=("dockerd (from docker.io)")
  done
  if (( ${#unusable[@]} > 0 )); then
    log "ERROR: apt install reported success but these commands are still absent:"
    log "       ${unusable[*]}"
    exit 1
  fi
}

install_aws_cli() {
  # Every tool this script downloads lands in INSTALL_DIR, and the AWS CLI is no
  # exception: a caller that overrides it and later exposes only that directory
  # must find `aws` there. So the question is not "is a pinned aws on PATH
  # somewhere" but "is it in INSTALL_DIR" — the pinned check alone would return
  # early and leave that directory without one.
  SPACE_AWS_CLI_BIN_DIR="$INSTALL_DIR"
  if space_smoke_aws_cli_at_pin "$INSTALL_DIR/aws"; then
    log "aws $SPACE_AWS_CLI_VERSION already installed ($INSTALL_DIR/aws)"
    return 0
  fi
  if [[ "$OS" != "linux" ]]; then
    log "not linux; install the AWS CLI v2 manually if you need the node-drain smoke"
    return 0
  fi

  # A pinned CLI already installed elsewhere is linked rather than
  # downloaded again: same version, no
  # second ~70MB fetch, and INSTALL_DIR ends up with the executable either way.
  local resolved
  if resolved="$(command -v aws 2>/dev/null)" && space_smoke_aws_cli_at_pin "$resolved"; then
    log "linking the pinned aws from $resolved into $INSTALL_DIR"
    if ln -sf "$resolved" "$INSTALL_DIR/aws" 2>/dev/null \
      || sudo_prefix_run ln -sf "$resolved" "$INSTALL_DIR/aws"; then
      space_smoke_aws_cli_at_pin "$INSTALL_DIR/aws" && return 0
    fi
    log "could not link it; installing the bundle instead"
  fi
  # shellcheck disable=SC2207
  local privileged=($(sudo_prefix))
  if [[ ${#privileged[@]} -eq 0 && $EUID -ne 0 ]]; then
    # Same fallback as every downloaded tool above: install where this user can
    # write rather than refusing. The bundle takes any --bin-dir/--install-dir,
    # so a sudo-less host still ends up with a working aws.
    log "no root or passwordless sudo; installing the AWS CLI v2 under $INSTALL_DIR"
    SPACE_AWS_CLI_INSTALL_DIR="${HOME}/.local/aws-cli"
  fi
  space_smoke_install_aws_cli ${privileged[@]+"${privileged[@]}"} || {
    log "ERROR: AWS CLI v2 install failed"
    exit 1
  }
  # Reach it for the rest of this run; `ensure_path` handles future shells.
  case ":$PATH:" in
    *":$SPACE_AWS_CLI_BIN_DIR:"*) ;;
    *) PATH="$SPACE_AWS_CLI_BIN_DIR:$PATH" ;;
  esac
}

log "install dir: $INSTALL_DIR (override with INSTALL_DIR=…)"
log "platform: $OS/$ARCH"
install_kind
install_kubectl
install_helm
install_grpcurl
install_tilt
install_apt_packages
install_aws_cli
install_go
ensure_path
# Report what will actually be used, resolved through PATH rather than assumed to
# be under `$INSTALL_DIR`. `want_version` short-circuits when a tool is already
# installed *somewhere else* on PATH — which is the normal case on a provisioned
# devbox, where these live in /usr/local/bin while `INSTALL_DIR` has fallen back
# to ~/.local/bin because /usr/local/bin is not writable. Exec'ing
# "$INSTALL_DIR/<tool>" then fails with 127 for a tool that is present and
# correct, and takes the whole script's exit status with it.
log "done. Versions:"
report_version() {
  local tool="$1"
  shift
  local path
  if ! path="$(command -v "$tool" 2>/dev/null)"; then
    echo "  $tool: NOT FOUND on PATH" >&2
    return 1
  fi
  printf '  %s (%s): ' "$tool" "$path"
  "$path" "$@" 2>&1 | head -8 || true
}
report_version kind    version
report_version kubectl version --client --output=yaml
report_version helm    version --short
report_version grpcurl --version
report_version tilt    version
