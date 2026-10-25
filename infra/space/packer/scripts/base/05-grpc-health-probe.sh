#!/usr/bin/env bash
# Install the gRPC health probe needed by host-side worker services.
# Runs as root during Packer builds.
set -euo pipefail

GRPC_HEALTH_PROBE_VERSION=v0.4.48
GRPC_HEALTH_PROBE_SHA256=ceeacfc858929b223f90a823e7da46ab34605ec7c2a9ee1dd272fa9e4fedf5e6
GRPC_HEALTH_PROBE_ASSET=grpc_health_probe-linux-amd64

if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "ERROR: no pinned grpc_health_probe artifact for $(uname -m)" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

curl --retry 5 --retry-delay 2 --retry-max-time 180 \
  --connect-timeout 20 --retry-all-errors -fsSL \
  -o "$tmpdir/grpc_health_probe" \
  "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/${GRPC_HEALTH_PROBE_ASSET}"
printf '%s  %s\n' "$GRPC_HEALTH_PROBE_SHA256" "$tmpdir/grpc_health_probe" | sha256sum -c -

install -d -m 0755 -o root -g root /opt/space/bin
install -m 0755 -o root -g root "$tmpdir/grpc_health_probe" /opt/space/bin/grpc_health_probe
/opt/space/bin/grpc_health_probe -version
