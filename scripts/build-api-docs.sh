#!/usr/bin/env bash
# Build the read-only reference from the real supported-contract exporter.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
for command in git node npm python3; do
  command -v "$command" >/dev/null || { echo "Missing prerequisite: $command" >&2; exit 1; }
done
node -e 'const [major, minor] = process.versions.node.split(".").map(Number); if (major < 22 || (major === 22 && minor < 12)) { throw new Error("Node.js 22.12+ is required"); }'
node -e 'if (Number(process.argv[1].split(".")[0]) < 10) { throw new Error("npm 10+ is required"); }' "$(npm --version)"
mkdir -p target/docs
spec="$(mktemp "$root/target/docs/openapi.XXXXXX")"
trap 'rm -f "$spec"' EXIT
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_INCREMENTAL=0
if [[ -n "${SPACE_API_DOCS_RUST_IMAGE:-}" ]]; then
  # CI uses the repository-owned Rust/protoc image; no daemon is needed locally.
  # SPACE_API_DOCS_CARGO_REGISTRY optionally mounts a host Cargo registry, so
  # the build reads crates CI already restored instead of downloading them.
  registry_args=()
  if [[ -n "${SPACE_API_DOCS_CARGO_REGISTRY:-}" ]]; then
    registry_args=(--volume "$SPACE_API_DOCS_CARGO_REGISTRY:/usr/local/cargo/registry")
  fi
  docker run --rm --volume "$root:/workspace" ${registry_args[@]+"${registry_args[@]}"} --workdir /workspace \
    --env CARGO_PROFILE_DEV_DEBUG=0 \
    --env CARGO_INCREMENTAL=0 \
    "$SPACE_API_DOCS_RUST_IMAGE" \
    cargo run --locked -p pplx-space-api --example m1_openapi > "$spec"
else
  for command in cargo cc protoc; do
    command -v "$command" >/dev/null || { echo "Missing prerequisite: $command" >&2; exit 1; }
  done
  cargo run --locked -p pplx-space-api --example m1_openapi > "$spec"
fi
# The combined build already installed the shared lockfile for Mermaid.
if [[ "${SPACE_DOCS_NPM_CI_DONE:-}" != 1 ]]; then
  npm ci --prefix docs/api --ignore-scripts --no-fund --no-audit
fi
node docs/api/build.mjs "$spec"
