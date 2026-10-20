#!/usr/bin/env bash
# SPIKE Q4: builds space-credmgr-svc alone, by one identical command, from two
# trees (main, and main with #642 cherry-picked), each in its own target dir
# (cargo's mtime fingerprints would otherwise reuse one tree's crates for the
# other), and layers each binary onto the running worker image. Shortcut: a
# single-package build, so feature unification differs from the full Kind
# artifact build; both variants share it, so #642 is the only difference.
# Usage: build-credmgr-variants.sh <out-dir> <base-image> name=tree...
set -euo pipefail
OUT="$1"; BASE="$2"; shift 2
FEAT="pplx-space-credmgr-svc/otel,pplx-space-credmgr-svc/datadog,pplx-space-credmgr/test-hooks,pplx-space-common-svc/test-hooks"
for v in "$@"; do
  name=${v%%=*}; src=${v#*=}
  echo "== $name $(git -C "$src" rev-parse HEAD)"
  docker run --rm -e CARGO_PROFILE_RELEASE_LTO=false -e CARGO_TARGET_DIR=/target \
    -v "$src:/workspace:ro" -v "$OUT/target-$name:/target" -w /workspace space-rust-chef-base:local \
    cargo build --locked --release -p pplx-space-credmgr-svc --bin space-credmgr-svc --features "$FEAT"
  cp "$OUT/target-$name/release/space-credmgr-svc" "$OUT/space-credmgr-svc.$name"
  sha256sum "$OUT/space-credmgr-svc.$name"
  printf 'FROM %s\nCOPY space-credmgr-svc.%s /opt/space/bin/space-credmgr-svc\n' "$BASE" "$name" >"$OUT/Dockerfile.$name"
  # Not a space-* repository: Tilt's prune-superseded-images removes those
  # from the nodes while no workload references them.
  docker build -q -f "$OUT/Dockerfile.$name" -t "spike-credmgr-authority:$name" "$OUT"
done
