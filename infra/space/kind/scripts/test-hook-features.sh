#!/usr/bin/env bash
# test-hook-features.sh — print the Kind host build's test-hook features,
# comma-separated on one line: `<package>/test-hooks` for every workspace
# member that defines a `test-hooks` feature, so a crate that gains or drops
# one needs no list edited. Every member is in the host build's package
# selection, so cargo accepts each one.
#
# Usage, from the workspace root, with the build's SPACE_KIND_RUST_BUILD
# (infra/space/kind/lib/rust-build.sh):
#   bash infra/space/kind/scripts/test-hook-features.sh test-hooks
#   bash infra/space/kind/scripts/test-hook-features.sh release   # prints nothing
#
# docker/Dockerfile.rust-build-kind runs it for its cook and build stages, and
# build-rust-artifacts.sh's incremental mode runs it inside the builder image.
set -euo pipefail

case "${1:-}" in
  test-hooks) ;;
  release) exit 0 ;;
  *) echo "usage: $0 test-hooks|release" >&2; exit 2 ;;
esac

cargo metadata --offline --no-deps --format-version 1 | python3 -c '
import json, sys
features = [p["name"] + "/test-hooks" for p in json.load(sys.stdin)["packages"] if "test-hooks" in p["features"]]
if not features:
    sys.exit("no workspace member defines a test-hooks feature")
print(",".join(features))'
