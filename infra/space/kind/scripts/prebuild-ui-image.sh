#!/usr/bin/env bash
# Builds the space-ui image as the Tiltfile's space-ui build does, under a tag
# of its own, so that build then finds every layer in this host's build cache.
#
# Kind CI runs it on each shard while the shard waits for the Rust producer.
# The UI needs no Rust, but Tilt holds an image's build until its resource's
# dependencies are ready, here space-api and the console's provisioning, and
# the Next.js build then took about 100 s at the end of the bring-up. The build
# arguments are read from that build in the Tiltfile, its only
# `--build-arg NEXT_PUBLIC_*` arguments, so the two cannot drift apart; finding
# none fails the pre-build, and Tilt then builds the UI itself. Run with the
# same environment as the bring-up (SPACE_CI_IMAGE_MIRROR and
# SPACE_KIND_BUILD_CACHE_REPOSITORY), so both builds compute the same keys.
#
# Usage: infra/space/kind/scripts/prebuild-ui-image.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../../.."

args=()
while read -r arg; do
    args+=(--build-arg "${arg}")
done < <(grep -oE '"--build-arg NEXT_PUBLIC_[A-Z_]+=[^ "]+' Tiltfile | sed 's/^"--build-arg //')
(( ${#args[@]} > 0 )) || { echo "the Tiltfile's space-ui build has no --build-arg NEXT_PUBLIC_* arguments" >&2; exit 1; }

bash infra/space/kind/scripts/pull-base-images.sh docker/Dockerfile.ui
exec bash infra/space/kind/scripts/runtime-image-cache.sh build space-ui \
    --provenance=false --sbom=false -t space-ui:prebuilt "${args[@]}" .
