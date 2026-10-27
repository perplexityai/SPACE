#!/usr/bin/env bash
# space-images.sh — the SPACE images the Kind harness builds locally and loads
# into the nodes that run them under the `<name>:tilt-<hash>` references Tilt
# deploys in place of their `<name>:local` refs (the Tiltfile's custom_builds).
#
# Source it for the array, or run it to print one name per line (the
# superseded-image prune reads that). These are the only repositories
# prune-superseded-images.py may remove tags of: Kind cannot re-pull them, so
# every other image in a node's store is left alone. Each name's Dockerfile
# sets `org.opencontainers.image.title` to that name, which is how the prune
# recognizes one of these images after its tag has moved to a newer build.

SPACE_KIND_LOCAL_IMAGES=(
  space-api
  space-data-plane
  space-cluster-controller
  space-worker
  space-seed-publish
  space-ui
  space-kind-key-broker
  space-kind-sts-bridge
  space-kind-fake-ec2
)

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  printf '%s\n' "${SPACE_KIND_LOCAL_IMAGES[@]}"
fi
