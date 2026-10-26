#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 1 ]]; then
  echo "Usage: $0 OUTPUT_DIRECTORY" >&2
  exit 2
fi
directory="$(cd -- "$(dirname -- "$0")" && pwd)"
docker buildx build --platform linux/amd64 --target export \
  --file "$directory/Dockerfile.hardened" \
  --build-arg "BUILD_JOBS=${BUILD_JOBS:-16}" \
  --output "type=local,dest=$1" "$directory"
