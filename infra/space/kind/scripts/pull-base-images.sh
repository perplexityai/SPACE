#!/usr/bin/env bash
# pull-base-images.sh — put every registry image the given Dockerfiles build
# FROM into the local image store before the build runs, retrying failed pulls.
#
# The Tiltfile runs this ahead of each `docker build`. Each Kind smoke shard
# starts every runtime image build at once, all resolving their bases as
# anonymous public.ecr.aws pulls, and on 2026-09-25 two consecutive main runs
# each lost a shard when one of those pulls got "429 Too Many Requests". A
# failed `docker build` cannot say whether the registry or the Dockerfile was
# at fault, so it cannot be retried safely. A failed `docker pull` of a named
# public image can only be the registry or the network, so every failure here
# is retried, whatever its message. Once the image is local the build stops
# depending on the registry: BuildKit finds the manifest and config already in
# the content store, and falls back to the local image when its tag lookup
# fails.
#
# Usage: pull-base-images.sh [--list] <dockerfile>...
#   --list prints the images instead of pulling them, so a caller that hands
#   images to other Docker daemons (scripts/tests/smoke-kind-parallel.sh) can
#   name exactly the set these builds read.
#   Pulls the images named by FROM lines, directly or through an ARG default,
#   and by `# syntax=` directives. Build stages, `scratch`, the `:local` images
#   this repository builds, and references an ARG only partly expands are left
#   to the build. Images already present are not pulled, so a warm box makes no
#   registry request here.
#
# Env: SPACE_KIND_PULL_ATTEMPTS (default 5) bounds the attempts per image;
#   SPACE_KIND_PULL_BACKOFF_SECONDS (default 2) is the first retry delay, which
#   doubles after each failure and gets up to as much again in jitter so the
#   builds that failed together do not retry together.
#   SPACE_CI_IMAGE_MIRROR (GitHub CI) first takes each image listed in
#   ci/mirrored-images.txt from the repository's GHCR mirror, falling back to
#   its source's pinned digest, and retries only an image both failed to give.
set -euo pipefail

attempts="${SPACE_KIND_PULL_ATTEMPTS:-5}"
backoff="${SPACE_KIND_PULL_BACKOFF_SECONDS:-2}"
mirror_images="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)/ci/scripts/mirror-images.sh"

base_images() {
    awk '
        FNR == 1 { split("", arg); split("", stage) }
        /^#[[:space:]]*syntax[[:space:]]*=/ {
            sub(/^#[[:space:]]*syntax[[:space:]]*=[[:space:]]*/, "")
            print $1
            next
        }
        toupper($1) == "ARG" && index($2, "=") > 0 {
            value = substr($2, index($2, "=") + 1)
            gsub(/"/, "", value)
            arg[substr($2, 1, index($2, "=") - 1)] = value
            next
        }
        toupper($1) == "FROM" {
            i = 2
            while (substr($i, 1, 2) == "--") i++
            ref = $i
            if (ref ~ /^\$\{?[A-Za-z_][A-Za-z0-9_]*\}?$/) {
                name = ref
                gsub(/[${}]/, "", name)
                ref = arg[name]
            }
            if (ref != "" && ref != "scratch" && !(ref in stage) && ref !~ /:local$/ && ref !~ /\$/) print ref
            if (toupper($(i + 1)) == "AS") stage[$(i + 2)] = 1
        }
    ' "$@" | sort -u
}

pull_with_retries() {
    local image="$1" attempt=1 delay="$backoff" output
    while :; do
        if output="$(docker pull --quiet "$image" 2>&1)"; then
            return 0
        fi
        if (( attempt >= attempts )); then
            echo "ERROR: could not pull $image after $attempts attempts:" >&2
            printf '%s\n' "$output" >&2
            return 1
        fi
        echo "[pull-base-images] pull of $image failed (attempt $attempt/$attempts), retrying in ${delay}s+: $(tail -n 1 <<<"$output")" >&2
        sleep "$(( delay + RANDOM % (delay + 1) ))"
        attempt=$(( attempt + 1 ))
        delay=$(( delay * 2 ))
    done
}

list=0
if [[ "${1:-}" == --list ]]; then
    list=1
    shift
fi
if (( $# == 0 )); then
    echo "usage: $0 [--list] <dockerfile>..." >&2
    exit 64
fi

images="$(base_images "$@")"
if (( list )); then
    printf '%s\n' "$images"
    exit 0
fi
while IFS= read -r image; do
    [[ -n "$image" ]] || continue
    docker image inspect "$image" >/dev/null 2>&1 && continue
    if [[ -n "${SPACE_CI_IMAGE_MIRROR:-}" ]] && bash "$mirror_images" listed "$image"; then
        bash "$mirror_images" pull "$image"
        docker image inspect "$image" >/dev/null 2>&1 && continue
    fi
    echo "[pull-base-images] pulling $image"
    pull_with_retries "$image"
done <<<"$images"
