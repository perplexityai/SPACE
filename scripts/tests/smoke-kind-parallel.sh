#!/usr/bin/env bash
# smoke-kind-parallel.sh — the whole typed Kind smoke on one host, as N
# isolated benches side by side instead of N shards one after another.
#
# Each bench is one CI smoke shard's runner (kind-ci.yml, job `smoke`): its own
# network namespace, dockerd, HOME and TMPDIR (scripts/tests/lib/kind-bench.sh),
# on which the unmodified bring-up and typed smoke run with every default name
# and port. Bench i of N runs CI's baseline step and then
# `smoke-kind-typed.sh all i/N`, so together the benches cover both API
# surfaces' inventory exactly once, as the CI shards do.
#
# What every bench would otherwise do for itself, and gets the same result
# from, is done once on the host and handed over:
#   - the Rust artifact image: built once (build-rust-artifacts.sh, its normal
#     auto mode, so the host's persistent target dir is reused) and handed to
#     each bench as CI hands it to its shards (rust-artifact-handoff.py
#     record/load, then SPACE_KIND_RUST_ARTIFACTS=prebuilt);
#   - the SPACE Kind node image, the kindest/node base and the pre-built UI
#     image, saved once and loaded into every bench's daemon;
#   - Docker Hub: one pull-through mirror for every bench daemon, so an image
#     costs one pull from the host's egress, not one per bench, against the
#     anonymous rate limit that egress shares.
# The bench's Rust-independent preparation (cluster creation, observability
# install) overlaps the host's Rust build, as CI's shards overlap the producer.
#
# Usage:
#   scripts/tests/smoke-kind-parallel.sh [run] [--benches N] [--keep] [--out DIR]
#   scripts/tests/smoke-kind-parallel.sh teardown
#
#   --benches N  benches to run (default: from the host's CPUs and memory, printed)
#   --keep       leave the benches up afterwards; `teardown` removes them
#   --out DIR    where logs, timings and the summary go
#                (default: ~/.space-devbox/kind-parallel/<run id>)
#
# Exit status: 0 every bench passed; 1 a scenario failed; 2 inconclusive, or a
# bench did not come up.
#
# Env: SPACE_KIND_SUBSTRATE, SPACE_SMOKE_TIERS (default routine),
# SPACE_SMOKE_INCLUDE_SCENARIOS, SPACE_KIND_TELEMETRY_MODE (default otel) and
# SPACE_CI_IMAGE_MIRROR pass through to every bench. SPACE_KIND_BENCH_ROOT
# (default ~/.space-benches) holds the benches' Docker data.
# shellcheck source-path=SCRIPTDIR
# Most functions run only through phase, host_step or a trap.
# shellcheck disable=SC2317
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
KIND_BENCH_ROOT="${SPACE_KIND_BENCH_ROOT:-$HOME/.space-benches}"
# shellcheck source=lib/kind-bench.sh
source "$SCRIPT_DIR/lib/kind-bench.sh"

log() { printf '[kind-parallel %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 2; }

cd "$REPO_ROOT"
COMMAND=run
BENCHES=""
KEEP=0
OUT=""
if [[ "${1:-}" == run || "${1:-}" == teardown ]]; then
  COMMAND="$1"
  shift
fi
while (( $# > 0 )); do
  case "$1" in
    --benches) BENCHES="${2:?--benches needs a count}"; shift 2 ;;
    --benches=*) BENCHES="${1#*=}"; shift ;;
    --keep) KEEP=1; shift ;;
    --out) OUT="${2:?--out needs a directory}"; shift 2 ;;
    --out=*) OUT="${1#*=}"; shift ;;
    -h|--help) sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

# Two registries on the host's own Docker, kept across runs:
#   - a pull-through Docker Hub mirror every bench daemon uses, so an image
#     costs one pull from the host's egress, whatever N is;
#   - the Kind layer cache (infra/space/kind/lib/build-cache.sh) as a loopback
#     registry: the host exports the runtime images' and seed builder's
#     dependency stages and the UI build into it once, and every bench's
#     builds import them, instead of each bench's BuildKit building them again.
# Both listen on an address of a dummy interface in the host namespace, which
# every bench reaches through its default route and nothing outside the host
# can; the layer cache also on the host's loopback. build-cache.sh accepts only
# a loopback repository besides GHCR, so each bench forwards its own
# 127.0.0.1:$CACHE_PORT to the host's.
REGISTRY_LINK="${KIND_BENCH_PREFIX:0:8}0"
REGISTRY_IP=192.168.241.1
REGISTRY_IMAGE="public.ecr.aws/docker/library/registry:2"
CACHE_ROOT="$KIND_BENCH_ROOT-cache"
MIRROR_NAME="${KIND_BENCH_PREFIX}-hub-mirror"
MIRROR_PORT=5000
CACHE_NAME="${KIND_BENCH_PREFIX}-layer-cache"
CACHE_PORT=5001
LAYER_CACHE="localhost:$CACHE_PORT/space-kind-build-cache"
# The whole UI build goes into the scope of its cached deps stage
# (runtime-image-cache.sh), which every UI build already imports, so a bench's
# Tilt build of the UI is a cache hit without a pre-build. A scope of its own
# beside that one measured as a miss on two of four benches: BuildKit took the
# deps stage's result from either import, and only one led on to the rest.
# shellcheck source=../../infra/space/kind/lib/build-cache.sh
source "$REPO_ROOT/infra/space/kind/lib/build-cache.sh"
UI_CACHE_SCOPE="$(space_kind_layer_scope ui-deps)"
# The seed the benches copy instead of building one each.
SEED_DIR="$CACHE_ROOT/seed"

teardown_all() {
  local i pids=()
  for i in $(kind_bench_existing); do
    log "tearing down bench $i"
    kind_bench_teardown "$i" &
    pids+=("$!")
  done
  wait ${pids[@]+"${pids[@]}"} || true
  # The benches' checkouts went with their directories.
  git -C "$REPO_ROOT" worktree prune
}

kind_bench_platform
kind_bench_lock
if [[ "$COMMAND" == teardown ]]; then
  sudo -n true 2>/dev/null || die "teardown needs passwordless sudo"
  teardown_all
  # The registries' data stays in $CACHE_ROOT for the next run.
  docker rm -f "$MIRROR_NAME" "$CACHE_NAME" >/dev/null 2>&1 || true
  sudo -n ip link del "$REGISTRY_LINK" 2>/dev/null || true
  exit 0
fi

# Refusals (no sudo, no KVM, a missing tool, a loopback resolver) print here,
# not into a log.
kind_bench_preflight

# Default N: a bench is one 16-core CI runner's work, but mostly waiting. What
# it holds is about 50 GB of disk under $KIND_BENCH_ROOT at its peak, 6 GiB of
# memory, and bursts of CPU and disk writes while it comes up, all N at once
# (measured; see "The whole suite on one host" in infra/space/kind/README.md).
default_benches() {
  local cpus mem_gib disk_gib by_cpu by_mem by_disk n
  cpus="$(nproc)"
  mem_gib=$(( $(awk '/^MemTotal:/ {print $2}' /proc/meminfo) / 1024 / 1024 ))
  mkdir -p "$KIND_BENCH_ROOT"
  disk_gib=$(( $(df -k --output=avail "$KIND_BENCH_ROOT" | tail -n 1) / 1024 / 1024 ))
  by_cpu=$(( cpus / 8 ))
  by_mem=$(( mem_gib / 16 ))
  by_disk=$(( disk_gib / 60 ))
  n=$(( by_cpu < by_mem ? by_cpu : by_mem ))
  (( by_disk < n )) && n=$by_disk
  # Past six the bring-ups' disk and CPU contention outgrows what a shorter
  # typed phase saves (measured from four to six).
  (( n > 6 )) && n=6
  (( n < 1 )) && n=1
  printf '%s\n' "$n"
}

if [[ -z "$BENCHES" ]]; then
  BENCHES="$(default_benches)"
  log "benches: $BENCHES (from $(nproc) CPUs, $(( $(awk '/^MemTotal:/ {print $2}' /proc/meminfo) / 1024 / 1024 )) GiB of memory and the free disk under $KIND_BENCH_ROOT; --benches overrides)"
fi
if ! [[ "$BENCHES" =~ ^[1-9][0-9]*$ ]] || (( BENCHES > KIND_BENCH_MAX )); then
  die "--benches must be 1..$KIND_BENCH_MAX (got $BENCHES)"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-n${BENCHES}"
OUT="${OUT:-$HOME/.space-devbox/kind-parallel/$RUN_ID}"
# An earlier run's host/<step>.ok markers would satisfy this run's waits.
if [[ -d "$OUT" && -n "$(ls -A "$OUT" 2>/dev/null)" ]]; then
  die "--out $OUT is not empty; give each run a directory of its own"
fi
mkdir -p "$OUT/host"
OUT="$(cd "$OUT" && pwd)"
HOST="$OUT/host"
RUN_START="$(date +%s)"
printf '%s\n' "$RUN_START" >"$OUT/started"
printf '%s\n' "$BENCHES" >"$OUT/bench-count"
log "run $RUN_ID: output in $OUT"

# One line per phase: <owner> <phase> <start epoch> <end epoch> <exit status>.
#
# A step's status is its own: every step runs as `phase ... || ...` or under
# `if`, where bash ignores `set -e` for the whole step body, a subshell's
# `set -e` and an asynchronous one's included (tried). So every command in a
# step that can fail is followed by `|| return 1`: without it a failed Rust
# build went on to save and hand over the image the host already had.
TIMINGS="$OUT/timings.tsv"
: >"$TIMINGS"
phase() {
  local owner="$1" name="$2"; shift 2
  local start rc=0
  start="$(date +%s)"
  "$@" || rc=$?
  printf '%s\t%s\t%s\t%s\t%s\n' "$owner" "$name" "$start" "$(date +%s)" "$rc" >>"$TIMINGS"
  return "$rc"
}

# Load, memory and disk every 30 s, for the run's resource peaks.
sample_resources() {
  local docker_root
  docker_root="$(timeout 20 docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
  printf 'epoch\tload1\tmem_used_gib\tcontainer_mem_gib\thome_used_gib\tdocker_root_used_gib\tbench_cgroups_mem_gib\tcpu_busy_pct\tiowait_pct\tcpu_psi_some10\tio_psi_some10\tio_psi_full10\thome_read_mbps\thome_write_mbps\n' >"$OUT/resources.tsv"
  # busy, iowait and total jiffies since boot, for the share of each sample.
  cpu_jiffies() { awk '$1 == "cpu" {print $2+$3+$4+$7+$8, $6, $2+$3+$4+$5+$6+$7+$8}' /proc/stat; }
  psi() { awk -v kind="$2" -F'avg10=' '$0 ~ "^" kind {split($2, v, " "); print v[1]}' "/proc/pressure/$1" 2>/dev/null || echo NA; }
  # The block device under $HOME, where the benches' data roots are: the
  # bring-ups were measured to be bound by its write throughput.
  local disk
  disk="$(lsblk -no PKNAME "$(df --output=source "$KIND_BENCH_ROOT" | tail -n 1)" 2>/dev/null | head -n 1)"
  [[ -n "$disk" ]] || disk="$(basename "$(df --output=source "$KIND_BENCH_ROOT" | tail -n 1)")"
  sectors() { awk -v d="$disk" '$3 == d {print $6, $10}' /proc/diskstats; }
  local prev cur cg bench_mem f prev_io cur_io
  prev="$(cpu_jiffies)"
  prev_io="$(sectors)"
  cg="/sys/fs/cgroup$(awk -F: '$1 == 0 {print $3}' /proc/self/cgroup)"
  while [[ -d "$OUT" ]]; do
    sleep 30
    cur="$(cpu_jiffies)"
    cur_io="$(sectors)"
    bench_mem=0
    for f in /sys/fs/cgroup/"${KIND_BENCH_PREFIX}"-*/memory.current; do
      [[ -r "$f" ]] && bench_mem=$(( bench_mem + $(<"$f") ))
    done
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$(date +%s)" \
      "$(cut -d' ' -f1 /proc/loadavg)" \
      "$(awk '/^MemTotal:/ {t=$2} /^MemAvailable:/ {a=$2} END {printf "%.1f", (t-a)/1048576}' /proc/meminfo)" \
      "$(awk '{printf "%.1f", $1/1073741824}' "$cg/memory.current" 2>/dev/null || echo NA)" \
      "$(df -B1 --output=used "$HOME" | awk 'NR==2 {printf "%.1f", $1/1073741824}')" \
      "$( [[ -n "$docker_root" ]] && df -B1 --output=used "$docker_root" 2>/dev/null | awk 'NR==2 {printf "%.1f", $1/1073741824}' || echo NA)" \
      "$(awk -v b="$bench_mem" 'BEGIN {printf "%.1f", b/1073741824}')" \
      "$(awk -v p="$prev" -v c="$cur" 'BEGIN {
          split(p, a, " "); split(c, b, " "); t = b[3] - a[3]
          if (t > 0) printf "%.0f\t%.0f", 100 * (b[1] - a[1]) / t, 100 * (b[2] - a[2]) / t; else printf "NA\tNA" }')" \
      "$(psi cpu some)" "$(psi io some)" "$(psi io full)" \
      "$(awk -v p="$prev_io" -v c="$cur_io" 'BEGIN {
          split(p, a, " "); split(c, b, " ")
          if (b[1] != "") printf "%.0f\t%.0f", (b[1] - a[1]) * 512 / 30e6, (b[2] - a[2]) * 512 / 30e6; else printf "NA\tNA" }')" \
      >>"$OUT/resources.tsv"
    prev="$cur"
    prev_io="$cur_io"
  done
}

BACKGROUND=()
# Every process a background job started, children first.
kill_tree() {
  local pid="$1" child
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    kill_tree "$child"
  done
  kill -TERM "$pid" 2>/dev/null || true
}

FINISHED=0
on_exit() {
  local rc=$?
  trap - EXIT INT TERM
  local pid
  for pid in ${BACKGROUND[@]+"${BACKGROUND[@]}"}; do
    kill_tree "$pid"
  done
  if (( KEEP == 1 )); then
    log "--keep: benches left up; remove them with: $0 teardown"
  elif (( KEEP == 0 )); then
    phase run teardown teardown_all >"$OUT/teardown.log" 2>&1 || true
    rm -f "$HOST/shared-images.tar" "$HOST/rust/kind-rust.tar.gz"
  fi
  if (( ! FINISHED )); then
    log "interrupted (exit $rc); logs kept in $OUT"
    (( rc == 0 )) && rc=2
  fi
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- host preparation, once ------------------------------------------------

mark() { : >"$HOST/$1.$2"; }
# Wait for host step <name> to finish; fails if it did.
await() {
  local name="$1"
  until [[ -e "$HOST/$name.ok" || -e "$HOST/$name.failed" ]]; do
    sleep 2
  done
  [[ -e "$HOST/$name.ok" ]]
}
# Run host step <name> in the background, marking its outcome.
host_step() {
  local name="$1"; shift
  (
    if phase host "$name" "$@" >"$HOST/$name.log" 2>&1; then
      mark "$name" ok
    else
      mark "$name" failed
      log "host step $name failed; see $HOST/$name.log"
    fi
  ) &
  BACKGROUND+=("$!")
}

prepare_host() {
  kind_bench_ensure_docker || return 1
  kind_bench_raise_limits "$BENCHES"
  # socat forwards each bench's loopback to the layer cache; rsync copies the
  # checkout's uncommitted changes into each bench's.
  for tool in socat rsync; do
    command -v "$tool" >/dev/null 2>&1 \
      || sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$tool" >/dev/null 2>&1 \
      || { log "benches need $tool"; return 1; }
  done
  for tool in kind kubectl tilt helm aws uv jq curl; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      log "installing the Kind prerequisites ($tool is missing)"
      bash "$REPO_ROOT/infra/space/kind/scripts/install-prereqs.sh" || return 1
      break
    fi
  done
  # Loop devices are kernel-global, but a container's /dev holds only the nodes
  # its host had when the container was created (scripts/lib/space-loop-devices.sh).
  # N benches' node caches, encrypted sandboxes and seed builds take indices
  # far past the eight a fresh host has, and a seed build whose container
  # lacks the node for the index it was handed fails ("cannot mount seed
  # image"); measured with four benches. Staged before any bench container
  # exists, every container's /dev has the nodes.
  # shellcheck source=../lib/space-loop-devices.sh
  SPACE_LOOP_DEVICE_TARGET="${SPACE_LOOP_DEVICE_TARGET:-$(( 64 + 128 * BENCHES ))}" \
    source "$REPO_ROOT/scripts/lib/space-loop-devices.sh" || return 1
  space_loop_stage_pool log
  # Kind nodes share the host kernel and cannot load modules; CI loads the
  # BYOK targets on the runner the same way (kind-ci.yml, load-byok-targets).
  # shellcheck source=../lib/space-smoke-prereqs.sh
  source "$REPO_ROOT/scripts/lib/space-smoke-prereqs.sh" || return 1
  space_smoke_prepare_encrypted_targets || return 1
  # Staged once, mounted read-only into every bench's nodes.
  if ! bash "$REPO_ROOT/infra/space/kind/scripts/install-fc-vendor.sh" --check >/dev/null 2>&1; then
    bash "$REPO_ROOT/infra/space/kind/scripts/install-fc-vendor.sh" || return 1
  fi
}

start_registry() { # name port data-dir [env...]
  local name="$1" port="$2" data="$3" binds=()
  shift 3
  if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != true ]]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
    mkdir -p "$data" || return 1
    binds=(-p "$REGISTRY_IP:$port:5000")
    [[ "$port" == "$CACHE_PORT" ]] && binds+=(-p "127.0.0.1:$port:5000")
    local env=() setting
    for setting in "$@"; do env+=(-e "$setting"); done
    docker run -d --name "$name" --restart unless-stopped "${binds[@]}" \
      -v "$data:/var/lib/registry" -e REGISTRY_STORAGE_DELETE_ENABLED=true \
      ${env[@]+"${env[@]}"} "$REGISTRY_IMAGE" >/dev/null || return 1
  fi
  local _
  for _ in $(seq 1 30); do
    curl -fsS -m 2 "http://$REGISTRY_IP:$port/v2/" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

start_registries() {
  if ! ip link show "$REGISTRY_LINK" >/dev/null 2>&1; then
    sudo -n ip link add "$REGISTRY_LINK" type dummy || return 1
    sudo -n ip addr add "$REGISTRY_IP/32" dev "$REGISTRY_LINK" || return 1
    sudo -n ip link set "$REGISTRY_LINK" up || return 1
  fi
  start_registry "$MIRROR_NAME" "$MIRROR_PORT" "$CACHE_ROOT/hub-mirror" \
    REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io || return 1
  start_registry "$CACHE_NAME" "$CACHE_PORT" "$CACHE_ROOT/layer-cache"
}

# Every image a bench's builds start FROM: the Dockerfiles Tilt and the seed
# build read (Tiltfile, build-seed.sh).
base_image_dockerfiles() {
  {
    grep -o 'docker/Dockerfile\.[A-Za-z0-9._-]*' "$REPO_ROOT/Tiltfile"
    printf '%s\n' templates/firecracker/seed/Dockerfile
  } | sort -u | grep -v -e rust-build -e rust-artifacts-host
}

# The images every bench loads before it creates its cluster: the node images
# the cluster config names, and the build bases.
shared_images() {
  local node base listed dockerfiles=() images=()
  node="$(bash "$REPO_ROOT/infra/space/kind/scripts/kind-node-image.sh" ensure)" || return 1
  base="$(bash "$REPO_ROOT/infra/space/kind/scripts/kind-node-image.sh" cluster-base)" || return 1
  docker image inspect "$base" >/dev/null 2>&1 || docker pull -q "$base" || return 1
  listed="$(base_image_dockerfiles)" || return 1
  mapfile -t dockerfiles <<<"$listed"
  bash "$REPO_ROOT/infra/space/kind/scripts/pull-base-images.sh" "${dockerfiles[@]}" || return 1
  listed="$(bash "$REPO_ROOT/infra/space/kind/scripts/pull-base-images.sh" --list "${dockerfiles[@]}")" || return 1
  mapfile -t images <<<"$listed"
  # `docker save` keeps tags, not digests, and an untagged load would be a
  # dangling image: save each name:tag@digest reference under its tag.
  local image saved=("$node")
  for image in "$base" "${images[@]}"; do
    [[ -n "$image" ]] || continue
    if [[ "$image" == *@* ]]; then
      docker tag "$image" "${image%@*}" || return 1
      image="${image%@*}"
    fi
    saved+=("$image")
  done
  printf '%s\n' "${saved[@]}" >"$HOST/shared-images.txt" || return 1
  docker save -o "$HOST/shared-images.tar" "${saved[@]}" || return 1
}

# The handoff's identity checks read a GitHub run's coordinates; this run is
# one, with the checkout's own commit.
handoff_env() {
  printf 'GITHUB_REPOSITORY=local/space\n'
  printf 'GITHUB_SHA=%s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD)"
  printf 'GITHUB_RUN_ID=%s\n' "$RUN_START"
  printf 'GITHUB_RUN_ATTEMPT=1\n'
}

rust_image() {
  local identity=() line listed
  listed="$(handoff_env)" || return 1
  while IFS= read -r line; do identity+=("$line"); done <<<"$listed"
  local reuse_out="$HOST/rust-reuse.out"
  : >"$reuse_out" || return 1
  # Main's image for identical Rust inputs, where the registry is readable.
  env "${identity[@]}" GITHUB_OUTPUT="$reuse_out" \
    python3 "$REPO_ROOT/infra/space/kind/scripts/rust-artifact-handoff.py" reuse \
    ghcr.io/perplexityai/space-kind-rust-artifacts || true
  if ! grep -q '^reused=true$' "$reuse_out"; then
    bash "$REPO_ROOT/infra/space/kind/scripts/build-rust-artifacts.sh" || return 1
  fi
  install -d "$HOST/rust" || return 1
  # pipefail (set at the top) makes a failed save fail the pipeline.
  docker image save space-rust-build-kind:local | gzip -1 >"$HOST/rust/kind-rust.tar.gz" || return 1
  env "${identity[@]}" python3 "$REPO_ROOT/infra/space/kind/scripts/rust-artifact-handoff.py" \
    record "$HOST/rust" || return 1
}

# The layer cache every bench reads: the cached stages, exported as main CI
# exports them to GHCR, and the whole UI build under its own scope.
build_cache() {
  SPACE_KIND_BUILD_CACHE_REPOSITORY="$LAYER_CACHE" SPACE_KIND_BUILD_CACHE_WRITE=1 \
    bash "$REPO_ROOT/infra/space/kind/scripts/runtime-image-cache.sh" publish || return 1
  bash "$REPO_ROOT/infra/space/kind/scripts/prebuild-ui-image.sh" \
    --cache-to "type=registry,ref=$LAYER_CACHE:$UI_CACHE_SCOPE,mode=max,image-manifest=true,oci-mediatypes=true" \
    || return 1
}

# One seed for every bench: its inputs are the Rust image's guest binaries, the
# seed recipe and builder, and the sandbox SSH key, all identical across
# benches once they share the host's key. A bench handed this directory and
# that key finds its seed up to date (build-seed.sh) and builds none.
seed() {
  await rust || return 1
  SPACE_SEED_OUTPUT_DIR="$SEED_DIR" bash "$REPO_ROOT/infra/space/kind/scripts/build-seed.sh" || return 1
}

# The full inventory, for the coverage check: the typed runner's own registry
# and partition, read in this checkout's project environment. Written whole or
# not at all, so the summary never reads a partial file as the inventory.
inventory() {
  (cd "$REPO_ROOT" && SPACE_SMOKE_BROWSER_SCENARIOS="${SPACE_SMOKE_BROWSER_SCENARIOS:-withhold}" \
    uv run --frozen --python 3.11 python "$SCRIPT_DIR/lib/kind_parallel_report.py" inventory \
      --benches "$BENCHES" >"$HOST/inventory.json.tmp") || return 1
  mv "$HOST/inventory.json.tmp" "$HOST/inventory.json" || return 1
}

# --- benches ---------------------------------------------------------------

bench_env() {
  local i="$1"
  local file="$OUT/benches/$i/bench.env"
  kind_bench_write_env "$i" "$file" || return 1
  {
    printf 'SPACE_VENDOR_ROOT=%s\n' "$SPACE_VENDOR_ROOT"
    # CI's smoke job environment (kind-ci.yml), pinned the same way.
    printf 'SPACE_KIND_TELEMETRY_MODE=%s\n' "${SPACE_KIND_TELEMETRY_MODE:-otel}"
    printf 'SPACE_KIND_OBSERVABILITY_LAYOUT=%s\n' "${SPACE_KIND_OBSERVABILITY_LAYOUT:-ci}"
    printf 'SPACE_SMOKE_BROWSER_SCENARIOS=%s\n' "${SPACE_SMOKE_BROWSER_SCENARIOS:-withhold}"
    printf 'SPACE_SMOKE_TIERS=%s\n' "${SPACE_SMOKE_TIERS:-routine}"
    printf 'SPACE_KIND_RUST_ARTIFACTS=prebuilt\n'
    printf 'SPACE_KIND_BUILD_CACHE_REPOSITORY=%s\n' "$LAYER_CACHE"
    local name
    for name in SPACE_KIND_SUBSTRATE SPACE_SMOKE_INCLUDE_SCENARIOS SPACE_CI_IMAGE_MIRROR \
        SPACE_KIND_RUST_BUILD SPACE_CONSOLE_PASSWORD; do
      [[ -n "${!name:-}" ]] && printf '%s=%s\n' "$name" "${!name}"
    done
    handoff_env
    printf 'GITHUB_ENV=%s\n' "$OUT/benches/$i/handoff.env"
  } >>"$file" || return 1
  printf '%s\n' "$file"
}

# Bench i's daemon, configured to pull Docker Hub through the host's mirror.
bench_docker() {
  local i="$1"
  kind_bench_claim "$i" || return 1
  kind_bench_network_up "$i" || return 1
  kind_bench_pidns_up "$i" || return 1
  KIND_BENCH_MIRROR="http://$REGISTRY_IP:$MIRROR_PORT" kind_bench_docker_up "$i"
}

# One bench, start to finish. Its log is the bench directory's bench.log; its
# outcome is the `result` file the summary reads.
run_bench() {
  local i="$1" dir="$OUT/benches/$1" env_file
  exec >"$dir/bench.log" 2>&1
  # Globals, not locals: the EXIT trap runs after run_bench has returned and
  # its locals are gone. Each bench runs in a subshell of its own.
  BENCH_RESULT=up_failed
  BENCH_RESULT_FILE="$dir/result"
  trap 'printf "%s\n" "$BENCH_RESULT" >"$BENCH_RESULT_FILE"' EXIT
  phase "bench$i" docker bench_docker "$i" || return 1
  env_file="$(bench_env "$i")" || return 1
  local src
  src="$(kind_bench_dir "$i")/src"
  phase "bench$i" checkout kind_bench_checkout "$REPO_ROOT" "$src" || return 1
  # Bench commands start in the bench's checkout (kind_bench_exec keeps $PWD).
  cd "$src" || return 1
  bx() { kind_bench_exec "$i" "$env_file" "$@"; }
  # The bench's own loopback end of the host's layer cache, in its namespaces,
  # so it ends with them.
  bx setsid socat "TCP-LISTEN:$CACHE_PORT,bind=127.0.0.1,fork,reuseaddr" "TCP:$REGISTRY_IP:$CACHE_PORT" \
    </dev/null >"$dir/cache-forward.log" 2>&1 &
  await shared-images || return 1
  phase "bench$i" load_images bx docker load -q -i "$HOST/shared-images.tar" || return 1
  # Everything up to the Rust handoff is Rust-independent, so it overlaps the
  # host's build, in kind-ci.yml's order. CI's UI pre-build has no step here:
  # the layer cache holds the host's whole UI build (UI_CACHE_SCOPE).
  # SPACE_CI_IMAGE_MIRROR for this step only: with it the bring-up loads the
  # infra charts' third-party images into the infra node from the bench's
  # daemon (bootstrap_kind_stack__load_mirrored_chart_images), so they come
  # through the host's Docker Hub mirror instead of one kubelet pull per bench.
  # Where the GHCR mirror is unreadable each image falls back to its source.
  phase "bench$i" prepare_cluster bx env SPACE_CI_IMAGE_MIRROR="${SPACE_CI_IMAGE_MIRROR:-ghcr}" \
    bash "$src/infra/space/kind/scripts/prepare-cluster.sh" || return 1
  phase "bench$i" observability bx env SPACE_KIND_CLUSTER=space-local bash -c '
      python3 infra/space/kind/scripts/datadog_backend.py preflight --for-deployment >/dev/null
      bash infra/space/kind/scripts/install-observability.sh' || return 1
  await rust || return 1
  phase "bench$i" rust_handoff bx python3 "$src/infra/space/kind/scripts/rust-artifact-handoff.py" \
    load "$HOST/rust" || return 1
  cat "$dir/handoff.env" >>"$env_file" || return 1
  # The host's seed and the key it was built with, where the bench's own seed
  # build looks (build-seed.sh); without them the bench builds its own.
  if await seed; then
    # shellcheck disable=SC2016 # expanded by the bench's shell
    phase "bench$i" seed_copy bx bash -c '
        install -d -m 0755 "$HOME/.space-devbox/kind/ssh" "$HOME/.space-devbox/kind/seed"
        cp -a "$1/." "$HOME/.space-devbox/kind/ssh/"
        cp -a --sparse=always "$2" "$HOME/.space-devbox/kind/seed/build"' _ \
      "$HOME/.space-devbox/kind/ssh" "$SEED_DIR" || true
  fi
  await build-cache || true
  BENCH_RESULT=baseline_failed
  phase "bench$i" baseline bx env BOOTSTRAP_TIMEOUT=1800 DATA_PLANE_URL=http://127.0.0.1:8090 \
    bash "$src/scripts/tests/smoke-kind-e2e.sh" || return 1
  BENCH_RESULT=typed_failed
  local rc=0
  phase "bench$i" typed bx env \
      SMOKE_SHARD="$i/$BENCHES" \
      SPACE_M1_GENERATOR_MIRROR="${SPACE_M1_GENERATOR_MIRROR:-}" \
      SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE="$dir/admin-token" \
      SPACE_SMOKE_KIND_COLD_RECEIPTS="$dir/cold-receipts" \
      bash "$src/scripts/tests/lib/kind-parallel-typed.sh" "$dir/typed.log" || rc=$?
  BENCH_RESULT="typed_exit_$rc"
}

# --- run -------------------------------------------------------------------

if git -C "$REPO_ROOT" status --porcelain --untracked-files=no | grep -q .; then
  log "NOTE: the checkout has uncommitted changes; the Rust handoff identifies the image by commit"
fi

stale="$(kind_bench_existing | tr '\n' ' ')"
if [[ -n "${stale// /}" ]]; then
  log "removing benches left by an earlier run: $stale"
  teardown_all
fi

# Staged once on the host, mounted read-only into every bench's nodes.
export SPACE_VENDOR_ROOT="${SPACE_VENDOR_ROOT:-$HOME/.space-devbox/space/vendor}"

sample_resources &
BACKGROUND+=("$!")

phase run host_prep prepare_host >"$HOST/prepare.log" 2>&1 \
  || { tail -n 30 "$HOST/prepare.log" >&2; die "host preparation failed; see $HOST/prepare.log"; }
phase run registries start_registries >"$HOST/registries.log" 2>&1 \
  || { tail -n 30 "$HOST/registries.log" >&2; die "the host registries did not start; see $HOST/registries.log"; }

host_step rust rust_image
host_step shared-images shared_images
host_step build-cache build_cache
host_step seed seed
host_step inventory inventory

# The benches' directories are the caller's; only their Docker data roots are root's.
mkdir -p "$KIND_BENCH_ROOT" 2>/dev/null || sudo -n install -d -o "$(id -u)" -g "$(id -g)" "$KIND_BENCH_ROOT"
[[ -O "$KIND_BENCH_ROOT" ]] || sudo -n chown "$(id -u):$(id -g)" "$KIND_BENCH_ROOT"
declare -A BENCH_PIDS=()
for i in $(seq 1 "$BENCHES"); do
  mkdir -p "$OUT/benches/$i"
  run_bench "$i" &
  BENCH_PIDS[$i]=$!
  BACKGROUND+=("$!")
done
log "started $BENCHES benches; logs in $OUT/benches/<i>/bench.log"

for i in "${!BENCH_PIDS[@]}"; do
  wait "${BENCH_PIDS[$i]}" || true
  log "bench $i finished: $(cat "$OUT/benches/$i/result" 2>/dev/null || echo unknown)"
done
await inventory || true
FINISHED=1
# Torn down before the summary, so its time is in it.
if (( ! KEEP )); then
  phase run teardown teardown_all >"$OUT/teardown.log" 2>&1 || true
  rm -f "$HOST/shared-images.tar" "$HOST/rust/kind-rust.tar.gz"
  KEEP=2
fi

rc=0
python3 "$SCRIPT_DIR/lib/kind_parallel_report.py" summarize "$OUT" || rc=$?
log "summary: $OUT/summary.md"
exit "$rc"
