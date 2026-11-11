#!/usr/bin/env bash
# Opt-in, host-local KVM experiment. No scheduler, service, or fleet integration.
set -euo pipefail
umask 077

usage() {
  cat <<'HELP'
Usage: kvm-halt-poll-canary.sh plan|apply|restore [options]

Required:
  --host-id ID             Stable, unique HOST identity, not a container hostname
  --seed SEED              Fixed experiment seed; keep unchanged across the fleet
  --percent N              Integer treatment percentage, 0..100
  --poll-ns N              Treatment cap in nanoseconds (0 disables host polling)
  --expected-current-ns N  Required baseline; refuse unexpected values

Optional:
  --parameter-file PATH   Default: /sys/module/kvm/parameters/halt_poll_ns
  --state-dir PATH        Default: /var/lib/space/kvm-halt-poll-canary
  -h, --help              Show this help

plan is read-only and requires no root privileges. apply and restore require
root. Requires Linux, Bash 4+, GNU coreutils, and flock (util-linux).
SHA-256(seed, host ID) assigns approximately N% of hosts to treatment;
this is NOT an exact fleet-size quota. Nonselected hosts' caps are never modified.
All hosts must use identical experiment arguments. Changing the seed reshuffles
the cohort. Restore the old experiment before starting a new seed or target.

apply saves the original cap before writing, verifies readback, and emits a UTC
timestamp plus assignment and observed cap. Reapplying is idempotent. restore
uses the saved baseline, not a guessed default, and refuses to overwrite an
unexpected cap. State is scoped to the host identity, experiment, and boot.
Reboot resets runtime sysfs settings; rerun apply if continuing the experiment.
Do not run simultaneous experiments or delete state before rollback.

Example (synthetic host; preview first, then repeat with apply as root):
  kvm-halt-poll-canary.sh plan --host-id worker-001 --seed example-v1 \
    --percent 25 --poll-ns 10000 --expected-current-ns 200000
Use identical arguments with restore for explicit rollback on every host.

Run on the host, or provide the HOST's mounted parameter file and a persistent
host state directory. This does not check per-VM KVM_CAP_HALT_POLL overrides,
collect polling counters, measure guest health, or export metrics. A successful
readback establishes only the host setting. Record assignments externally;
host-count percentage is not traffic percentage. No automatic rollout or
rollback is installed. Removing the invoking container does not undo a write.

Kernel behavior and overrides:
https://docs.kernel.org/virt/kvm/halt-polling.html
HELP
}

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
uint() { [[ $1 =~ ^(0|[1-9][0-9]{0,9})$ ]] && (( $1 <= 4294967295 )); }

if [[ ${1:-} == --help || ${1:-} == -h ]]; then usage; exit 0; fi
mode=${1:-plan}
(( $# == 0 )) || shift
case "$mode" in plan|apply|restore) ;; *) usage >&2; exit 2 ;; esac
host_id='' seed='' percent='' poll_ns='' expected=''
parameter=/sys/module/kvm/parameters/halt_poll_ns
state_dir=/var/lib/space/kvm-halt-poll-canary
while (( $# )); do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --host-id|--seed|--percent|--poll-ns|--expected-current-ns|--parameter-file|--state-dir)
      (( $# >= 2 )) || die "missing value for $1"
      case "$1" in
        --host-id) host_id=$2 ;;
        --seed) seed=$2 ;;
        --percent) percent=$2 ;;
        --poll-ns) poll_ns=$2 ;;
        --expected-current-ns) expected=$2 ;;
        --parameter-file) parameter=$2 ;;
        --state-dir) state_dir=$2 ;;
      esac
      shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done
[[ -n $host_id && -n $seed ]] || die "--host-id and --seed are required"
uint "$percent" || die "--percent must be 0..100"
(( percent <= 100 )) || die "--percent must be 0..100"
uint "$poll_ns" || die "--poll-ns must be an unsigned 32-bit integer"
uint "$expected" || die "--expected-current-ns must be an unsigned 32-bit integer"
[[ $parameter == /* && $state_dir == /* ]] || die "paths must be absolute"
[[ -r $parameter && ! -L $parameter ]] || die "parameter must be a readable, nonsymlink file"
current=$(<"$parameter")
uint "$current" || die "parameter does not contain an unsigned 32-bit integer"
digest=$(printf '%s\0%s' "$seed" "$host_id" | sha256sum)
digest=${digest%% *}
bucket=$(( 16#${digest:0:8} % 10000 ))
cohort=control
(( bucket >= percent * 100 )) || cohort=treatment
boot_id=$(</proc/sys/kernel/random/boot_id)

report() {
  printf 'timestamp=%s mode=%s host_id=%q seed=%q boot_id=%s bucket=%s cohort=%s percent=%s target_ns=%s original_ns=%s observed_ns=%s result=%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$mode" "$host_id" "$seed" "$boot_id" \
    "$bucket" "$cohort" "$percent" "$poll_ns" "$original" "$current" "$1"
}

original=$expected
if [[ $mode == plan ]]; then
  report preview
  exit 0
fi
(( EUID == 0 )) || die "$mode requires root"
# Reject symlinked or writable-by-others ancestors before creating root state.
ancestor=$state_dir
while [[ $ancestor != / ]]; do
  [[ ! -L $ancestor ]] || die "state path contains a symlink: $ancestor"
  if [[ -e $ancestor ]]; then
    [[ -d $ancestor && $(stat -c %u "$ancestor") == 0 ]] ||
      die "state path must have root-owned directory ancestors"
    permissions=$(stat -c %a "$ancestor")
    (( (8#$permissions & 0022) == 0 )) || die "state path is writable by others"
  fi
  ancestor=$(dirname "$ancestor")
done
mkdir -p -- "$state_dir"
exec 9>"$state_dir/lock"
flock -x 9
state="$state_dir/$digest-$boot_id"
if [[ -e $state || -L $state ]]; then
  [[ -f $state && ! -L $state ]] || die "invalid saved-state file"
  mapfile -t saved <"$state"
  [[ ${#saved[@]} == 3 ]] || die "invalid saved-state contents"
  original=${saved[0]}
  uint "$original" || die "invalid saved baseline"
  [[ ${saved[1]} == "$poll_ns" && ${saved[2]} == "$percent" && $original == "$expected" ]] ||
    die "experiment arguments changed; restore with the original arguments first"
  [[ $cohort == treatment ]] || die "saved treatment is now control; restore original experiment"
fi
# Re-read after locking; do not overwrite an unrelated operator's changed cap.
current=$(<"$parameter")
uint "$current" || die "invalid current cap"
if [[ $cohort == control ]]; then
  [[ $current == "$expected" ]] || die "control cap differs from expected baseline"
  report unchanged
  exit 0
fi
if [[ $mode == restore ]]; then
  if [[ ! -f $state ]]; then
    [[ $current == "$expected" ]] || die "no saved baseline for this host/boot; refusing restore"
    report already-baseline
    exit 0
  fi
  [[ $current == "$poll_ns" || $current == "$original" ]] ||
    die "cap changed outside this experiment; refusing restore"
  printf '%s\n' "$original" >"$parameter"
  current=$(<"$parameter")
  [[ $current == "$original" ]] || die "restore readback mismatch"
  # Keep the journal so a repeated restore verifies the same baseline.
  report restored
  exit 0
fi
if [[ ! -f $state ]]; then
  [[ $current == "$expected" ]] || die "cap differs from expected baseline"
  original=$current
  temporary=$(mktemp "$state_dir/.baseline.XXXXXX")
  trap 'rm -f -- "${temporary:-}"' EXIT
  printf '%s\n%s\n%s\n' "$original" "$poll_ns" "$percent" >"$temporary"
  sync -f "$temporary"
  mv -- "$temporary" "$state"
  sync -f "$state_dir"
fi
[[ $current == "$original" || $current == "$poll_ns" ]] ||
  die "cap changed outside this experiment; refusing apply"
printf '%s\n' "$poll_ns" >"$parameter"
current=$(<"$parameter")
[[ $current == "$poll_ns" ]] || die "apply readback mismatch; restore saved baseline"
report applied
