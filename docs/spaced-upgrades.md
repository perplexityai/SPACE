# Upgrading spaced in an existing sandbox

[Documentation](../README.md)

`spaced-svc` is the daemon inside the guest; `spaced` is its companion CLI.
Updating the host or publishing a new template does not replace these binaries
in an existing sandbox. Upgrade the pair from the same verified guest release.

## Three upgrade modes

| Mode | Status | Behavior |
|---|---|---|
| **1. Wake and upgrade** | Supported today as a manual operator procedure | Activate the sandbox through its normal lifecycle, replace and restart spaced, then persist the updated sandbox through normal snapshot publication. This is disruptive and counts as sandbox use. |
| **2. Upgrade opportunistically when the sandbox is running** | Planned; not implemented | Wait for a customer-initiated running period and a safe opportunity to upgrade, without waking a dormant sandbox solely for maintenance. Requires coordination with active workloads, retries, version tracking and persistence. |
| **3. Upgrade outside the customer-invoked lifecycle** | Planned; not implemented | Perform maintenance without advancing customer lifecycle/activity accounting or running customer workloads. Requires an isolated maintenance mechanism and coherent snapshot publication. |

Only mode 1 is currently supported. Manually upgrading a sandbox that happens
to be running is still this disruptive procedure; it is not an implementation
of the coordinated opportunistic mode. There is no automatic updater or
maintenance API providing modes 2 or 3.

### Effects of wake and upgrade

Treat the whole maintenance window as customer sandbox use. Normal activation
can allocate a new execution generation, place the sandbox on a worker and
materialize its artifacts. A retained execution can be reused, so not every
wake increments every generation. Access and commands update the corresponding
activity clocks (`last_access_at`, `last_command_at`, and aggregated
`last_used_at`) and affect lifecycle timers; the exact touches depend on the
API path and policy. Administrative SSH does not make this a lifecycle-neutral
operation. Once runtime billing is introduced, these maintenance periods are intended to
count toward the customer's bill.

Restoring a suspended VM resumes customer processes, which can modify files,
run timers and make network requests before maintenance starts. Restarting
spaced interrupts command connections and can terminate its managed workloads.
The current seed explicitly couples `space-workloads.service` to
`spaced.service`; older guests can have different unit layouts. Suspending
again records a new snapshot. It does not undo activity, placement history,
workload effects or elapsed runtime.

### Why offline disk editing is not mode 3

A full suspend snapshot contains the old spaced process and filesystem caches
in saved RAM. Replacing disk bytes while retaining that memory neither upgrades
the running process nor preserves a coherent filesystem view. Restoring while
paused keeps both customer processes and spaced frozen; it does not provide an
execution environment for the upgrade.

Encrypted artifacts and incremental btrfs streams additionally require their
own key and parent-lineage handling. Catalog digests are write-once, and workers
can retain cached artifacts. An S3 overwrite plus a new checksum is not a
supported replacement-selection mechanism. Mode 3 needs implementation across
execution, workload isolation, accounting and persistence; it is not an alternate
sequence of today's operator commands.

## Execute mode 1: wake and upgrade

This procedure covers a Linux/systemd guest with the standard executable paths.
Use normal SPACE lifecycle and backup operations so full snapshots, encryption,
and incremental storage remain the responsibility of their existing owners.
Do not edit snapshot metadata, catalog rows or S3 objects manually.

### 1. Prepare the maintenance window and release

Before waking the sandbox:

- Obtain approval for the exact sandbox and workload interruption. Agree on the
  final state, normally suspended for an originally suspended sandbox. Coordinate
  customer traffic so new work does not race the restart or final suspend.
- Record its complete ID, current state, selected snapshot, execution/placement,
  activity timestamps and lifecycle policy. Confirm normal durable snapshot
  publication is configured and available. A local-only suspend is not an S3
  backup; resolve that configuration before promising a durable upgrade.
- Select an immutable, tested Linux guest release for the guest architecture.
  Obtain `spaced-svc` and `spaced` from that release's build artifacts, with
  independently verified SHA-256 hashes. Check the deployed hosts' admission
  policy and the guest's existing kernel and units against [versioning](versioning.md).
  Do not assume a matching release number proves compatibility, or substitute
  host-side binaries for the guest binaries.
- Arrange authorized access to the worker that hosts the sandbox after wake,
  with `space-ssh`, access to its sandbox manager, visibility of the worker's
  VM PIDs and `/var/run/netns`, and permission to enter network namespaces.
  Provision a guest SSH key trusted by this sandbox. The guest session must
  survive a spaced restart. Keep credentials outside this repository.

Run the API commands in steps 2 and 5 on the operator machine with Bash, `curl`,
`jq` and Python 3. Run the guest-access commands in steps 3 and 4 from an
approved shell on the sandbox's **current worker**, with Bash, `space-ssh`,
OpenSSH, `sudo`, `nsenter`, util-linux `setsid` and `tar`. The guest needs Bash,
GNU coreutils, `tar` and systemd. Do not run the installation through a spaced
command session: stopping spaced can kill that session.

For Linux, [`space-ssh`](../crates/l50_app/ssh/cli/src/main.rs) resolves the
sandbox through the local manager, checks its VM PID and enters its network
namespace before calling SSH. The fixed guest address is not directly reachable
from an ordinary operator shell; plain `ssh` or `scp` does not select that
namespace. `space-ssh` does not wake the sandbox. Its current Linux path disables
SSH host-key checking, so verify the exact sandbox/worker/namespace association
through trusted operator access; do not describe it as host-key verification.

### 2. Wake through the normal lifecycle

Use the sandbox's owning API surface. This example uses the Native API and a
credential authorized for the sandbox's organization. For an E2B-compatible
caller, its resume maps to SPACE restore and its pause maps to SPACE suspend;
do not substitute Native pause for suspend.

```bash
set -euo pipefail
umask 077
: "${SPACE_API_URL:?Set the deployment API origin}"
: "${SPACE_API_TOKEN:?Set an authorized API token}"
API_ORIGIN=$(python3 - "$SPACE_API_URL" <<'PYTHON'
import sys
from urllib.parse import urlsplit
value = sys.argv[1]
if any(c.isspace() or ord(c) < 32 or ord(c) == 127 or c == "\\" for c in value):
    raise SystemExit("API origin must not contain whitespace, controls or backslashes")
url = urlsplit(value)
if (url.scheme not in {"http", "https"} or not url.hostname
        or url.username is not None or url.password is not None
        or url.path not in {"", "/"} or "?" in value or "#" in value):
    raise SystemExit("Use an HTTP(S) origin without credentials, path, query or fragment")
url.port  # Reject malformed ports before sending credentials.
if url.scheme == "http" and url.hostname not in {"127.0.0.1", "localhost", "::1"}:
    raise SystemExit("HTTPS is required except for loopback HTTP")
print(value.rstrip("/"))
PYTHON
)
readonly API_ORIGIN
SANDBOX_ID='<complete-sbx-id>'
RUN_DIR=$(mktemp -d /tmp/spaced-upgrade.XXXXXXXX)
api() {
  local path=$1; shift
  curl --disable --noproxy '*' --proto '=http,https' \
    --fail-with-body --silent --show-error --connect-timeout 10 --max-time 60 \
    -H "Authorization: Bearer $SPACE_API_TOKEN" \
    -H 'Content-Type: application/json' "$@" "$API_ORIGIN$path"
}
wait_state() {
  local path=$1 wanted=$2 state deadline=$((SECONDS + 1800))
  while (( SECONDS < deadline )); do
    api "$path" > "$RUN_DIR/poll.json"
    state=$(jq -er '.state' "$RUN_DIR/poll.json")
    [[ "$state" == "$wanted" ]] && return 0
    case "$state" in
      failed|canceled|expired|deleted|deleting|stopped|paused|suspended)
        cat "$RUN_DIR/poll.json"; return 1 ;;
    esac
    sleep 5
  done
  echo "Timed out waiting for $path; inspect before retrying" >&2
  return 1
}
api "/v1/sandboxes/$SANDBOX_ID" > "$RUN_DIR/before.json"
api "/v1/sandboxes/$SANDBOX_ID/snapshots" > "$RUN_DIR/snapshots-before.json"
if [[ ! -e "$RUN_DIR/wake-key" ]]; then
  (set -o noclobber; python3 -c 'import uuid; print(uuid.uuid4())' > "$RUN_DIR/wake-key")
fi
[[ -s "$RUN_DIR/wake-key" ]] || { echo "Empty wake key; inspect before retrying" >&2; exit 1; }
if [[ ! -s "$RUN_DIR/wake.json" ]]; then
  api "/v1/sandboxes/$SANDBOX_ID:wake" -X POST \
    -H "Idempotency-Key: $(cat "$RUN_DIR/wake-key")" > "$RUN_DIR/wake.json.tmp"
  jq -e '.operation_id | strings | select(startswith("op_"))' \
    "$RUN_DIR/wake.json.tmp" >/dev/null
  mv "$RUN_DIR/wake.json.tmp" "$RUN_DIR/wake.json"
fi
WAKE_OP=$(jq -er '.operation_id' "$RUN_DIR/wake.json")
wait_state "/v1/operations/$WAKE_OP" succeeded
wait_state "/v1/sandboxes/$SANDBOX_ID" running
```

Confirm the origin is the intended deployment before setting the token. The
validator allows HTTPS, or HTTP only on loopback, and rejects credentials,
paths, queries and fragments. The helper disables curl configuration files and
inherited proxies and does not follow redirects.

Keep `RUN_DIR`, the target sandbox ID and the original request keys. Run only
one operator session per directory. On a retry, restore these same values and
helper definitions; do not rerun the `mktemp` initialization. The wake and
suspend blocks reuse a saved operation response and poll it instead of posting
again. If a response was lost, they replay the same request with the same key.
An empty key or malformed saved response requires inspection, not a new key.
Do not reuse the directory for a different sandbox or a new maintenance cycle.

After wake, verify guest readiness through the normal client as in
[Your first sandbox](first-sandbox.md). Resolve
the current worker/guest route again: a stored address can belong to an older
placement. Identify its full canonical sandbox UUID in the manager. The Native
API `sbx_...` ID and the worker CLI selector are different representations;
use the resolved UUID below rather than assuming they are interchangeable.

### 3. Inspect and stage over administrative SSH

Inspect the guest's architecture, available disk space/inodes, memory and unit
layout. Allow space for both incoming binaries, backups and replacement files.
If storage or the guest is unhealthy, resolve that before restarting spaced.
The examples require regular executables at `/usr/local/bin/spaced-svc` and
`/usr/local/bin/spaced`, with `spaced.service` executing that service binary.
Custom paths, symlinks, capabilities or unit migrations need an adapted plan.

On the current worker, put the verified release binaries in `release/` using
your authorized worker-access path. Set the independently recorded hashes again
in this shell. Retain the worker-side evidence directory alongside the operator's
API-operation records. If placement changes, resolve the new worker before
continuing; do not keep using an old namespace or PID.

```bash
# Run in an approved shell on the sandbox's CURRENT WORKER.
set -euo pipefail
umask 077
SANDBOX_UUID='<full-canonical-sandbox-uuid>'
GUEST_SSH_KEY='<absolute-worker-path-to-authorized-guest-key>'
SVC_SHA256='<verified-spaced-svc-sha256>'
CLI_SHA256='<verified-spaced-sha256>'
[[ "$SVC_SHA256" =~ ^[0-9a-f]{64}$ && "$CLI_SHA256" =~ ^[0-9a-f]{64}$ ]] || exit 1
WORKER_RUN_DIR=$(mktemp -d /tmp/spaced-upgrade.XXXXXXXX)
guest() {
  env -u DISPLAY -u SSH_ASKPASS -u SUDO_ASKPASS SSH_ASKPASS_REQUIRE=never \
    setsid --wait space-ssh --user root --identity-file "$GUEST_SSH_KEY" \
    "$SANDBOX_UUID" "$@"
}
guest true </dev/null  # Exercise the actual manager, sudo/nsenter and SSH path.
guest 'id; uname -m; df -h / /var/tmp; df -i / /var/tmp; free -m;
  systemctl cat spaced.service;
  systemctl show spaced.service -p MainPID -p ControlGroup -p KillMode -p NRestarts;
  sha256sum /usr/local/bin/spaced-svc /usr/local/bin/spaced;
  /usr/local/bin/spaced-svc --version; /usr/local/bin/spaced --version'
STAGE=$(guest 'umask 077; mktemp -d /var/tmp/spaced-upgrade.XXXXXXXX')
[[ "$STAGE" =~ ^/var/tmp/spaced-upgrade\.[A-Za-z0-9]+$ ]] || exit 1
printf '%s\n' "$STAGE" > "$WORKER_RUN_DIR/guest-stage"
tar -C release -cf - spaced-svc spaced | guest "tar -C '$STAGE' -xf -"
```

The `guest` wrapper runs every call without a controlling terminal and disables
askpass helpers. The `true` preflight exercises the same user, key, namespace and
SSH path as the transfers; a password prompt must fail rather than wait on a
terminal during a binary stream. Configure non-interactive sudo permission and
a usable SSH key or agent before proceeding. If the preflight fails, stop and
resolve access; do not remove these controls to get a transfer started.

Verify that the administrative SSH session is outside the spaced/workload
cgroups that will be stopped. Preserve the installed units; a binary upgrade
alone does not install a new seed's service configuration or kernel. Ensure
there is enough time under the current lifecycle policy to finish the restart
and checks. Any approved temporary lifecycle-policy change is itself customer
state and must be recorded and restored; do not silently disable it.

### 4. Back up, stop, replace and start

In the same worker shell, create the installation script. It validates the staged
files inside the guest, backs up the installed pair, stops spaced, then replaces
both executables before starting it. Each rename is atomic, but the pair is not
one transaction; a failed or interrupted installation requires inspection and
rollback before starting the service.

```bash
cat > "$WORKER_RUN_DIR/install.sh" <<'SCRIPT'
set -euo pipefail
umask 077
stage=$1 svc_sha=$2 cli_sha=$3
[[ $(id -u) == 0 ]] || exit 1
cd "$stage"
printf '%s\n' "$svc_sha  spaced-svc" "$cli_sha  spaced" | sha256sum -c -
for bin in spaced-svc spaced; do
  [[ -f "/usr/local/bin/$bin" && ! -L "/usr/local/bin/$bin" ]] || exit 1
done
mkdir backup
cp -p /usr/local/bin/spaced-svc /usr/local/bin/spaced backup/
(cd backup && sha256sum spaced-svc spaced > SHA256SUMS)
systemctl cat spaced.service > spaced.service.before
systemctl show spaced.service -p MainPID -p NRestarts > service.before
sync
systemctl stop spaced.service
for bin in spaced-svc spaced; do
  next="/usr/local/bin/$bin.$(basename "$stage").new"
  install -o root -g root -m 0755 "$bin" "$next"
  mv -f "$next" "/usr/local/bin/$bin"
done
sync
systemctl start spaced.service
systemctl is-active --quiet spaced.service
pid=$(systemctl show spaced.service -p MainPID --value)
[[ "$pid" =~ ^[1-9][0-9]*$ ]] || exit 1
printf '%s\n' "$svc_sha  /proc/$pid/exe" \
  "$cli_sha  /usr/local/bin/spaced" | sha256sum -c -
/usr/local/bin/spaced-svc --version
/usr/local/bin/spaced --version
/usr/local/bin/spaced ping
systemctl show spaced.service -p MainPID -p NRestarts -p ExecMainStatus
SCRIPT
guest "cat > '$STAGE/install.sh'" < "$WORKER_RUN_DIR/install.sh"
guest "bash '$STAGE/install.sh' '$STAGE' '$SVC_SHA256' '$CLI_SHA256'" \
  > "$WORKER_RUN_DIR/install.log" 2>&1
cat "$WORKER_RUN_DIR/install.log"
```

If SSH disconnects or the script fails, inspect its log and the service over
SSH before retrying. Do not rerun it over the existing backup. Check
`journalctl -u spaced.service` in the guest, confirm there is no restart loop,
and compare the reported versions with the chosen release. A CLI `--version`
prints the on-disk version; hashing `/proc/<MainPID>/exe` verifies the daemon
actually running. The ping checks the guest connection, not the whole API path.

Through the normal SPACE client, run a small approved command and verify its
output and exit status. Reconnect the guest process endpoint and check host-side
guest readiness. This verification is ordinary command activity and belongs to
the maintenance window. Do not declare success based only on systemd being active.

### 5. Suspend and verify durable publication

Return to the operator shell from step 2, retaining its `RUN_DIR` and API
functions. After successful checks, coordinate the final suspend with the
customer. For an originally running sandbox, do not unexpectedly suspend new customer work;
agree on the persistence/final-state window first. Restore any approved temporary
policy changes and remove only disposable staging files, retaining the recorded
binary backups for the agreed rollback period.

```bash
if [[ ! -e "$RUN_DIR/suspend-key" ]]; then
  (set -o noclobber; python3 -c 'import uuid; print(uuid.uuid4())' > "$RUN_DIR/suspend-key")
fi
[[ -s "$RUN_DIR/suspend-key" ]] || { echo "Empty suspend key; inspect before retrying" >&2; exit 1; }
if [[ ! -s "$RUN_DIR/suspend.json" ]]; then
  api "/v1/sandboxes/$SANDBOX_ID:suspend" -X POST \
    -H "Idempotency-Key: $(cat "$RUN_DIR/suspend-key")" > "$RUN_DIR/suspend.json.tmp"
  jq -e '.operation_id | strings | select(startswith("op_"))' \
    "$RUN_DIR/suspend.json.tmp" >/dev/null
  mv "$RUN_DIR/suspend.json.tmp" "$RUN_DIR/suspend.json"
fi
SUSPEND_OP=$(jq -er '.operation_id' "$RUN_DIR/suspend.json")
wait_state "/v1/operations/$SUSPEND_OP" succeeded
wait_state "/v1/sandboxes/$SANDBOX_ID" suspended
api "/v1/sandboxes/$SANDBOX_ID/snapshots" > "$RUN_DIR/snapshots-after.json"
jq '.data[] | {id, state, created_at}' "$RUN_DIR/snapshots-after.json"
```

Wait for the normal backup publisher to finish. Correlate the new snapshot ID
with this suspend and execution, and verify its catalog state is `ready` with
its intended durable S3 storage location. Follow pagination if needed. A
`suspended` response, a queued upload, or an older ready snapshot is not proof
that this upgrade is durable. If publication fails, retain the local artifact
and investigate the normal backup failure; do not overwrite S3 manually or
remove its only copy. Leave the task incomplete until publication succeeds.

Record the release digest and hashes, old/new versions, service checks,
operation IDs, new snapshot ID and final sandbox state. A full suspend now
captures the new daemon in memory along with the updated filesystem. Verify it
at the next agreed restore, or perform an additional approved wake/check/suspend
cycle now; that verification causes another ordinary lifecycle cycle. Retain
the compatible host/kernel/template dependencies needed to restore it.

### Rollback after a failed guest restart

From the current worker, open the guest session with
`space-ssh --user root --identity-file "$GUEST_SSH_KEY" "$SANDBOX_UUID"` and
use the recorded guest staging directory. Before executing this block, verify
that its backup is the original pair for this upgrade and that no other operator has completed a newer upgrade.

```bash
# Run as root INSIDE the same guest over administrative SSH.
set -euo pipefail
STAGE='<recorded-guest-stage-directory>'
cd "$STAGE/backup"
sha256sum -c SHA256SUMS
systemctl stop spaced.service
for bin in spaced-svc spaced; do
  next="/usr/local/bin/$bin.$(basename "$STAGE").rollback"
  install -o root -g root -m 0755 "$bin" "$next"
  mv -f "$next" "/usr/local/bin/$bin"
done
sync
systemctl start spaced.service
systemctl is-active --quiet spaced.service
/usr/local/bin/spaced ping
```

Repeat the live-daemon hash, version, API-path and stability checks. Rollback
restores binaries, not killed processes, external effects, activity timestamps
or generations. If a failed upgrade has already been snapshotted, reverting the
binaries requires another normal activation and persistence cycle. Do not roll
back the whole customer disk to an older snapshot without separately accounting
for intervening writes.

## Implementation references

- [Execution-generation allocation](../crates/l40_api/api/src/controllers/substrate/kubernetes/store/execution_rows.rs).
- [Activity clocks and lifecycle timers](../crates/l30_storage/sbx/mgr/store/src/auto_lifecycle.rs).
- [Guest workload ownership on daemon restart](../crates/l40_api/spaced/guest/server/src/cgroup.rs) and [seed units](../templates/firecracker/seed/rootfs-builder.sh).
- [Snapshot capture and publication](../crates/l40_api/sbx/mgr/server/src/service/suspend_snapshot_publication.rs).
- [Snapshot metadata](../crates/l40_api/sbx/mgr/server/src/snapshot_upload.rs) and [write-once digests](../crates/l30_storage/snapshot/store/src/lib.rs).
