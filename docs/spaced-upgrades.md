# Upgrading spaced in an existing sandbox

[Documentation](../README.md)

`spaced-svc` is the daemon inside the guest; `spaced` is its companion CLI.
Updating the host or publishing a new template does not replace these binaries
in an existing sandbox. Upgrade the pair from the same verified guest release.

## Three upgrade modes

| Mode | Status | Behavior |
|---|---|---|
| **1. Wake and upgrade** | Supported today as a manual operator procedure | Activate the sandbox through its normal lifecycle, replace and restart spaced, then persist the updated sandbox through normal snapshot publication. This is disruptive and counts as sandbox use. |
| **2. Upgrade a running sandbox through its manager** | Experimental, explicit operator selection | Stage an authorized bundle and restart only when idle, or with explicit force. Requires a qualified two-slot guest template; never wakes dormant sandboxes. |
| **3. Upgrade outside the customer-invoked lifecycle** | Planned; not implemented | Perform maintenance without advancing customer lifecycle/activity accounting or running customer workloads. Requires an isolated maintenance mechanism and coherent snapshot publication. |

Automatic fleet rollout is disabled. The manager-owned mode below is available
only on templates built with the selector contract; older guests retain the
manual procedure. Offline maintenance remains unimplemented.

## Manager-owned guest update and restart

Deploy capable host services **and the OCI build engine on every builder**
before publishing an updater-capable template. An older OCI engine cannot
preserve the slot layout required by the new seed's systemd drop-in; using it
with that seed produces an unbootable agent.
The seed builder recognizes `artifact.json` beside its `spaced-svc` and `spaced`
inputs and installs two slots, the committed selector, and systemd hooks. OCI
assembly preserves that layout and installs a complete verified binary pair.
Without that descriptor, a seed stays on the legacy layout. Do not convert a
running legacy guest by copying symlinks or unit fragments into it.

For local Kind development, commit the candidate before building an
updater-capable seed. Dirty image provenance contains a `-dirty-` suffix and
intentionally produces the legacy layout; it cannot identify a release bundle.
A clean commit is necessary but not sufficient: supply the verified descriptor
and qualify the exact resulting template before selecting it in policy. Worker
image builds also require `SPACE_BUILD_TIME` from the source commit timestamp;
Kind and bundle CI supply it automatically. Do not use rebuild wall time.

Before publishing a guest release, check the installed selector ABI against
both the old and new binaries. Additive diagnostic journal fields may remain
on schema 1; a new phase, changed decision semantics, changed hashing, or an
incompatible selector CLI requires a new seed contract and explicit host
qualification. Unknown phases are not additive diagnostics: the old selector
falls back to its committed link. Test interrupted commit/link repair with
the previous committed selector, not only the new binary.

Linux uses systemd's pre-start selector and startup timeout. The experimental
Cloud Hypervisor + Windows backend uses SCM plus bounded Task Scheduler
recovery in the existing agent binary; see the [Windows recovery contract](../templates/cloud-hypervisor/windows/README.md#guest-update-recovery).
VMM choice does not change maintenance ownership. The shared RPCs describe
logical payloads, slots and execution/boot fences; SQLite receipts remain
host-owned. No SQL or protobuf replacement is required for Windows.
Unknown installed contracts are unsupported. Host artifact selection matches
the installed recovery ABI and architecture; it never tries the Linux installer
on another OS. The catalog selects independent target bundles by recovery contract and
architecture, so one node can authorize Linux and Windows simultaneously.
Changing one entry does not withdraw another entry's authority.

A release bundle carries the ordinary workspace version, source revision,
diagnostic build time, binary hashes and `guest_update_required_since`. Normal
selection requires both its version and that marker to exceed the installed
guest version. For example, target 0.8.145 with marker 0.8.143 leaves a guest at
0.8.143 running but updates a guest at 0.8.140 to 0.8.145. Equal-version bytes
with a different digest are rejected. A build date is never an ordering key.
See [versioning](versioning.md) for immutable release allocation prerequisites.

Node installation distributes verified artifacts into the root-owned directory
`/opt/space/guest-update/<artifact-digest>/` and installs root-owned
`/etc/space/guest-update/policy.json`. All parent directories and files must be
non-writable by group/other; the manager rejects symlinks. Use the installer as
part of a direct service deployment (or `--root` for an offline image):

```sh
sudo python3 scripts/build/install-agent-bundle.py /path/to/verified-bundle
```

Repeat for another platform's bundle; the installer preserves other entries.
For container deployments, bake **all** supported platform bundles and the
complete catalog into the worker image with `--root`. The stock worker image
contains Linux only. A host-side installation is not mounted into the container,
and edits inside a running container do not survive replacement. Publish a
custom worker image containing Windows too for mixed-platform container nodes;
do not use ad-hoc installs into a running pod as deployment configuration.
It verifies both payload identities, publishes the bundle before atomically
replacing policy, and retains authorization when reinstalling the same selection.
New authorization is a content-derived UUIDv8 over the complete promotion, so
identical offline image rebuilds also retain receipt authority.
The guest ceiling defaults to that exact target version and the host floor to
the bundle's workspace version; `--minimum-host` requires compatibility review.
Same-version replacement with different bytes is refused. The policy is keyed
by installed recovery contract and architecture:

```json
{
  "targets": {
    "linux-systemd-selector-v1": {
      "x86_64": {
        "authorization": "019b0000-0000-7000-8000-000000000001",
        "promotion": {
          "channel": "stable",
          "target": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
          "target_version": "0.8.145",
          "guest_update_required_since": "0.8.143",
          "intent": "upgrade",
          "minimum_host": "0.8.143",
          "maximum_guest": "0.8.145"
        }
      }
    }
  }
}
```

These are illustrative identities, not installable values. The catalog selects
host-authorized bundles by recovery contract and architecture; it has no template
digest allowlist or publisher attestation. Manager-owned maintenance requires a
committed guest version of at least 0.8.2 and a guest-reported recovery contract
matching the trusted image profile. Local catalog authority is rechecked at Arm
and Commit, without a template-manager lookup. Earlier guests keep serving work;
use recreation or the separate manual procedure to reach this baseline.

Eligibility deliberately trusts guest-reported capability. Preserve the installed
recovery units/tasks and slots; broken supervision may require manual repair,
regardless of the manager's bounded operation deadline.

The ceiling must cover permitted placement/rollback hosts and does not bypass
protocol or platform compatibility. Windows bundles occupy their own
`windows-scm-two-slot-v1` entry, installed using the same command for direct
services or baked into the complete worker image for container deployments.

The CLI selects the appropriate entry using guest status; the manager
independently checks trusted image identity. Install the manager, CLI and catalog
as one node release. Replacing one platform entry never withdraws
another platform's receipt authority. Drain unfinished operations before changing
a target's authority. Malformed policy files are rejected before installation;
after draining guest updates, remove the invalid file and reinstall each platform
target to recreate the catalog. The installer does not translate other formats.

For installer reauthorization, default authority is derived from promotion
identity. Returning A → B → A or removing and reinstalling A reuses the original
authority and consumed budget. After investigation, pass
`--authorization <fresh-uuidv7>` to permit another bounded attempt, and retain
that value in deployment inputs. Identical reinstalls preserve it. Do not use
reauthorization as an automatic retry loop. Container deployments must bake the
complete catalog into their worker image; direct-service installs may merge entries.

The host socket retains its root-or-sandbox-manager UDS trust boundary. Root
is a trusted node administrator and can invoke its maintenance primitives;
this is not a security boundary against root. Operational tooling must use the
manager commands below to obtain durable authorization and lifecycle fencing.

Run on the sandbox's current worker, as an authorized local operator:

```sh
space-sbxmgr guest-update status <sandbox-id> --json
space-sbxmgr guest-update apply <sandbox-id> --json
space-sbxmgr guest-update status <sandbox-id> --operation <operation-id> --json
```

Both mutation commands generate a UUIDv7 operation ID and discover the current
execution automatically. Neither ID flag is required. Before submission the CLI
prints the selected retry flags to stderr, leaving JSON on stdout unchanged.
Apply/restart return an operation receipt immediately. Poll that same operation;
reuse `--operation` and `--expected-execution` on `apply` or `guest-restart`
using the printed IDs after an ambiguous response. Automation may supply both
flags upfront when it needs to persist its own request identity. `status` accepts
`--operation`, but not
`--expected-execution` or `--force`. `apply --policy` defaults to
`/etc/space/guest-update/policy.json`; it must match the worker's configured
authority, rather than granting new authority from the CLI.
A normal request defers without interrupting active work, process attachments,
retained results/retry records, filesystem watches, descendants or forwarding.
Process records can remain relevant for 15 minutes after exit. Long-lived
background services and short idle-to-suspend policies can prevent any eligible
window. Maintenance does not extend lifecycle deadlines or activity clocks.

`guest-update apply --force` and `guest-restart --force` permit terminating API
workloads and their descendants. They retain compatibility, storage and
execution checks, and do not kill independent SSH/template services. Force is
not rollback authorization and does not provide an independent kill channel
when the guest bridge is already hung. Systemd supervises crashes; no watchdog
is enabled. Never automatically replay interrupted customer commands.

For a separate restart that intentionally terminates active API workloads, run:

```sh
space-sbxmgr guest-restart <sandbox-id> --force --json
```

A rollback requires a fresh operator authorization selecting an older immutable
release with `intent: "rollback"`. Forward release history survives rollback;
reusing a lower version or an operation UUID does not erase it. Retry allowance
is at most two activations per execution and authorization. A normal candidate
that passed its self-test but failed to commit gets one retry after at least
five minutes plus jitter. If the retry again rolls back after its self-test
without committing, the receipt ends with `retry_exhausted`. Reaching the
host-wait phase does not prove host fault. Earlier
candidate failure quarantines the selection. Forced maintenance never retries
interruption automatically. A new authorization permits another bounded
attempt after the operator has investigated the outcome. Lifecycle cancellation
before activation adds no new backoff; cancellation after activation retains
the five-minute delay plus jitter to prevent restart churn. Existing backoff
is never shortened by cancellation.
A busy refusal does not consume an activation if the guest durably aborts the
staged attempt and confirms the original daemon is still running its committed
bundle. The refund and receipt settlement are atomic; repeated settlement cannot
refund twice. A timeout, missing evidence, or changed guest generation does not
justify a refund.
Previously used authorizations cannot be reintroduced to replenish that
allowance. The worker activates at most one guest at a time; other accepted
receipts remain queued. The rollout owner must implement a per-target
failure-rate breaker in the fleet admission controller before enabling automatic rollout. It must stop
new admissions for a failing target across workers; this operator-only
mechanism neither runs that controller nor enables automatic rollout.

Timeout changes persist while maintenance runs. Stop/delete revokes maintenance
authority. Explicit pause, suspend and memory-capture requests preempt optional
maintenance. A scheduled lifecycle event within the activation budget plus its
30-second margin, or expiration of the operation deadline, also defers arming
or aborts a running trial. During activation, the lifecycle cutoff uses the
fixed activation finish time plus the margin, rather than a sliding window.
Inventory reads and other ordinary artifact-lock waiters do not preempt maintenance.
A queued lifecycle capture cancels a transfer before activation;
partial bytes never authorize a restart. Once armed, pause, suspend and
checkpoint must wait for a settled, configured guest before capturing memory.
Settlement retains the conservative 210-second activation budget; an
unreachable guest can cause the capture request to fail. There is no promise
of immediate pause or suspend while a trial is already running. A broken guest
that cannot flush or settle still
requires normal stop/delete or operator recovery, rather than an unsafe
snapshot. Capture quiesces only the metadata subscription. The manager owns
lifecycle routing; actual guest-update/reconfiguration holds own customer
admission. A failed capture therefore cannot leave a metadata pause blocking
customer traffic, including on legacy, pool and builder guests without a
configuration observer. A paused execution with a pending receipt from older
host code must be explicitly resumed to settle it before another capture;
maintenance never wakes it implicitly. Successful checkpoints restart metadata
observation after VM resume. Capture requires fresh maintenance status; an I/O
or transport failure prevents capture because cached stability cannot exclude a
new trial. This read does not create a configuration hold for pool or builder guests.
Forwarded connections refused during maintenance are reset and logged at debug
level; they are not queued across a guest replacement.

During an operation-owned guest update, new command, file and forwarding
requests wait at spaced-host until the configured terminal generation is ready.
Authentication and existing request limits run before this wait; maintenance and
configuration RPCs have separate capacity. Waiting does not decode stream bodies
or count as guest work. Cancellation or deadline expiry removes the waiter without
executing it later. The wait uses the caller's remaining deadline and a fixed
210-second cap from the first Hold; repeated Hold calls do not extend it. Shorter
client deadlines can still fail before dispatch. Ordinary configuration-only
holds retain their existing refusal behavior.

The trial self-test spawns the real filesystem worker, resolves/validates its
account, checks its protocol handshake and stats `/` through it. It does not
exercise the customer process API; qualify command execution separately in
the deployment canary before selecting the release broadly.

The guest keeps exactly two bundle slots. Partial transfer uses the inactive
slot, with capacity checks and no extra archive. Reuse refuses while an old
binary remains mapped or open. Systemd invokes the short selector from the
committed slot; an unready candidate times out and the next start selects the
fallback. A durable commit record, not readiness notification, commits the
release. The host reapplies process environment and forwarding before releasing
its hold. Recovery reconciles the persisted manager receipt with fresh guest
status. Guest fsync orders journal changes but does not guarantee survival of
a host failure before host writeback; do not infer the guest commit from the
manager receipt alone after that failure.
Host-only and manager-only restarts adopt a surviving guest without closing
customer admission. A changed guest incarnation triggers reconfiguration;
transport disconnection alone does not. Simultaneous host/guest loss has no
surviving host observation, so ordinary crash configuration retains the existing
best-effort guarantee outside an explicit guest-update hold.

In protocol v1, equal operation/execution IDs are reserved for background
configuration ownership. Shared owner parsing and constructors enforce this
convention; manager operation RPCs reject that reserved combination before
probing or recording intent. Root remains a trusted node administrator.

Confirmation failures include the operation ID and status in warning logs.
Terminal rejections stop repeated confirmation; the next durable guest status
still decides whether commit happened. Receipts retain bounded typed outcomes,
not arbitrary remote error text; correlate the operation ID with host logs for
the detailed cause. A lost reply or deadline is never proof that commit failed;
a viable trial retries confirmation after reading fresh status.

An unchanged committed start needs no writable metadata. Recovery transitions
still need to persist journal/link changes; a disk that becomes full or read-only
during a transition can prevent startup and requires operator repair. Slot reuse
keeps inode-based checks for mapped/open binaries, including hardlinks. Unreadable
process metadata or traversal limits defer staging; special-filesystem fd metadata
can delay the scan.

A missing or malformed journal enters `RecoveryRequired` after verifying the
committed bytes. Serving can resume, but guest maintenance remains disabled:
the lost receipt might contain a consumed attempt, so treating it as `Stable`
would erase replay protection. Preserve both slots and the available metadata
for diagnosis. The selector replaces a malformed `update-state` with its recovery
record; the original bytes are no longer available afterward. If corruption is
detected before restart, preserve that journal before the selector runs.
Before retiring the execution, recover unbacked-up customer data
or verify a current backup. Delete discards the disk, and stop can proceed after
a failed or timed-out flush; neither is a data-recovery step. Use the qualified
SSH/VM recovery path if ordinary guest access is unavailable. Once data is safe,
retire the execution through normal stop/delete and provision from a qualified
template; do not reset the journal on a live execution or reuse its authorization.
See [guest I/O lifecycle boundaries](space/design/guest-io.md#lifecycle-boundaries)
for the persistence guarantees.

The selector-v1 trial window is 30 seconds from selection, capped by the
attempt's overall expiry. The manager's 420-second operation and 210-second
activation/reconciliation budgets include staging, shutdown and fallback; they
are not extra candidate confirmation time. The seed unit's startup timeout
must cover the trial and selector overhead; qualify changes together with the
selector contract. An absent host cannot extend the candidate's startup.

Drain unfinished maintenance before rolling host services back, and verify
that the destination manager accepts the database's applied migration catalog.
Draining does not make a pre-protocol manager able to reopen the migrated
SQLite database. An image-only rollback to an incompatible catalog is
unsupported. The guest-update down migration drops receipt and policy history;
it is not a state-preserving rollback procedure. Preserve verified backups and
use a reviewed recovery plan that retains the required state, following the
[database rollback guidance](../db/README.md).

If the committed selector itself is broken, preserve both slots
and use the qualified independent SSH or VM-level recovery path. The two-slot
bound is a guest-visible payload bound; deleting a template-shared file does
not reclaim the template's host CoW extents.

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

This legacy procedure covers a Linux/systemd guest with regular files at the
standard executable paths. It intentionally rejects the two-slot symlinks; use
the manager-owned operations above for those guests.
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
- [Guest workload ownership on daemon restart](../crates/l40_api/spaced/guest/server/src/platform/unix/cgroup.rs) and [seed units](../templates/firecracker/seed/rootfs-builder.sh).
- [Snapshot capture and publication](../crates/l40_api/sbx/mgr/server/src/service/suspend_snapshot_publication.rs).
- [Snapshot metadata](../crates/l40_api/sbx/mgr/server/src/snapshot_upload.rs) and [write-once digests](../crates/l30_storage/snapshot/store/src/lib.rs).
