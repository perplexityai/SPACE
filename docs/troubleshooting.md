# SPACE: Troubleshooting

[Documentation](../README.md)

First identify which stage failed: local setup, API authentication, a sandbox operation, or a command stream. An accepted request, a running sandbox, and a successful command are three different outcomes.

## Local installation

This section covers the [Kind installation](installation.md). Use the explicit `kind-space-local` context so diagnostics target that local deployment, not another cluster. If you selected another cluster name, replace the context accordingly.

| Symptom | Check and action |
|---|---|
| `required tool not on PATH: kind` or `command not found: tilt` | Complete [Install and verify tools](installation.md#install-and-verify-tools), including the Bash startup configuration and fresh-login check. The bring-up does not install tools. |
| Tools work in one terminal but not a new one | Confirm that `$HOME/.local/bin` is on PATH in the new Bash shell. Repeat the startup configuration and fresh-login check in [Install and verify tools](installation.md#install-and-verify-tools); another inline export alone will not fix future logins. |
| SSH disconnects during a build | Reconnect to the same host and run `tmux attach-session -t space-setup`. Inspect the running command before starting anything else; if you did not use tmux, check whether the original build is still running before retrying. |
| Running setup from a Mac | Use a Linux x86_64 host with KVM and run the setup there over SSH. Installing the command-line tools on macOS does not satisfy the host requirements. |
| `/dev/kvm` or `/dev/net/tun` is missing, or storage setup fails | Check device access, nested virtualization, privileged Docker operations, loop devices, and btrfs support on the host. |
| Package installation fails | Install the packages named in the error, or provide root/non-interactive sudo access. Rerun the failed step rather than skipping it. |
| Pods remain pending or restart | Run the commands below and inspect the first failing pod's events and container logs. Confirm the host supports the required privileged containers and storage operations. |
| `seed:latest` cannot be resolved | Check `space-seed-publish` logs and completion. The seed must be built and published before sandbox creation. |
| Login rejects `root@local` | Use the password selected before the bring-up (`password` by default). Confirm the bring-up completed and Tilt's `provision-local-console` resource succeeded; rerunning does not reset an existing password. |
| API or data plane refuses connections | Check service readiness: `kubectl --context kind-space-local -n space get pods,endpoints,svc` should show ready pods behind `space-api-entry` and `space-data-plane-entry`. localhost reaches them through the cluster's port mappings (`docker port space-local-worker3`); a cluster created before those mappings is refused by the bring-up with the command to recreate it. API reachability does not prove data-plane reachability. |
| `curl: (52) Empty reply from server` | A replica may have been replaced mid-request, or, for the UI and object storage, Tilt's port forward may be reconnecting after Tilt replaced its target pod. Check the forwarding logs before changing credentials. |

```bash
kubectl --context kind-space-local -n space get pods
kubectl --context kind-space-local -n space get events \
  --sort-by=.metadata.creationTimestamp
kubectl --context kind-space-local -n space logs job/space-seed-publish
kubectl --context kind-space-local -n space \
  logs job/space-db-migrate -c migrate
tilt logs provision-local-console --tail 50
```

For an individual pod, use `kubectl --context kind-space-local -n space describe pod <pod-name>` and `kubectl --context kind-space-local -n space logs <pod-name> -c <container-name>`. Use `--previous` to read the previous instance's logs after a container restart.

Sandbox pods (`space-sbx-…`) run in their own namespace, `sandboxes`, not in `space`: list them and their events with `kubectl --context kind-space-local -n sandboxes get pods,events`. NetworkPolicies there admit only the API and cluster-controller on port 9800; `kubectl --context kind-space-local -n sandboxes get networkpolicies` shows them.

For an empty reply, inspect the Tilt resource that holds the forward (`space-ui` for port 3000, `space-object-store` for port 8333) for port-forward errors, and wait for its pod to be ready again before retrying:

```bash
for resource in space-ui space-object-store; do
  printf '\n== %s ==\n' "$resource"
  tilt logs "$resource" 2>&1 | grep -i 'port.forward' | tail -n 20
done
```

An empty reply does not establish whether a mutation reached the server. Inspect its outcome and retain the original request body and idempotency key before retrying.

If a port forward is absent, start the one you need in a separate terminal and leave it running. Each command below runs in the foreground, and requires its local port to be free.

```bash
kubectl --context kind-space-local -n space \
  port-forward --address=127.0.0.1 svc/space-ui 3000:3000
```

```bash
kubectl --context kind-space-local -n space \
  port-forward --address=127.0.0.1 svc/object-store 8333:8333
```

## Sandbox and command failures

For a guest-daemon update, see [Upgrading spaced](spaced-upgrades.md). The
supported wake-and-upgrade procedure is disruptive and uses normal lifecycle
and activity accounting. Opportunistic and isolated maintenance upgrades are
planned but not implemented.

Use the Python example's read-only `inspect` action from [Your first sandbox](first-sandbox.md), with `RUN_DIR` set to the exact saved run directory. Keep the same exported endpoint settings and a current `SPACE_API_TOKEN`.

```bash
python3 examples/first_sandbox.py inspect "${RUN_DIR:?Set the saved run directory}"
```

This prints sandbox listings and the saved creation/deletion operations, sandbox, and running-process list. Operations report lifecycle progress or failure; the sandbox reports its current state and conditions. If a request failed before returning IDs, inspect its HTTP error and the saved request in `state.json`, rather than constructing an empty-ID URL.

| Symptom | Check and action |
|---|---|
| Missing management token, `401`, or `403` | Repeat [authentication](installation.md#authenticate) and verify authorization for the resource. Do not use a command attachment token for management requests. |
| Create is accepted but execution is not ready | Poll the operation, confirm `running`, and check the process endpoint. Inspect operation errors and sandbox conditions before creating another sandbox. |
| Sandbox is `paused`, `suspended`, or `stopped` | Waiting alone will not restart it. Inspect its state, request the appropriate lifecycle action, and wait for readiness. See [lifecycle behavior](architecture.md#lifecycle-and-persistence), including the default idle policy. |
| Command exists but has not started | Attach to its data-plane events URL. Creating the command session alone does not start the process. |
| Stream authentication fails | Use `attach.token`, not `SPACE_API_TOKEN`, and the correct session URL. Check expiry and attach promptly after creating the command. |
| Example rejects an endpoint, events URL, or redirect | Use HTTPS except for loopback HTTP. Check the data-plane origin and session ID; correct the configuration rather than removing destination or redirect checks. |
| HTTP stream succeeds but output is incomplete | Check for `process_exited`, its exit result and `output_truncated`, and any `error` or `resync_required` frame. A closed connection is not proof of successful execution. |
| Reconnect reports `resync_required` | The stream could not resume exactly: output from before the reconnect may be missing or repeated. Check that the client sends the last `id` it received exactly as received. A process that has written more than 2^33 - 1 output chunks always resumes this way. Inspect the saved event transcript and query the process only if it is still running. Rerun only if repeating the command's effects is safe. |
| Command exits nonzero | Read stderr and the exit result from the saved event transcript. Check that the executable and working directory exist inside the sandbox, not just on the host. |
| Single-process GET returns `not_found` | This endpoint inspects a running or recently exited process, not historical command results. It returns `not_found` before attachment and once the sandbox no longer keeps the exit: 15 minutes after exit, sooner once many newer processes start, and right after exit on a sandbox from an older template. |

Inspect a running process with `GET /v1/sandboxes/{sandbox_id}/processes/{process_id}`. An empty process list is expected after a command exits; use the saved event transcript to check its result. Keep a failed transcript for diagnosis, but review it for secrets before sharing it.

## A sandbox quarantined after an interrupted suspend

A suspend the node loses before its own capture is saved, to a host restart or
a Firecracker exit, leaves no snapshot of the sandbox's last state. Its
instance filesystem on the node may then be the only copy of that state.
sandbox-manager keeps the filesystem and quarantines the sandbox with one of
two failure reasons, the ones its retention policy keeps the filesystem for,
and nothing recovers or deletes it automatically:

- `suspend_publish_failed_no_artifact_runtime_gone`: the runtime is gone and
  no usable capture of the suspend's admitted attempt is kept. That covers a
  plaintext suspend whose attempt left no complete capture of its own, an
  attempt whose only capture belongs to an earlier suspend, and a running
  suspend that failed before its capture returned or while it was still set
  to discard the capture.
- `suspend_capture_unresolved`: an encrypted capture's evidence doesn't settle
  it. The producer reported an error, refused with the runtime gone, left
  receipts that contradict the attempt, or reached no outcome within the
  attempt's six-hour window. With no receipt yet, the sandbox stays
  `suspending` until that window ends.

Startup and periodic recovery leave a sandbox `suspending` when they can't
read its captures, for a later recovery pass to try again. On Kubernetes, when the
interrupted suspend was the pod's teardown, the pod stays `Terminating`: the
shim releases a task only once sbx-manager answers that the execution is
durable, superseded or deleted, and a quarantined sandbox gets none of those
answers.

**Detect it.**

- On the node, `space-sbxmgr list` shows the sandbox's `STATE` as
  `quarantined`.
- In the control plane, the admin API's
  `GET /admin/api/v1/debug/sandbox/{sandbox_id}` returns the active quarantine
  with `reason_code` `suspend_snapshot_publication_failed` and `next_action`
  `requires_operator_attention`.
- sandbox-manager emits a `sandbox_quarantined` sandbox event and, when its
  recovery of the suspend (at startup, periodically, or from a teardown
  retry) quarantines it, a `suspend_failed` event with the failure reason.
  When a running
  publication fails this way, the control plane receives only that failure
  reason. The cause stays on the node, as the `reason_message` of
  sandbox-manager's own quarantine record and, when the failing step emits a
  `suspend_failed` event, in that event's `error` field.
  internal-api emits another `sandbox_quarantined` event when the node's report
  reaches the control plane.
- The shim's `shim.kill` and `shim.delete` teardown spans keep failing for the
  pod.

**Alert on it.**

- `space.sandbox.worker.quarantine.entered` with
  `reason:suspend_snapshot_publication_failed` counts every sandbox that
  enters this state. It also counts a suspend quarantined for a publication
  failure that leaves the filesystem erasable as usual, which the discard
  below refuses; the debug route's `reason_message` and the discard's own
  check tell them apart.
- `space.snapshot.create.recovery` with `kind:suspend` and
  `outcome:quarantined_no_artifact_runtime_gone` counts the sandboxes that
  recovery of the suspend quarantines for either reason, at startup,
  periodically or from a teardown retry, not the ones a running publication
  quarantines.

`space.shim.operation.error` with `step:prepare_teardown_suspend` also rises,
but it counts every retried teardown release too, so it makes a noisy alert.

**Delete it.** An ordinary delete refuses the sandbox, because it would erase
what may be the only copy, and a control-plane delete answers `409`. Once the
filesystem is known to be expendable, an operator on the node discards it
explicitly. The discard needs a cluster administrator's approval of the
target, the blast radius, the rollback and the verification plan first. Deploy a sandbox-manager that supports the discard first: an older
one ignores the flag and runs an ordinary delete, which refuses this sandbox
but makes none of the checks below and records no reason.

```bash
space-sbxmgr delete <sandbox-uuid> --discard-retained-filesystem \
    --reason "<why the filesystem can be discarded>" --blocking
```

The command takes the sandbox's full UUID and runs as root or as an account
sandbox-manager grants its local operator role; a service account, such as
node-api's, is refused. It's refused for every sandbox that isn't quarantined
(or failed) with one of those two failure reasons, and for a sandbox that
isn't on the node. The command reads the sandbox's state and failure reason
before it sends anything, and sandbox-manager checks them again under the
sandbox's cleanup lock. It also refuses while a capture could still finish:
while the sandbox's suspend attempt is inside its six-hour window, while a
backup of its artifacts is being admitted or its upload is pending, running
or cancelling, and while its runtime is live or its liveness can't be read.
A sandbox quarantined at once can therefore be discarded only once its
attempt's window has passed. sandbox-manager logs the reason in a warning
line before it admits the delete (a restart after admission still finishes
it), then records it in a `delete_requested` event with operation
`discard_retained_filesystem` and deletes the sandbox and its filesystem.

**What releases the pod.** The delete records a receipt covering the
highest execution generation it can read: the generation the pod's teardown
waits on, from the teardown's record, and the row's last observed
generation. The shim's next teardown poll is then answered as deleted and it
releases the task, and Kubernetes finishes removing the pod. If a record
doesn't decode, sandbox-manager logs `teardown suspend operation does not
decode` or `retained route generation does not decode` and the receipt covers
only the generations it could read. When those don't reach the generation the
pod waits on, its poll isn't answered as deleted and the pod stays
`Terminating`.

**If the control plane deleted it first.** A Kubernetes teardown that has
no runtime address for the pod, or no observed execution, deletes the
control-plane sandbox without asking the node. The node then hears that the
sandbox was deleted and node-ctl asks sandbox-manager to evict it, and the
eviction refuses the only copy as a delete does: sandbox-manager logs
`refusing cluster-deleted eviction`, node-ctl counts the eviction as deferred
and retries it every 30 minutes, and the sandbox stays quarantined on the
node. Discard it as above to release it.

**Finish in the control plane.** The control-plane sandbox stays
`quarantined` after the discard. Delete it with `DELETE /v1/sandboxes/{sandbox_id}`, or the admin
API's force-delete, which now completes because the node no longer has the
sandbox. With the orphaned-sandbox reconciler enabled, quarantine recovery may
instead return it to `suspended`, but only when all of these hold: the
reconciler runs live, not in dry run; the API server confirms the sandbox's
pod is gone, before and after the node is asked, so a pod still `Terminating`
holds it; the node reports the sandbox missing; and a ready suspend snapshot
taken before the quarantine covers all of the sandbox's recorded activity. That
snapshot doesn't include the writes the discarded filesystem held.

## A sandbox whose pod is gone while its node still holds it

When a Kubernetes sandbox's pod disappears, the cluster controller's
orphaned-sandbox reconciler would normally return the sandbox to `suspended`
from its last snapshot. The node the sandbox is placed on may still hold a
newer copy, so the reconciler leaves the sandbox to that node in two ways.

- Its candidate listing skips every sandbox whose active node has reported it.
  Such a sandbox stays in its lifecycle without a pod until the node reports
  it settled: a teardown suspend reports it `suspended`, a quarantine reports
  it `quarantined`, or a census leaves it out. A healthy node whose pod
  vanished without its teardown running may never do any of those, and
  nothing bounds that wait.
- A node's report can also land after the listing, for a sandbox the node
  hadn't reported yet, such as one restored onto a node whose publishes there
  failed. For a sandbox a recovery could restore, the plan then defers to the
  node, whatever the age of its census, and the
  recovery's own statement checks the node's report again before it writes.
  While a snapshot of the sandbox is still being created, the plan defers
  (reason `in_flight_snapshot`) before anything else. Otherwise a sandbox
  with no snapshot the recovery accepts doesn't defer: it's quarantined, or
  deleted if it never finished creating, as before. Live classification and
  recovery serialize on a per-sandbox advisory lock with any publish reporting
  the sandbox present, so a publish already holding that lock finishes before
  either reads `sandbox_observed_states` (dry-run planning doesn't take the
  lock). The next listing skips the sandbox while the node keeps reporting it.

**Detect it.** `space.sandbox.api.reconciler.rows_deferred` with
`reason:node_reports_local_copy` counts each plan that deferred to the node.
The listing's skip has no metric of its own: such a sandbox is one the node
reports while the control plane has no pod for it.

## A v2 node drain that waits

A v2 drain (`:drain {engine: v2, hold_seconds}`) shows in the admin node response as `drain_engine: v2` and `drain`: the drain's `generation` and `deadline`, `cancelling` after an undrain whose cancel the cluster controller hasn't finished on the node, `fenced` once the terminate transaction recorded the instance to terminate, `evacuated_total` (every sandbox the controller recorded as evacuated, a released one included), when the controller stored the node's last report (`reported_at`) and when the set of sandboxes it reports evacuated last grew (`evacuated_grew_at`), and the report itself: `evacuated`, `pending` (each sandbox with its `wait`, whether it is `running`, whether it is a warm-pool member, and the node's reason, which is for display) and `blocked`. `drain_last_error` says what the controller is waiting for. The console's node page shows the same as Drain progress. The fence and the dispatch are kept in `drain_status` (`terminate_instance`, `dispatched_at`, `terminated_observed_at`), which the statements below read.

The node seals itself against VM activations and makes every resident durable elsewhere except a running sandbox, which it lists `pending` with the wait `hold` until the deadline, and a stopped one, which it never moves (see below). Its calls work until then. From the deadline on, every call from the controller asks the node to evacuate, and it suspends the running sandboxes and makes them durable too. The deadline is read by the database clock on each tick, so a drain whose controller doesn't call the node (the controller is down, or the node unreachable) holds where it is. A sandbox the drain suspended stays suspended until a call wakes it, which restores it on another node while this node is still draining, drained or terminating; the drain restores nothing. The API refuses calls against the node's sandboxes under two rules, both read from the node's recorded drain. The activation seal holds in every v2 phase, its cancel pending included: every activation of a sandbox of the node that isn't running (a start, wake, restore, resume, implicit wake, or runtime recovery) is refused with a 503 and `Retry-After`, and a running sandbox's calls, and calls that activate no VM, go through. The placement block holds only once the drain is `drained` or `terminating` (a v2 drain `draining`, or with its cancel pending, blocks no placement): every call against the node's sandboxes, a running sandbox's included, is refused, under a v2 drain with a 503 and `Retry-After` (a v1 drain's placement block is a 409). Neither rule refuses a `suspended` or `restoring` sandbox whose current snapshot is durable: the API's drain gate reads no drain for it, in any phase, and its wake restores it on another node, since the drain keeps the node cordoned. The controller writes no sandbox row while the drain drains; the terminate transaction releases the durably suspended ones, and a later wake places them elsewhere.

A v2 drain doesn't move a stopped sandbox, and never terminates a node that holds one. `:drain {engine: v2}` asks the node's preview first, and answers 409 (`code` `drain_v2_stopped_sandboxes`, with the count) when the preview lists a sandbox that is stopped; when the preview can't be read, the drain starts unchecked. While the drain lasts, its cancel pending included, a stop, a kill or a rollback without restart of a sandbox on the node is refused with a 503 and `Retry-After`, following the node's recorded drain, whatever `SPACE_DRAIN_V2_ENABLED` says, and the node holds an idle auto-stop of a suspended, started or paused sandbox until it is unsealed; an idle auto-stop already running when the node seals is listed `pending` until it ends. A stopped sandbox that appears anyway, on Kubernetes a stop that raced the API's check, is listed `pending` with the wait `external` and the reason `stopped sandbox: drain v2 doesn't move stopped sandboxes`: the drain never reaches `drained`, and `:terminate`'s fresh check refuses it. Delete the sandbox, or undrain the node, start the sandbox and drain again.

A drain suspends a running sandbox the way an idle auto-pause does, and restores nothing. A client brings it back as it would after an idle suspend. A native client whose sandbox's policy doesn't wake on access, which is the native create's default, gets `409` `sandbox is suspended, not running` with a `wake` detail naming `POST /v1/sandboxes/{id}:wake`, and calls `:wake`. An ECW (E2B) client reconnects with `connect`. Either restores the sandbox on another node while the node is still draining, drained or terminating; after an undrain the node is schedulable again, and the restore may land on it. The first wake is placed without the drain's evacuation tolerations.

The first release drains clean nodes only. A node whose preview lists a quarantined sandbox, a sandbox whose cleanup failed, or a phantom holder (a sandbox the control plane places on the node though the node doesn't host it) won't reach `:terminate`: resolve those first. A preview skips node-manager's orphan inventory, which a drain call reads, so a drain can find more node-wide blockers than its preview listed.

Past its deadline, a drain whose node has reported no further sandbox evacuated for 15 minutes is stalled: `drain_last_error` starts `the drain is stalled`, and `node_drain.v2.stalled` reads 1 for the node. The console shows it stalled from `evacuated_grew_at` plus 15 minutes. The mark changes nothing else, and it clears once the node reports another sandbox evacuated or the drain finishes. Before its deadline a drain is never stalled, however long it holds. A sandbox that never becomes durable keeps its wakes getting the 503.

| What the listing shows | What it means and what to do |
|---|---|
| `:drain` answers 409 with `code` `drain_v1_refused_while_v2_enabled` | Drain v2 is on (`SPACE_DRAIN_V2_ENABLED`), and v1 and v2 drains never run together, so every v1 `:drain` is refused. Drain with `engine: v2` and `hold_seconds`. `:undrain` and `:terminate` still run. |
| `:drain` answers 409 with `code` `drain_v2_evacuation_cap` | Every evacuation slot is held (`SPACE_DRAIN_V2_MAX_EVACUATING_NODES`). Retry after `Retry-After`, or free a slot: shorten a drain in flight with `:drain {engine: v2, hold_seconds: 0}`, or undrain one. |
| `:drain` answers 409 with `code` `drain_v2_stopped_sandboxes` | The node's preview lists stopped sandboxes (`stopped`, `stopped_sandbox_ids`), which a v2 drain doesn't move. Delete them, or start them, then drain again. |
| `:drain` answers 409 with `code` `drain_v2_node_heartbeat_stale` and `Retry-After: 60` | The node isn't answering, and a v2 drain is the node's own work. Find out why the node is down, and restore it. If it won't come back: terminate the instance and let crash handling clean up, or undrain every v2 drain, let each cancel finish, then turn `SPACE_DRAIN_V2_ENABLED` off, after which a v1 drain is accepted again. |
| `:drain` answers 409, `undrain still finishing` | An earlier v2 drain's undrain hasn't reached the node yet (no phase, `drain_engine` still `v2`): the node may still be sealed under that drain. Retry once the engine is gone. See a stuck cancel below. |
| `drain_last_error` starts `the drain is stalled` | Past the deadline, nothing more has been evacuated for 15 minutes. `pending` says what the node still does or waits for, each with `since`, when the row entered its state: a sandbox whose wait is `external` waits on work the drain doesn't drive, such as its own lifecycle. `blocked` lists node-wide problems, such as a scan that failed or an untracked runtime process, which keep the drain from finishing. |
| `the node answered DrainNode under generation N sealed under generation M` (or `with a cancel's result`, `with no result`) `; it records nothing from the answer` | The answer isn't this drain's, so the controller records nothing from it and asks again every tick; `node_drain.v2.unanswered` reads 1. Check the node's sandbox-manager, or undrain. |
| `DrainNode failed: ...` | The node is down or unreachable, or refused the call, which the error names; a node whose sandbox-manager predates drain v2 answers `Unimplemented`. `node_drain.v2.unanswered` reads 1, and the drain keeps its slot and asks again every tick. A node whose machine is gone is let go once its heartbeat is stale and EC2 lists its instance `terminated`. An `Aborted` call, another drain call in progress on the node, is skipped for the tick and recorded nowhere. |
| `cordon convergence failed: ...; not calling DrainNode` | The Kubernetes Node under the drain's node name has another UID than the one registration recorded, so the controller doesn't call it. Registration settles which machine it is. |
| `drained`, `drain_last_error` starts `terminate refused:` | `:terminate` moved the node to `terminating`, and the terminating tick's fresh `DrainNode` call didn't let it fence: the call failed (`terminate refused: DrainNode failed: ...`, an unreadable seal after a sandbox-manager restart among them) or the node reported something left (`terminate refused: the node reports N pending, M blocked: ...`). The node is back in `drained`. Resolve what it names and `:terminate` again. |
| `terminating`, `terminate held: ...` | The fresh check passed, but rows or executions still name the node. The terminate transaction releases the durably suspended rows, and a row whose restore is in flight settles once the restore lands elsewhere. A stopped row that still names the node, one the node no longer holds among them, holds it: delete it. A phantom row, a `quarantined` row naming the node, a row the drain can't make durable, and a `failed` or `deleted` row whose execution on the node is still active don't settle on their own and hold the node until someone resolves them. The controller retries every tick; the node stays `terminating` until the reference clears or the node is undrained. A failed or non-empty fresh report returns it to `drained` instead (the row above). |
| `terminating`, `drain_last_error` names the `aws` terminator | The cluster runs another terminator. Under it the v2 loop never calls the node for the fresh check, never asks EC2 about a node's instance and writes no fence, so it never lets a dead v2 node go itself. `:undrain` hands back a node it hasn't fenced; terminate that one another way. A fence written while the cluster ran `aws` stays when the terminator changes, and `:undrain` is refused once a fence exists. |
| `terminating`, `terminating fenced instance ... failed: ...` | Auto Scaling refuses the termination; the controller retries with backoff. There is no undrain after the fence, since a termination may already be accepted. Terminate the fenced instance in AWS: once EC2 says it is `terminated`, the drain lets the node go. |
| `EC2 does not list fenced instance ..., and was never seen to list it terminated` | EC2 doesn't list the fenced instance, and the drain never saw it `terminated`, so it neither dispatches the termination nor deletes the row. A missing instance alone isn't death. Check the instance in AWS. If the machine is gone, use the guarded let-go below. |
| `the termination of fenced instance ... was dispatched N minutes ago; EC2 answers unknown (...)` | The termination was dispatched, and EC2 gave no answer. The drain can't tell whether the instance is gone, so it keeps the row and says so on every check. Restore EC2 access; if you have confirmed the machine gone in AWS, use the guarded let-go below. |
| `Auto Scaling accepted the termination of ...; EC2 still answers ... after N minutes` | Auto Scaling accepted the termination and EC2 still lists the instance. Check the instance in AWS. |
| `fenced instance ... is terminated; waiting for its Kubernetes Node ... to go` | EC2 says the fenced instance is `terminated` but its Node object stays. Confirm EC2 reports that instance `terminated` and that the Node's `providerID` names it, then delete the Node with a uid precondition. The delete needs a cluster administrator's approval of the target, the blast radius, the rollback and the verification plan first. The drain's next check deletes the row if its heartbeat is stale and nothing names the node; a row or execution that still names it has the drain let the node go instead. |
| `... is terminated, but ...`, ending `not letting it go` | EC2 says the recorded (or fenced) instance is terminated, but the node's heartbeat is fresh, or its Kubernetes Node is live on another instance or names none: EC2's answer may be about another machine. The drain waits. Check the Node and its machine. |
| `drain_last_error` starts `v2 drain let go` | The drain released the node: its heartbeat is stale and its instance terminated outside the drain, a fenced instance terminated with no dispatch confirmed, or the delete found a row or execution still naming the node. The phase and drain columns are cleared, and the gone-node loops handle the node as any crashed node. |
| No phase, `drain.cancelling`, `DrainNode cancel failed: ...` | The undrain's cancel hasn't reached the node. The controller sends it every tick, forever: giving up would uncordon a sealed node or orphan its seal. `node_drain.v2.unanswered` reads 1. See a stuck cancel below. |
| A registration warning that a Node under a new uid reports no EC2 instance yet, and `node_drain.v2.registration_deferred` | A node with a v2 drain re-registered under a new Kubernetes Node UID before the Node reported its `providerID`. Registration defers it; the next sync keeps the drain if the Node reports the recorded instance, and clears it if it reports another. |
| A wake or start refused with a 503 on a node no drain lists, and `node_drain.v2.orphaned_seal_refusals` | An orphaned seal: the node's sandbox-manager is sealed, but Postgres records no v2 drain of it. See below. |
| A sandbox the drain suspended is still `suspended` after the drain, or after an undrain | Expected: the drain restores nothing, and a `:wake`, or a call its `wake.on` policy wakes on, restores it: on another node while the node is still draining, drained or terminating, and possibly on the same node after an undrain, which makes the node schedulable again. `node_drain.v2.evacuated_still_suspended` counts, for each drain, the sandboxes it saw running that have stayed suspended for over 15 minutes. |

**An orphaned seal.** A node can stay sealed with no v2 drain recorded, for instance after its row lost the drain while the node was unreachable. Nothing heals it on its own, and every activation on the node keeps being refused. With `SPACE_DRAIN_V2_ENABLED` on, `:drain {engine: v2, hold_seconds: ...}` then `:undrain` heals it: the drain takes a new, later generation and the undrain's cancel unseals the node. With it off, or instead, send the node `DrainNode{generation, Cancel}` through its node-api with the generation it is sealed under, which its refusals name.

**A stuck cancel.** An undrain whose cancel never reaches the node keeps the node cordoned and its evacuation slot held, and `:uncordon` is refused meanwhile (`code` `drain_v2_cancel_pending`): the node may still be sealed. Restore the node's connectivity so the cancel lands, upgrade the node if its sandbox-manager predates drain v2, or replace the machine: once its heartbeat is stale and EC2 lists its instance `terminated`, the drain lets it go and the row is cleared.

**A backward step of the database clock.** A drain's generation is its start in microseconds by the database clock, and the node takes generations only forward, refusing one at or below a generation it was cancelled under. After the clock stepped backward, a new drain can be refused by the node for that reason (`FailedPrecondition` naming both generations). Undrain it, and drain again once the clock has passed the cancelled generation.

**An unknown engine with a NULL phase.** A `drain_engine` this build doesn't know, with no phase, refuses every activation on the node, fails closed, and no loop clears it. Clear it by hand once you have confirmed no drain of that engine runs. The update needs a cluster administrator's approval of the target, the blast radius, the rollback and the verification plan first: `UPDATE nodes SET drain_engine = NULL, drain_status = NULL WHERE id = '<node id>' AND drain_phase IS NULL AND drain_engine = '<engine>';`.

A v2 node whose machine you have confirmed gone in AWS, but which the drain doesn't let go, keeps its drain, and a draining or undrained one keeps its evacuation slot. That happens when EC2 never reports its instance `terminated`, doesn't list it, or gives no answer, or when the cluster runs another terminator, under which the v2 loop never asks EC2. Let it go by hand. The let-go's statement needs a cluster administrator's approval of the target, the blast radius, the rollback and the verification plan first. First read what the row records:

```sql
SELECT n.drain_phase,
       n.drain_status->>'generation' AS generation,
       n.drain_status->>'terminate_instance' AS fence,
       k.instance_id, k.k8s_node_uid, k.k8s_node_name,
       s.reachable, s.stale_after,
       COALESCE(s.reachable AND s.stale_after > NOW(), FALSE) AS heartbeat_fresh
  FROM nodes n JOIN node_k8s k ON k.node_id = n.id
  LEFT JOIN node_status_snapshots s ON s.node_id = n.id
 WHERE n.id = '<node id>';
```

Stop unless `heartbeat_fresh` is `false`: the drain lets a node go only once its heartbeat is stale, by the API's node-status rule (`node_heartbeat_fresh`). A heartbeat is fresh while the node's `node_status_snapshots` row is `reachable` and its `stale_after` is still ahead of the database clock; each status publication sets `stale_after` to its time plus two of the node's status poll intervals plus 30 seconds, at most 10 minutes (2 minutes 30 seconds at node-ctl's default 60-second interval). A fresh heartbeat means the node still publishes, whatever EC2 says of its instance: it may be running and sealed, and clearing its drain would leave it sealed with no drain recorded. Don't let it go; find out which machine publishes. Confirm in AWS that this instance is the machine that is gone. Then confirm it is the machine the node registered, as the drain itself requires before it lets a node go: `kubectl get node <k8s node name> -o jsonpath='{.spec.providerID}'` answers `NotFound`, or names that instance (`aws:///<zone>/<instance id>`). A live Node that names another instance, or none, means the recorded instance is stale and the machine may still be running, sealed under the drain: don't let it go. Then fill in the values you verified. Write `NULL`, unquoted, for an undrained node's phase or an unfenced node's fence. The statement locks the row, and clears the drain only if every value still matches and the heartbeat is still stale, so a same-name replacement that registered in the meantime, a drain that moved on, or a node whose heartbeat came back is left as it is. It touches no sandbox row.

```sql
WITH verified AS (
  SELECT n.id
    FROM nodes n JOIN node_k8s k ON k.node_id = n.id
   WHERE n.id = '<node id>'
     AND n.drain_engine = 'v2'
     AND k.instance_id = '<instance id>'
     AND k.k8s_node_uid = '<k8s node uid>'
     AND (n.drain_status->>'generation')::BIGINT = <generation>
     AND n.drain_phase IS NOT DISTINCT FROM '<phase>'
     AND n.drain_status->>'terminate_instance' IS NOT DISTINCT FROM '<fence>'
     AND NOT EXISTS (
           SELECT 1 FROM node_status_snapshots s
            WHERE s.node_id = n.id AND s.reachable AND s.stale_after > NOW()
         )
     FOR UPDATE OF n, k
)
UPDATE nodes n
   SET drain_phase = NULL, drain_mode = NULL, drain_started_at = NULL,
       drain_engine = NULL, drain_status = NULL,
       drain_last_error = 'v2 drain let go by an operator: machine gone'
  FROM verified
 WHERE n.id = verified.id;
```

`UPDATE 1` let the node go. `UPDATE 0` means the row no longer matches what you verified, or its heartbeat is fresh again: read it again, verify it again in AWS, and only then retry with the new values.

**Monitors before turning drain v2 on.** Alert on `node_drain.v2.orphaned_seal_refusals` (any orphaned seal), and on `node_drain.v2.unanswered` reading 1 for a node for more than 15 minutes (a drain or cancel the node doesn't answer).

**Rolling back the node half.** Roll sandbox-manager back only with no v2 drain in flight. It is image-only once the release deployed everywhere before it already contains the activation seal. Otherwise, run the seal migration's down on every node first, so the older build doesn't meet a seal row it doesn't know.

**Known gaps.** A sandbox whose policy has no implicit wake answers its first call after a drain with the native `409` and a `:wake` hint. The first wake after a drain is placed without the drain's evacuation tolerations. After an undrain, the sandboxes the drain suspended stay suspended until a call wakes them. A suspend the node started before a cancel may finish after it. A drain waits while the controller is down. A sandbox that never becomes durable keeps its wakes getting the 503 for as long as the drain runs. A sandbox the drain suspended whose snapshot isn't durable yet still reads `started`: a wake answers that it already completed and a command stays pending, rather than a 503, until the row catches up.

## Timeouts mean different things

A client deadline limits how long the client waits; it does not cancel a lifecycle operation or establish that a process stopped. Inspect the resource before retrying or issuing another command.

| Limit | Value | Meaning |
|---|---|---|
| Walkthrough API request | 30 seconds normally; 180 seconds for process readiness | A whole-exchange Python deadline, not a server-side cancellation. |
| Walkthrough state polling | 5 minutes per target state | Stops waiting, not the server-side operation. |
| Walkthrough event stream | 45 seconds | Stops reading the stream, not a reliable process-cancellation mechanism. |
| Command `timeout` | `30s` in the example; none when omitted | Server-side execution deadline; expiry terminates the process group. |
| Attachment token lifetime | 5 minutes | Separate from the command's execution timeout; observe the returned attachment expiry. |
| Default sandbox idle policy | 6 hours without access or command activity | Suspends an idle sandbox rather than imposing a fixed lifetime. |

The Python example uses Linux signals to bound whole requests and stream reads. Run it as an executable on the documented Linux host, not in a background Python thread.

## Retry without duplicating work

For endpoints that declare a required `Idempotency-Key`, keep the original request body and key until the mutation's outcome is known. The Python example saves them in `state.json`. For a retry of the same action, resend the same body with the same key; changing the body under the same key causes an `idempotency_conflict`.

Process kill and signal do not deduplicate requests with `Idempotency-Key`. Do not blindly retry an ambiguous outcome; inspect the process and its event stream first, because a repeat can apply the action again.

Check a returned operation ID before resending a lifecycle mutation. A new key represents a new request and can create another sandbox or command; do not use a new key merely because a connection timed out.

Read the API error's `code`, `message`, `details`, `request_id`, and `retryable` fields. Correct non-retryable errors first, and use bounded backoff for retryable errors without discarding the original key.

A file route's `404` (`not_found`) whose `details` include `{"key": "resource", "value": "path"}` means the sandbox is yours and the path inside it does not exist, so there is no need to look the sandbox up again. Without that detail, the sandbox itself was not found or is not yours; the two answer alike.

`rate_limited` is not the only `429`. A `POST /v1/sandboxes` that would exceed the organization's or the user's sandbox limit gets `429` `quota_exceeded` with `retryable: false` and no `Retry-After`: resending it unchanged fails the same way until a sandbox is deleted or an in-flight create ends. See `SPACE_NATIVE_ORG_SANDBOX_LIMIT` in `docs/configuration.md`.

A command create's `400` (`invalid_request`) whose `details` include `{"key": "field", "value": "pty"}` means it asked for a `pty` with `Accept: text/event-stream`. Server-sent events cannot drive a terminal, so drop the `pty`, or create the command without that header and attach over WebSocket.

A sandbox that is at capacity, or shutting down, can refuse a request before doing any of its work: a retryable `429` (`rate_limited`) or `503` with `Retry-After`. That request did nothing, though an earlier attempt of the same action may have; wait at least that long and resend it with the same body and key, or reattach to the same session. SPACE does not retry these refusals for you. A timeout leaves the outcome unknown, and it is retryable only where the endpoint deduplicates by its key. Lifecycle operations and command creation deduplicate for 24 hours; file writes, batch writes, and renames only for 60 seconds after they complete, and not across a restart of the sandbox's agent, so retry them promptly or check the file first. File `mkdir`, `remove`, and change tracking do not deduplicate: their timeouts are not retryable, so check the sandbox's state before repeating them.

When reporting a problem, include the source revision, failing step, request ID, relevant sandbox/operation/process/session IDs, and sanitized error or event output. Exclude bearer tokens, passwords, private keys, and sensitive command environment values.
