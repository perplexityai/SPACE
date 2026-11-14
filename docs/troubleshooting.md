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
  deleted if it never finished creating, as before. A report whose publish hasn't committed by
  then still isn't seen. The next listing skips the sandbox while the node
  keeps reporting it.

**Detect it.** `space.sandbox.api.reconciler.rows_deferred` with
`reason:node_reports_local_copy` counts each plan that deferred to the node.
The listing's skip has no metric of its own: such a sandbox is one the node
reports while the control plane has no pod for it.

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
