# SPACE: Troubleshooting

[Documentation](../README.md)

First identify which stage failed: local setup, API authentication, a sandbox operation, or a command stream. An accepted request, a running sandbox, and a successful command are three different outcomes.

## Local installation

This section covers the [Kind installation](installation.md). Use the explicit `kind-space-local` context so diagnostics target that local deployment, not another cluster. If you selected another cluster name, replace the context accordingly.

| Symptom | Check and action |
|---|---|
| `required tool not on PATH: kind` or `command not found: tilt` | Complete [Install and verify tools](installation.md#install-and-verify-tools), including the Bash startup configuration and fresh-login check. Bootstrap does not install tools. |
| Tools work in one terminal but not a new one | Confirm that `$HOME/.local/bin` is on PATH in the new Bash shell. Repeat the startup configuration and fresh-login check in [Install and verify tools](installation.md#install-and-verify-tools); another inline export alone will not fix future logins. |
| SSH disconnects during a build | Reconnect to the same host and run `tmux attach-session -t space-setup`. Inspect the running command before starting anything else; if you did not use tmux, check whether the original build is still running before retrying. |
| Running setup from a Mac | Use a Linux x86_64 host with KVM and run the setup there over SSH. Installing the command-line tools on macOS does not satisfy the host requirements. |
| `/dev/kvm` or `/dev/net/tun` is missing, or storage setup fails | Check device access, nested virtualization, privileged Docker operations, loop devices, and btrfs support on the host. |
| Package installation fails | Install the packages named in the error, or provide root/non-interactive sudo access. Rerun the failed step rather than skipping it. |
| Pods remain pending or restart | Run the commands below and inspect the first failing pod's events and container logs. Confirm the host supports the required privileged containers and storage operations. |
| `seed:latest` cannot be resolved | Check `space-seed-publish` logs and completion. The seed must be built and published before sandbox creation. |
| Login rejects `root@local` | Use the password selected during bootstrap (`password` by default). Confirm bootstrap completed or Tilt's `provision-local-console` resource succeeded; rerunning does not reset an existing password. |
| API or data plane refuses connections | Check service readiness: `kubectl --context kind-space-local -n space get pods,endpoints,svc` should show ready pods behind `space-api-entry` and `space-data-plane-entry`. localhost reaches them through the cluster's port mappings (`docker port space-local-worker3`); a cluster created before those mappings is refused by bootstrap with the command to recreate it. API reachability does not prove data-plane reachability. |
| `curl: (52) Empty reply from server` | A replica may have been replaced mid-request, or, for the UI and object storage, a port forward may be reconnecting after its target pod was replaced, including after a smoke test starts Tilt. Check the forwarding logs before changing credentials. |

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

For an empty reply, inspect `/tmp/space-local-pf-<service>.log` for `lost connection to pod`, then wait for a new `Forwarding from 127.0.0.1:<port>` message before retrying:

```bash
for log in /tmp/space-local-pf-*.log; do
  [[ -f "$log" ]] || continue
  printf '\n== %s ==\n' "$log"
  tail -n 60 "$log"
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
| HTTP stream succeeds but output is incomplete | Check for `process_exited`, its exit result, and any `error` or `resync_required` frame. A closed connection is not proof of successful execution. |
| Reconnect reports `resync_required` | Missed output is not replayed. Inspect the saved event transcript and query the process only if it is still running. Rerun only if repeating the command's effects is safe. |
| Command exits nonzero | Read stderr and the exit result from the saved event transcript. Check that the executable and working directory exist inside the sandbox, not just on the host. |
| Single-process GET returns `not_found` | This endpoint inspects a running process, not historical command results. It returns `not_found` before attachment and after exit. |

Inspect a running process with `GET /v1/sandboxes/{sandbox_id}/processes/{process_id}`. An empty process list is expected after a command exits; use the saved event transcript to check its result. Keep a failed transcript for diagnosis, but review it for secrets before sharing it.

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

A sandbox that is at capacity, or shutting down, can refuse a request before doing any of its work: a retryable `429` (`rate_limited`) or `503` with `Retry-After`. That request did nothing, though an earlier attempt of the same action may have; wait at least that long and resend it with the same body and key, or reattach to the same session. SPACE does not retry these refusals for you. A timeout leaves the outcome unknown, and it is retryable only where the endpoint deduplicates by its key. Lifecycle operations and command creation deduplicate for 24 hours; file writes, batch writes, and renames only for 60 seconds after they complete, and not across a restart of the sandbox's agent, so retry them promptly or check the file first. File `mkdir`, `remove`, and change tracking do not deduplicate: their timeouts are not retryable, so check the sandbox's state before repeating them.

When reporting a problem, include the source revision, failing step, request ID, relevant sandbox/operation/process/session IDs, and sanitized error or event output. Exclude bearer tokens, passwords, private keys, and sensitive command environment values.
