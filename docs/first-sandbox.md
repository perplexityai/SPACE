# SPACE: Your first sandbox

[Documentation](../README.md)

Create a sandbox, run a command, collect its output, and delete it through the SPACE API. The [Python example](../examples/first_sandbox.py) uses only the standard library: no package installation, virtual environment, or SDK is needed.

<a id="before-you-start"></a>

## Prepare your environment

Complete [Install and configure locally](installation.md), including authentication. Run the example from the repository root on the same Linux host, with Python 3.8 or newer. It uses the guide's Kind deployment, its API and data-plane ports (8080 and 8090, which the cluster maps to loopback), and the published `seed:latest` template.

The installation exports `SPACE_API_URL`, `SPACE_DATA_PLANE_URL`, and `SPACE_API_TOKEN`. Python inherits them from the shell; if you opened a new terminal, repeat the configuration and authentication steps first. `SPACE_TEMPLATE` defaults to `seed:latest`.

Management requests use the API bearer token. Command streams use the returned command attachment token; the two tokens are not interchangeable. The example requires HTTPS except for loopback HTTP, disables environment proxies and HTTP redirects, and accepts an attachment URL only at the configured data-plane origin and exact session path.

## Run the example

```bash
python3 examples/first_sandbox.py
```

The example prints a private run directory before making any API calls. Keep that path: it contains `state.json` with returned IDs and original mutation request bodies and idempotency keys, plus event transcripts. Tokens are not written to state.

The example follows this sequence:

1. **Create:** Request a Firecracker sandbox from `seed:latest`. Save its sandbox ID and operation ID.
2. **Wait:** Poll for operation success and a `running` sandbox, then check the guest process endpoint.
3. **Execute:** Create a command session, then attach to its event stream to start the process.
4. **Check:** Decode stdout and stderr from base64 and display them as they arrive. Require `process_exited` with exit code `0`, with no `error`, `resync_required`, or incomplete frame.
5. **Inspect:** List sandboxes, inspect this sandbox and its operation, and list its currently running processes.
6. **Choose cleanup:** After displaying the result, ask whether to delete the sandbox and remove its transcripts. Enter `y` to delete; Enter or any other answer keeps them.

You should see `Hello from SPACE` and `Command completed successfully`, followed by inspection results and the deletion prompt. A successful HTTP response or a closed stream alone is not a successful command.

If creation, readiness, or command execution fails (or you interrupt the example), it stops without further deletion. Inspection errors are printed separately and do not skip the cleanup prompt. Keep the run directory: **starting the example again creates a new request, not a retry of the previous run.**

## Adapt the request

The example's `main` function ties the API steps together. This is an excerpt for reading, not a separate runnable script:

```python
create_sandbox(config, directory, state)
run_command(config, directory, state)
inspect_run(config, state)
confirm_delete(config, directory, state)
```

`config` holds the endpoints and token; `directory` and `state` keep recovery records. `create_sandbox` sends a JSON request through `api` and polls the returned operation. `run_command` creates a command session, then reads its scoped event stream. Follow those functions in the example to see the HTTP paths, request bodies, and response handling.

To change the command, edit its `COMMAND` constant. For example:

```python
COMMAND = {
    "command": ["sh", "-c", "printf '%s\\n' \"$GREETING\"; pwd"],
    "cwd": "/tmp",
    "env": {"GREETING": "Hello from Python"},
    "timeout": "30s",
}
```

Commands use an argument array, not an implicitly interpreted shell string. Use `sh -c` explicitly for shell expansion, pipes, or redirection. Executables and working directories must exist inside the sandbox, not just on the host.

To customize the sandbox, edit the request body in `create_sandbox`. The default runtime is `fc`; the example selects it explicitly.

| Field | Request | Meaning |
|---|---|---|
| `template` | Sandbox | A published reference such as `seed:latest` or a digest reference. |
| `runtime` | Sandbox | Runtime selection; `fc` means Firecracker. |
| `name`, `metadata` | Sandbox | A readable name and application labels. Do not put credentials here. |
| `resources.vcpu`, `resources.memory_mib`, `resources.disk_mib` | Sandbox | A requested shape. Omitted fields remain `null` in the response while the deployment applies its defaults; the response does not reveal the resolved shape. |
| `env` | Sandbox | Environment values inherited by commands. |
| `command` | Command | Executable and argument array. |
| `cwd` | Command | Existing guest directory; defaults to the guest user's home directory. |
| `env` | Command | Add or override environment values for this command. |
| `timeout` | Command | Server-side execution deadline, such as `30s`; omitting it sets no command deadline. |

The example reads the stream for at most 45 seconds. Keep the command deadline below that limit, or increase `STREAM_TIMEOUT` as well.

Read `/openapi-m1.json` for exact request constraints and supported fields. This is an HTTP example, not a Python SDK.

<a id="command-output"></a>

## Read command output

The example displays decoded stdout and stderr and saves the raw event stream in its run directory. Handle binary output separately rather than displaying it in a terminal.

Command output is a live stream, not durable log storage. The sandbox retains a bounded amount of each process's recent output: reconnecting with `Last-Event-ID`, set to the last `id` received, exactly as received (only output and `resync_required` events carry one), resumes with the output after that event while it is retained, and otherwise produces `resync_required` first. After a process's first 2^33 - 1 output chunks, ids no longer carry its output position, and each reconnect replays the retained output behind `resync_required`. Keep the transcript when a stream fails; repeat a command only if repeating its effects is safe.

## Inspect and manage the sandbox

Set `RUN_DIR` to the exact directory printed by your run, replacing the placeholder below. Use the same exported endpoint settings and a current management token:

```bash
export RUN_DIR=/tmp/space-first-sandbox-REPLACE_ME
python3 examples/first_sandbox.py inspect "$RUN_DIR"
```

This performs read-only Python API requests. It lists sandboxes before inspecting the saved operation, sandbox, and process list; an individual request failure does not hide the other results. Missing IDs are never used to construct an empty-ID URL. The example rejects a saved run whose origins do not match the current configuration.

An empty process list is expected after hello-world exits. The single-process endpoint is for **running processes**, not history: it returns `not_found` before attachment and after exit. Use the saved event transcript for completed command results.

Sandbox and operation IDs can look similar. Use the complete `sbx_…` ID for sandbox paths and `op_…` for operation paths. The returned `session_id` and `process_id` currently contain the same `proc_…` value, but use their respective fields for their URLs.

The inspection prints one page from each list. Applications should follow `next_cursor` until it is null when they need all results.

| Lifecycle request | Effect |
|---|---|
| `POST /v1/sandboxes/{id}:pause` | Pause execution while keeping the VM resident. |
| `POST /v1/sandboxes/{id}:suspend` | Save VM state and filesystem and release active VM resources. |
| `POST /v1/sandboxes/{id}:wake` | Resume or restore the sandbox. |

Use a valid sandbox ID and an `Idempotency-Key` for each lifecycle mutation, then poll its operation. After wake, repeat the running-state and process-readiness checks before executing another command.

## Delete the sandbox and clean up

If you kept the sandbox or a step failed, inspect the run first. Then request deletion explicitly:

```bash
python3 examples/first_sandbox.py delete "$RUN_DIR"
```

This asks for confirmation, sends `DELETE`, and waits for its operation to succeed before removing **all tracked event transcripts**. HTTP errors, including `404`, are failures, not proof of successful deletion.

If deletion previously returned an operation ID, the example resumes polling that operation. If the request's outcome is unknown, it warns before confirmation and reuses the original delete key and body rather than generating another request. A failed deletion or interrupted wait leaves the IDs, request record, and transcripts available for diagnosis.

The private run directory and `state.json` remain after cleanup. They contain no saved bearer tokens, but customized request bodies and command output can contain secrets. Remove the directory when you no longer need its records; do not publish it or commit it.

<a id="if-a-step-fails"></a>

## Troubleshoot failures

Run `inspect` with the saved directory before retrying. See [sandbox and command failures](troubleshooting.md#sandbox-and-command-failures) for specific errors and [timeouts](troubleshooting.md#timeouts-mean-different-things) for wait limits.

`state.json` retains each mutation's method, path, body, and idempotency key before sending it. Valid returned IDs are saved even if another required field is missing. No mutation is retried automatically, and a timeout does not prove it failed. Follow [safe retry guidance](troubleshooting.md#retry-without-duplicating-work) rather than starting another run.

When reporting a problem, include the source revision, failed step, complete relevant IDs, and sanitized errors or transcript output. Exclude tokens, passwords, private keys, and sensitive environment values.
