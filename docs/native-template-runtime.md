# Native template startup

[Documentation](../README.md)

A template can declare up to 16 named foreground applications to start concurrently
at boot. The builder compiles validated app declarations into one systemd unit per app,
stored with a TOML inspection copy in `/etc/space/apps/`. These files are ordinary
rootfs content covered by its content hash. Recipes supply data, not unit text;
the guest needs no SPACE runtime interpreter or launcher.

Append one `set_runtime` step to a native template-build request:

```json
{
  "set_runtime": {
    "schema_version": 1,
    "apps": [{
      "name": "web",
      "command": ["/usr/local/bin/app", "--port", "8080"],
      "user": "user",
      "working_directory": "/home/user",
      "env": {"APP_MODE": "template"},
      "readiness": {"type": "exec", "command": ["/usr/bin/curl", "-fsS", "http://127.0.0.1:8080/ready"]},
      "startup_timeout_secs": 60
    }]
  }
}
```

For TOML recipes, the equivalent final step is:

```toml
[[steps]]
type = "set-runtime"
[steps.declaration]
schema_version = 1
[[steps.declaration.apps]]
name = "web"
command = ["/usr/local/bin/app", "--port", "8080"]
user = "user"
working_directory = "/home/user"
startup_timeout_secs = 60
[steps.declaration.apps.env]
APP_MODE = "template"
[steps.declaration.apps.readiness]
type = "exec"
command = ["/usr/bin/curl", "-fsS", "http://127.0.0.1:8080/ready"]
```

Add further `[[steps.declaration.apps]]` tables for additional apps. `SetRuntime`
replaces the whole app set; it does not merge names with the parent. Names are unique,
1–64 lowercase ASCII letters, digits, hyphens or underscores, starting with a letter.
There are no app dependencies or ordering rules; apps must tolerate peers starting
concurrently. Every app has its own user, environment, probe, timeout, startup policy
and restart budget.

Inside the guest, the builder writes the same declaration as TOML:

```toml
schema_version = 1

[[apps]]
name = "web"
command = ["/usr/local/bin/app", "--port", "8080"]
user = "user"
working_directory = "/home/user"
startup_timeout_secs = 60
startup_policy = "best-effort" # or "required"; currently checked before base snapshots
[apps.readiness]
type = "exec"
command = ["/usr/bin/curl", "-fsS", "http://127.0.0.1:8080/ready"]

[[apps]]
name = "worker"
command = ["/usr/local/bin/worker"]
user = "user"
working_directory = "/home/user"
startup_timeout_secs = 60
[apps.readiness]
type = "launch"
```

Use `{"clear_runtime": {}}`, or `type = "clear-runtime"` in TOML, to disable
inherited startup. Omit both to inherit a FromTemplate parent's exact runtime files.
OCI origins start without a declaration; OCI CMD and ENTRYPOINT remain ignored.
Scratch seed builds do not accept recipe steps: build a child of the seed to add
an application. A runtime step must be last, appear only once, and have no
`env_refs`. Template settings must be literal and reproducible.

The declaration is limited to 256 KiB. Commands are argv arrays, the working
directory must be absolute, and startup timeout is 1–60 seconds. `{"type":"launch"}`
readiness uses systemd `Type=exec`. Absolute executable paths run directly; PATH
and relative commands use `/usr/bin/env`, which execs in place. Paths ending in
whitespace or backslash also use `env --chdir` to avoid older systemd executor
serialization changing those bytes. For those commands
the unit initially acknowledges env execution; command lookup failures then fail
the unit. Use an exec probe when application-level readiness matters. An exec probe runs repeatedly until it
succeeds or the startup deadline expires. Application and probe use the same user,
working directory and template environment. Systemd supplies account environment defaults; the unit sets a standard PATH,
and declaration values override those defaults. User names must contain only
ASCII letters, digits, underscore, hyphen or dot (numeric UIDs are accepted).
Use absolute executable paths for names containing `=`. Working directories are
control-free absolute paths. Arguments and environment values preserve literal
quotes, newlines, `%` and `$`; no tenant value becomes shell source.

## Boot and snapshot behavior

The application uses template settings on every cold boot. Per-sandbox environment
variables, secrets and identity are **not** supplied to the application; they still
apply to commands launched later through process APIs. The app starts after the
agent, in `space-sessions.slice`, and survives agent restarts.

`space.target` wants `space-apps.target`, which wants the app units. Apps are
ordered after `space.target`; neither target requires app success or waits for
app readiness. Each app uses `space-sessions.slice` and has its own cgroup.
Systemd supplies identity, working directory, environment, exec acknowledgement,
restart policy and logging. An `ExecStartPost` shell loop runs an optional probe
as positional argv, bounded by the unit's startup timeout.

There is no per-boot frozen declaration. In a sandbox, tenant unit edits follow
normal systemd reload/restart semantics. Editing the TOML inspection copy alone
does not recompile units; use a native recipe step to change a template's apps.

Template apps do not gate guest readiness. Guest readiness means spaced is usable, regardless of
whether an app is starting, ready, failed, or misconfigured. Start, resume, restore,
and wake do not wait for app readiness. Systemd supervises apps independently.

Base-snapshot preparation checks the app units after agent readiness. No apps means
no wait. `active/running` means execution and any `ExecStartPost` probe succeeded;
activating, queued starts and automatic restarts keep waiting. Failed units and
foreground apps that exited are failures. The check returns as soon as all apps
have settled, with one budget per app (`startup_timeout_secs` plus two seconds,
measured from the check). Budgets run concurrently; they do not accumulate.

Each app declares its role in the template with `startup_policy = "best-effort"`
(the default) or `"required"`. Currently only base-snapshot preparation enforces
this policy; other lifecycle operations may adopt it in a later change:

| Policy | Ready | Failed or budget exhausted |
| --- | --- | --- |
| best-effort | Capture | Log app name/reason on the host and capture |
| required | Capture | Reject this snapshot attempt; keep cold boot available |

This implementation enforces startup policy only for base snapshots. It does not
change sandbox create, start, resume, restore or wake readiness. A best-effort
snapshot can contain a starting or
failed app. Readiness is an observation, not a quiescence guarantee: even a required
app can change state between observation and pause. Applications must tolerate
snapshotting and reconnection independently of their readiness probes.

Template-manager calls a node-internal `WaitTemplateApps` RPC. Sbx-mgr authorizes
only a running, non-pool, template-manager-owned sandbox, holds its lifecycle lock,
reads the bounded TOML declaration through its existing guest connection, and runs
one bounded `systemctl show` observer. It never starts or resets app units. The
observer is stopped before capture when the guest connection permits it; its
completed process record may be visible until normal guest retention expires.
When all apps are best-effort, observer failures become host warnings and do not
block capture. If observer cleanup cannot be confirmed, its guest-side 65-second
process timeout bounds any remaining observation after restore. A declaration
read/parse failure remains an error: the host cannot prove that no app is required. Guest binaries do not
change. An older sbx-mgr returns `Unimplemented`, failing preparation rather than
silently skipping required checks.

App output remains in the guest's shared volatile journal. Snapshot-check warnings
are host service logs; they have no new database column. Guest logs are lost when
the disposable preparation VM is deleted. The host logs app names and failure
categories, not command output or environment values.

After a failed preparation, the node keeps a five-minute in-memory backoff per
template name and digest. Successful preparations impose no cooldown. Expired
entries are removed; the cache retains at most 1,024 failures. A new digest can
start immediately. A manager restart or cache eviction permits an extra retry.
The periodic reconciler retries after the interval. There are no new SQL columns
and no stored copy of the runtime spec. Existing successful snapshots are reused
without checking apps again. Failed preparation does not prune old snapshots or
pool members.

Direct pools prefer a base snapshot and cold-boot if none is available. Kubernetes
pool creation already allows cold boot through the shim's ordinary create path.
Cold fallback preserves sandbox capacity, **not application readiness**. Snapshot
preparation is per node and happens after rootfs publication; it is not build-time
validation or release promotion gating. Operators who require prewarmed apps must
prepare and verify snapshots for the intended digest/nodes before promoting a
selector. This feature does not automate that promotion policy.

Do not bake per-sandbox secrets, unique tokens or identity into application startup.
Snapshots clone application memory, random state, files and open connections.
Applications must tolerate that cloning and network reconnection. Cold boot can
start the app before networking is usable; early outbound work needs retries.
App readiness probes affect only their systemd units. They do not gate sandbox
lifecycle operations, so no app-readiness receipt is needed across agent restart
or memory restore. Snapshot preparation does not reset a unit's startup timeout or
restart counters.

PID1 independently restarts each app after two seconds, with five starts per
30-second window per app.
A clean exit stays stopped. The app must remain in the foreground; descendants are
stopped with its unit. Startup is not coupled to spaced restarts. The app remains
systemd's MainPID, preserving signal-exit semantics.

Application, readiness-probe and service setup output share the guest's volatile journal.
Use `journalctl -u space-runtime@web.service` to inspect one app, or
`journalctl -u 'space-runtime@*.service'` for all apps. The seed configures a 16 MiB
journal retention budget with 2 MiB journal files; each app has a base rate limit of
1,000 messages per 30 seconds (journald scales the burst with available space).
Journald's limits govern shared retention, not an exact per-app output allocation
or a guaranteed previous invocation's history.
A noisy app can shorten retention of other guest logs, including spaced's, and
journald processes output outside the app's cgroup. There is no custom log relay,
rotation, journal namespace, custom monitor or notify handshake. Normal journald
restart preserves output streams; stopping it can break them (EPIPE/SIGPIPE).
Diagnostics disappear with guest cleanup. There is no new log API or persisted
failure-detail store.

## Build and rollout contracts

Builder VMs cold-boot with `systemd.mask=space-apps.target`. Each generated app
also has a condition on that argument, so it remains suppressed even if a recipe
reloads PID1 and starts it directly. Before user steps, native builds check the
mask and the existing user-work memory envelope. They need `space.target`, a
finite `space.slice` memory limit, systemd with `Type=exec`, and GNU `env --chdir`.
No native-runtime guest binary is required, but bases built from seed revisions
before the user-work slice was introduced do not satisfy the resource prerequisite
and must be rebuilt. Check the exact parent digest and its slice configuration;
a child's recent build date does not prove that its inherited seed has these limits.
The resource check remains fatal: app units use `TasksMax=infinity` and rely on
their parent slices for limits. Downgrading it to a warning would not preserve the
default per-service task limit used by legacy apps on those older bases.

The builder writes and reads files through its existing SSH channel. It captures
inherited files before user steps, installs replacements for SetRuntime, and checks
exact expected files and links at finalization, including Inherit and ClearRuntime.
Inherited units retain their original policy bytes across builder upgrades. Ordinary
steps cannot change the owned files; use SetRuntime/ClearRuntime. OCI imports clear
native runtime files during guest preparation. Builds inheriting native apps or
using SetRuntime/ClearRuntime require a successful filesystem flush and a confirmed
VM stop before rootfs copy, including when ClearRuntime leaves no apps. Forced stop
after the flush is allowed. No new host-side image mounting or parsing is introduced.

Older protobuf readers drop unknown runtime step bodies; the builder rejects the
missing body instead of silently ignoring startup intent. ECW maps its single
`startCmd` to app `app`. Legacy-only output remains valid during rolling deployment.
Native edits on legacy startup parents and mixed native/legacy output are rejected:
rebuild those lineages from recipes. New seeds omit legacy startup drop-ins;
native app units set their own slice and task settings. Previously published
images retain their compatibility drop-ins.

Roll out without rewriting existing sandbox images:

1. Deploy both changes through the normal pipelines, including the builder-boot
   mask on sandbox managers. Complete that host rollout before building native
   templates. Complete the ECW translator rollout before publishing seeds without
   legacy startup drop-ins: older translators rely on those drop-ins to place
   their apps in the user-work slice. The readiness protocol is unchanged for
   both guest generations.
2. Check the exact bases selected for new template builds. ECW `startCmd` and native
   SetRuntime/ClearRuntime builds fail against bases without the resource envelope
   until those bases are rebuilt. This can leave a build-availability window between
   code deployment and base promotion. Creates from existing published templates and
   existing sandbox lifecycle operations do not run this build check and keep their
   existing behavior. Old custom parents containing legacy apps also require rebuilding
   before native edits. A builder on an old manager fails the missing-mask check
   explicitly, rather than publishing a native build without suppression.
3. Rebuild seed, code-runtime and application templates using exact parent digests
   as planned. Validate templates, prepare snapshots/pools and promote selectors
   for new creates. Seed rebuilds also pick up the shared-journal size settings.
4. Keep existing sandboxes and their artifacts. Pause/resume and suspend/restore
   retain the old guest. Stop/start uses the existing rootfs, or resolves its saved
   template hash if preparation is needed; never retarget it to the latest alias.
   Retain artifacts while sandboxes or snapshots reference them. If both artifact
   and saved hash are missing, start fails explicitly. Pool adoption matches the
   resolved content hash, so old pool members cannot satisfy creates pinned to a
   new build.

The node's default cold-start and snapshot-restore readiness budgets remain
30 seconds for the agent, with the existing shorter warm-resume budget. Explicit
caller deadlines still win. Application startup adds no lifecycle readiness wait;
only base-snapshot preparation observes the bounded per-app readiness policy.

Before deployment, validate cold boot, base snapshots, pool adoption, stop/start,
pause/resume, suspend/restore and agent/node restarts on Firecracker. The disposable
systemd test covers guest process supervision, not VM or network behavior.

Required declarations use recipe schema version 2 on the wire, so an older builder
rejects them instead of dropping their policy. Finish rolling out both node daemons
before publishing required-app templates: an old template-manager does not know
about the new check. Schema-1 apps and ECW apps default to best-effort. Existing
legacy templates without a native declaration skip the check and retain their
current behavior; no guest rebuild is needed for the observer.

## Run the systemd acceptance harness

The fixture changes units and files. Run its shell test only inside a disposable
Linux container with private cgroups and no host mounts. Generate the fixtures on
the development machine (no Linux guest binary is needed):

```sh
cargo run -p pplx-space-build-mgr --example runtime_systemd -- /tmp/space-runtime-fixtures
```

Build an Ubuntu 24.04 or 26.04 image with `systemd`, `systemd-sysv`, and `python3`.
Launch `/sbin/init` with `--privileged --cgroupns=private --tmpfs /run
--tmpfs /run/lock`. Copy the emitted files into `/fixtures` in that container.
Then, **inside the disposable container**:

```sh
touch /run/space-runtime-test-container
bash /fixtures/test.sh
```

The marker is an accidental-execution guard, not isolation. The test checks literal
argv/environment handling, non-root identity, optional probes and failures,
independent restarts, agent and journal restart survival, unit edits, replacement,
clear and builder suppression. For the last check it temporarily replaces proc
command-line views inside the private container mount namespace: systemd containers
read PID1 argv, whereas VMs read the kernel command line.

Remove the container and task-owned image afterward. This explicitly invoked
harness is not part of the workspace test suite. It exercises real PID1 semantics,
not Firecracker boot, networking, pool adoption or snapshot transfer.
