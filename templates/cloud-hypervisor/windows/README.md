# Windows firmware and disk inputs

The `ch-54.0-windows-uefi` environment is an explicit, x86-64-only development
candidate. It is excluded from the default execution policy. It carries pinned
EDK2 firmware and a cold-boot machine contract with Hyper-V enlightenments. It
has no Linux kernel or initramfs and advertises no snapshot or device support.

The standalone path persists and validates the guest image contract,
supplies slot-scoped DHCP, and waits for a declared TCP service instead
of a guest agent. Stop requests ACPI shutdown and waits up to 120 seconds from
the durable cleanup job before forcing cleanup. Stop completion records the
observed outcome in the existing sandbox event stream.

This is a development environment, not a fleet default or a qualified Windows
release. Export, cluster distribution, snapshots, warm pools and agent APIs are
refused. Unexpected VMM-exit recovery remains a separate
[acceptance dependency](https://github.com/perplexityai/SPACE/issues/953).

Before producing version-4 templates, deploy the reader guard that checks
metadata and manifest schemas before cache expansion or filesystem operations.
Older execution readers reject version 4, but older template registration and
raw import paths can expand a disk before performing that check. Do not roll
readers back past the guard release while these templates are present. Initial
Windows imports must stay local to an upgraded standalone node. Keep fleet
publication and distribution disabled until destination capability checks exist.

## Firmware

The firmware is the x86-64 `CLOUDHV.fd` from
[Cloud Hypervisor EDK2 ch-97eeb7b09](https://github.com/cloud-hypervisor/edk2/releases/tag/ch-97eeb7b09):

- Size: 4,194,304 bytes.
- SHA-256: `dc2fc8f0e43b96712d9fccc52a3a590769606412b3e1bc911d217addd3bef624`.
- Component kind: `firmware`; role: `firmware`; format: `edk2-fd`.

Stage the verified download as
`firmware-ch-97eeb7b09--dc2fc8f0e43b/image` below the configured runtime-manager
boot-artifact feed (`--boot-artifacts-source <absolute-directory>`).
The feed supplies bytes, not trust: runtime-manager and
node-manager independently verify the catalog pin through the existing
component installation path.

```sh
space-rtmgr components install --kind firmware ch-97eeb7b09 \
  sha256:dc2fc8f0e43b96712d9fccc52a3a590769606412b3e1bc911d217addd3bef624
```

Firmware is passed to Cloud Hypervisor's `payload.firmware`, and is linked into
the contained launch root as `firmware.fd`. It never occupies the kernel role.

## Prepared guest disk

Start with a privately prepared Windows Server Core evaluation disk for
functional testing. Follow the upstream
[Windows image preparation instructions](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/v54.0/docs/windows.md):
use a GPT/UEFI raw disk, install the VirtIO block and network drivers, and enable
SAC and RDP with NLA enabled. Keep the adapter on DHCP, set the power-button
action to shut down, and disable shutdown-event-tracker prompts. Set `RealTimeIsUniversal` (DWORD) to `1` under
`HKLM\SYSTEM\CurrentControlSet\Control\TimeZoneInformation` because the VMM
presents a UTC hardware clock. Include the VirtIO socket driver for a future guest agent. Provision
credentials outside this repository; do not publish installed evaluation disks.

A firmware guest declares an explicit image contract in version-4 template
metadata and manifests. The initial agent-absent shape is:

```json
{
  "guest_image": {
    "version": 1,
    "disk": "opaque_raw_disk",
    "network": "slot_dhcp_v1",
    "agent": "absent",
    "readiness": { "kind": "tcp_service", "port": 3389 }
  }
}
```

The complete documents also require an exact execution-environment binding.
TCP readiness is not evidence of guest-agent support. An image with an agent
can declare `{"kind":"agent"}` when that combination is qualified; the contract
does not equate a particular OS with permanent absence of an agent.

The existing `rootfs.ext4` artifact filename remains the storage locator. Its
name does not determine the disk format. Opaque disks are copied and hashed as
bytes. Growth extends only the backing file; the guest must grow its own
partition/filesystem. Shrinking, host filesystem repair and recipe builds are
refused. Manifest/metadata disagreement is rejected before disk expansion.

## Local operator workflow

Deploy the schema guard first, then the firmware-capable runtime supervisor
before node-manager, sandbox-manager and template-manager. The supervisor also
resolves compiled environment references; updating only the managers leaves
Windows launches failing with `unknown compiled environment reference`, even
when node-manager successfully resolves the environment and registration works.
Validate a complete launch through the supervisor before declaring the node ready.

After deploying those components, add
`ch-54.0-windows-uefi` to the node's explicit allowlist while keeping its existing
default. Resolve its catalog reference through `space-nodemgr environment
--reference <reference>` and use the acknowledged execution metadata in the
version-4 image documents. Installing firmware alone does not allow execution.

Prepare the raw disk and metadata under a `build.importing.*` directory beneath
the template's cache directory, with the existing template-manager ownership.
Register it through the local operator command:

```sh
space-tmplmgr register --template windows-evaluation \
  --staging-dir build.importing.windows
space-sbxmgr create --template windows-evaluation --content-hash <digest> \
  --environment <reference> --auto-lifecycle-off
space-sbxmgr start <sandbox-id> --memory 4096 --blocking --ready-timeout-secs 120
```

Use the full sandbox UUID for operator channels:

```sh
sudo space-nodemgr guest-channel <sandbox-uuid>
sudo space-nodemgr guest-channel <sandbox-uuid> --port 3389 --listen 3390
```

The first command forwards the serial console. The second accepts one connection
on node loopback; reach it through an authenticated SSH local forward. RDP still
requires Windows credentials. Neither channel exposes the VMM control socket.
Use the deployment's client TLS environment when required by its node RPC setup.

`space-sbxmgr stop <id>` and `start` preserve the opaque system disk. Poll
`space-sbxmgr list` until stopped before starting again. The blocking Stop RPC
can time out before the 120-second guest shutdown budget; its durable cleanup
continues after that response.
For native SSH, enable Windows OpenSSH in the guest, provision an authorized
operator public key, and trust the guest's host key through the authenticated
console. Configure the caller's OpenSSH `Host space-<full-sandbox-uuid>` entry
with its `IdentityFile` and verified host key (or a `HostKeyAlias` for an
existing trusted entry). Then use `space-ssh <sandbox-id>`; Windows defaults to
`Administrator`, and `--user` and `--identity-file` override the login settings.
The CLI uses the execution-fenced node channel on port 22 and requires host-key
verification. It does not change Linux guest SSH or inject Linux environment
variables into Windows. Native SSH requires OpenSSH running in the guest; it
does not provide SPACE's guest-agent APIs.

SPACE command/file APIs still require a guest agent and intentionally refuse
this image, including while stopped. They must not wake it first.

## Contract and recovery boundaries

Agent availability is an explicit `agent` field, independent of readiness.
`GuestProfile` resolves the image contract against the compiled boot contract;
node-manager repeats that validation before disk mutation. DHCP follows the
network contract, and disk expansion follows the disk contract. Schema 4 is a
reader version, not a mutually exclusive feature marker: later fields must
compose with existing ones, and readers reject fields they cannot preserve.
The unpublished development contract must be recreated with the explicit agent
field; it must not be silently reinterpreted on an existing development database.

| Situation | Result and progress owner |
| --- | --- |
| Unsupported document schema | Template reader refuses before expansion or injection. |
| Old node receives the new environment | Exact environment resolution refuses before preparing the disk. |
| Old sandbox-manager omits the image contract | Node-manager rejects the default disk/network contract for UEFI before disk work. |
| Probe capacity is busy | Readiness retries within its activation deadline; operator channels have a separate budget. |
| Manager restarts while DHCP is needed | The 30-second reconciler reads durable image intent and recreates the fenced responder. |
| Execution is superseded | DHCP stops and operator channels close; stale requests cannot attach to the replacement. |
| ACPI shutdown fails or exceeds its persisted budget | Cleanup forcibly releases the runtime and reports an unclean outcome; the disk remains available for start. |

There are no firmware capability-discovery RPCs. Safe rollout depends on the
reader guard, exact environment admission and durable metadata validation, not
on a feature bit. Drain firmware guests before rolling back to readers that do
not support their metadata. The migration follows the runtime-upgrades migration;
never rename a migration after production deployment.


## Native spaced readiness development

Build with an explicit Cargo target and a matching cross-toolchain:

```sh
cargo build --locked --release --target x86_64-pc-windows-gnu \
  -p pplx-space-spaced-guest-svc --bin spaced-svc --no-default-features
```

Use a MinGW-w64 linker for Windows GNU or a native Windows MSVC toolchain for
`x86_64-pc-windows-msvc`. Linux builds use `x86_64-unknown-linux-musl` or
`aarch64-unknown-linux-musl` with the corresponding musl toolchain.
`protoc` runs on the build host, not the guest target; install its includes
there or set `PROTOC` and `PROTOC_INCLUDE`. Native tests remain separate from
cross-compilation. The Windows backend uses the
same guest bridge protocol as Linux. It runs under the Windows service manager
and bridges synchronous VirtIO socket calls onto bounded async streams; it
cannot use Tokio's Windows TCP socket selector for this provider.

Install the signed VirtIO socket driver and its Winsock provider first. In the
private guest, run `install-spaced.ps1 -Executable <path-to-spaced-svc.exe>` as
Administrator. The service depends on `VirtioSocket`, accepts only host CID 2,
and reports Running only after its listener binds. Startup failures are also
written to the Windows Application event log. `spaced-svc.exe --console` runs
the same backend in the foreground for diagnostics.

For this development stage, the version-4 image contract may declare
`"agent":"spaced"` and `"readiness":{"kind":"agent"}`, retaining the opaque
disk and slot DHCP settings. Node-manager derives the host's Windows agent
policy from the recorded execution and image contract. Guest claims cannot
widen that policy. Deploy the matching node-manager and spaced-host before
starting this image; older peers refuse it. Before exposing process/file-enabled
images to tenant APIs, also deploy the API and data-plane path/metadata changes:
older relays reject Windows paths or present legacy POSIX metadata. Keep these
images standalone and operator-only until that rollout is complete.

The native backend supports process and file operations when image preparation
runs `install-spaced.ps1 -Executable <binary> -EnableOperations`. This provisions
a regular local `user` account with a random credential in the SYSTEM/admin-only
service directory. Existing accounts are never silently adopted. Without this
configuration the guest advertises no process or file capabilities. The trusted
host policy intersects the guest's operation list with the Windows image policy.

Processes run in session 0 with that account's loaded profile and environment,
not as SYSTEM or in the RDP desktop. Direct `.exe` argv uses MSVC quoting;
`.bat`/`.cmd`, PTYs and signals other than SIGKILL are refused. Standard input,
EOF, stdout/stderr, exit codes, timeout and SIGKILL are supported. Malformed
signal numbers return invalid argument; valid unsupported signals return the
typed capability refusal.
A suspended child joins a non-breakaway, kill-on-close Job Object before running.
Termination covers processes in that job, not work delegated to other services.
Output retention is budgeted from guest RAM (1/64, clamped to 16–64 MiB),
including event storage and allocation headroom, across at most 512 records.
Each record retains 16–64 KiB of output; a cursor older than that window
returns `PROCESS_OUTPUT_LAGGED` instead of silently skipping output.
The primary reader applies backpressure; disconnecting it releases the
producer and leaves bounded replay. A valid reattachment can reclaim backpressure. After process exit, output drains for up to
one second so a stalled reader cannot hold cleanup indefinitely. Completed records
expire after 15 minutes or earlier under the 512-record bound. Lost Start replies
reattach by idempotency key while the record is retained.

Files use Windows local-drive paths and the same account's ACLs. Relative paths
start at its profile directory. UNC/device namespaces, alternate data streams,
and DOS device aliases are refused. Read, write, append, stat, list, mkdir,
rename and remove are supported; watches are not. Writes replace through a
same-directory temporary file; append requires an idempotency key. Mutation
completed outcomes are retained for 15 minutes, with at most 16,384 records
and 16 MiB of retained keys. Pending or uncertain outcomes remain recorded
and are never automatically replayed. Limits are 16 MiB per write/batch,
eight active filesystem workers and four uploads. Spaced-host file relays carry
Windows metadata. Managed Windows admission and public API/ECW exposure remain
separate work; these operator checks do not qualify those surfaces.

Volume flush and UTC synchronization are deferred to separate qualification work. The image still uses ACPI shutdown; base snapshots,
warm pools and automatic memory lifecycle remain refused. Do not publish an
evaluation disk or its provisioned credentials with this source.

The `node.windows_agent_readiness` typed smoke checks the pinned image contract,
then creates a sandbox, waits for agent readiness, stops it, cold-starts a new
execution and deletes it. Provision OpenSSH for this smoke: its unauthenticated
banner through the fenced node channel proves DHCP networking on both boots.
It uses the same explicit standalone-worker opt-in
as `node.windows_sandbox_lifecycle`. It passed on an x86-64 Linux/KVM worker
with Cloud Hypervisor 54 and a Windows Server 2025 evaluation image. This is
readiness and cold-lifecycle evidence, not process/file or memory-restore evidence.

Memory lifecycle remains unqualified: repeated VirtIO-socket reconnects stalled
after a direct Cloud Hypervisor restore. Base snapshots and warm pools remain
refused until restored connections pass without intervention.

The `windows_operations` Rust example exercises the guest bridge directly or
spaced-host (`--host` / `--sandbox`): regular identity, separate output streams,
stdin/EOF, exit status, replay, job timeout/descendant cleanup, file CRUD,
Windows metadata, append retries and denial of the service credential path.
`node.windows_agent_operations` runs it against a managed sandbox before and
after cold restart, then deletes the sandbox. Set
`SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE` to the built example's absolute path on
the worker. This is an operator-only host API check; it does not bypass the
API route-token requirement on sandbox-manager's tenant process endpoints.
Public and ECW Windows path/metadata changes are deferred to managed Windows
admission and its end-to-end tests.
Live public-API coverage also requires a managed-Windows admission and routing
path: current guest-image admission refuses these images on control-plane-managed
nodes. The standalone operator probe does not qualify that path. Retain it until
a public-API scenario has passed against an authorized Windows test deployment;
do not remove the admission restriction merely to run the scenario.
