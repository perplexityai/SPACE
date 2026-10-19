# Guest file and disk I/O

Status: implemented contract

How a sandbox's files reach its disk and come back out: the virtual disk and
its limits, the guest's upload and read paths, and when written data is
published and when it persists.

## The virtual disk

Each sandbox's root filesystem is an ext4 image attached to the Firecracker VM
as a virtio block device.

- **Cache mode.** The drive uses Firecracker's default `Unsafe` cache mode. The
  device offers no flush, so a guest `fsync` or `syncfs` completes once its
  writes reach the host's page cache. Nothing in the guest makes data survive
  a host failure; the host persists the image through its own writeback, and
  through snapshots for the lifecycle operations that take them.
- **One segment per request.** The device does not advertise more than one
  data segment per request (`VIRTIO_BLK_F_SEG_MAX`), so the guest sends one
  request per physically contiguous run of memory. Page-cache pages are
  scattered in a guest that has run a while, so buffered writeback and
  journal commits mostly reach the disk as 4 KiB requests. Large requests
  need physically contiguous memory, which only huge pages provide reliably.
- **Transparent huge pages.** The guest kernel boots with
  `transparent_hugepage=never`, and `spaced.service` switches it to `madvise`
  before the daemon starts, with synchronous and khugepaged defragmentation
  off. Only memory that asks for huge pages (`MADV_HUGEPAGE`), such as the
  upload buffers below, gets them, and a fault never stalls on compaction. A
  guest without these settings still serves uploads, in smaller requests.

## The drive limiter

Every sandbox's root drive has a Firecracker rate limiter with two token
buckets: operations (requests) and bandwidth (bytes). The defaults are 10,000
operations/s and 100 MiB/s; a warm pool can assign other limits, and each
sandbox carries its own.

The operations budget counts guest requests even when the host serves them from
page cache. At 4 KiB per request, 10,000 operations/s permits about 39 MiB/s,
subject to host contention and the separate bandwidth limit. With the default
30-second burst, the operations bucket holds 300,000 requests.

Limits are saved with each sandbox and preserved across start and restore.
Unassigned creates and warm-pool policies with omitted limits use the platform
default; explicit policy limits take precedence. Existing manually managed pool
configurations keep their stored assignments when their size is edited.

Each bucket holds `disk_io_burst_secs` (sandbox-manager configuration, default
30) seconds of its sustained rate and refills at that rate. The sustained
rates set a sandbox's long-run share of the node's disk. The burst lets an
idle sandbox spend up to that much at once, as fast as the device takes it:
commands that touch thousands of small files, such as reading a cold Python
install, and uploads arrive in bursts.

Every idle sandbox holds its credit, so many sandboxes spending it together
load the host's page cache and disk at once. The per-sandbox limiter does not
make a dense node fair: at hundreds of sandboxes each one's fair share is far
below its limit. That takes a host-level backstop, such as cgroup I/O
weights, which SPACE does not configure.

## Operator ceiling

Node-manager accepts `SPACE_NODEMGR_DISK_IO_MAX_OPERATIONS_PER_SEC`, a positive
integer maximum for each VM's root-drive operations/s. Unset means no operator
ceiling; the assigned default remains 10,000 operations/s. For example, setting
it to `1000` bounds a 10,000 operations/s assignment at 1,000 while leaving a
500 operations/s assignment unchanged. Bandwidth and additional drives are
unchanged. The ceiling also covers builder VMs on nodes where it is configured.
An empty, zero, malformed or out-of-range value prevents node-manager startup.
To leave builds exempt, configure worker node-manager workloads rather than
shared worker/builder values.

The value is read at startup. Apply it to every affected node-manager and
restart those services; running VMs need not be restarted. Cold boots receive
the ceiling in their jail configuration. Configuration is published through a
pinned source and temporary-file rename, with no file or directory fsync,
whether or not a ceiling is set. The unset case preserves the original bytes.
Snapshot loads clamp the saved assignment before applying it, then verify the
loaded drive before adoption or resume. Reusing an existing cold-boot VM also
verifies the ceiling before returning its launch response. An idle restore
launch skips that check until `LoadSnapshot` installs the drives. The sweep
keeps a VM with no drives pending, without a failure or a verified-cache entry.
A background sweep also lowers running VMs, starting immediately and
repeating every ten seconds, with at most four targets in flight and a
two-second deadline per target. Once verified, a VM is skipped until its
sandbox identity, slot, generation or PID changes; departed entries are
pruned on the next successful inventory. Discovery continues, but verified
VMs cause no lifecycle-lock or Firecracker API traffic. Busy or unreachable
targets log a warning and retry on later sweeps. A VM that never answers can
remain above the ceiling indefinitely: enforcement of existing VMs is best
effort and does not stop them or block node readiness. An old node-manager
does not enforce this setting.

Node metrics under `space.node_disk.io_ceiling` report `enabled`,
`maximum_iops`, `verified_vms`, `pending_vms`, `inventory_success` and
`last_inventory_timestamp_seconds`. Pending includes idle restore VMs and
failed, busy or superseded targets: it means unverified, not known
above-limit, and can be nonzero during normal restores. Only interpret
counts with a successful, fresh inventory. The `updates` counter counts
sweep-applied changes; `update_failures` counts failed sweep attempts with a
bounded `timeout` tag, and `inventory_failures` counts discovery errors.
Inventory errors emit `inventory_success=0` without resending counts or
their timestamp; consumers must not assume retained samples. Treat counts as
unavailable until a successful inventory. For individual confirmation,
inspect Firecracker's `GET /vm/config`; node-manager logs identify failed
targets. Configuration gauges are refreshed every ten seconds, including on
disabled nodes; disabled nodes perform no VM inventory or Firecracker API
checks.

Reducing the operations rate retains the installed refill interval and reduces
its token budget proportionally, including burst credit. A missing or disabled
limiter receives a 30-second bucket. Unchanged buckets are not patched, so
reconciliation does not repeatedly replenish their credit.

This is a lowering control: raising or removing the ceiling does not raise an
already-lowered running VM's allowance. Its next normal start or restore uses
the saved assignment, subject to the receiving node's ceiling. Saved sandbox
limits, warm-pool policies, pool configurations and API-reported assignments
are unchanged, so changing the ceiling does not invalidate warm slots. The
operator ceiling is separate from admission ceilings and aggregate host I/O
controls; it does not reserve management capacity or bound combined host load.

The Kind node-manager chart exposes the setting as
`diskIoMaxOperationsPerSec`, empty by default.

## Uploads

Uploads reach the guest's filesystem service as a stream of frames on a gRPC
connection that all of a sandbox's transfers share.

### Where the bytes go

A write that is not an append replaces its destination when it can: a new
file, or an existing regular file that the writing user owns, with one link
and no access ACL, in a directory where the user can create a temporary file.
The bytes go to a temporary file next to the destination, which is renamed
over it when the write completes.

Every other write goes to the destination in place: appends, other files, and
an eligible write whose directory refuses the temporary file. A destination
that cannot be renamed over, such as a bind mount, gets the upload copied into
it in place once the rename fails. None of these is published atomically.

### How the bytes are written

The bytes are staged in physically contiguous memory and written with
`O_DIRECT` wherever the file and its filesystem allow, so each block of data
takes one request instead of one per page.

- **Small uploads** (at most 256 KiB, not appends) are collected in ordinary
  memory, and when the upload finishes are copied into a free upload buffer
  and written direct in one call. They hold no buffer while their frames
  arrive.
- **Larger uploads** are collected in 2 MiB huge-page-aligned upload buffers,
  each written direct once full.
- **Partial blocks.** The final partial block, and anything the filesystem
  refuses to take direct, is written buffered from where the direct writes
  stopped.
- **The buffer pool.** Each filesystem service holds at most eight upload
  buffers (16 MiB). Idle buffers are kept, so the pool keeps its huge pages
  after the guest's memory fragments, and released lazily (`MADV_FREE`), so
  the kernel reclaims them first under memory pressure. Eight buffers keep
  concurrent megabyte uploads at the default bandwidth limit.
- **No waiting for a buffer.** An upload that finds no buffer free writes its
  frames through the page cache as they arrive. Waiting would stop reading
  frames on the connection that every transfer shares.

### Completing, publishing and acknowledging

Every write's data has been written, direct to the virtual disk or into the
guest's page cache, before the write is acknowledged.

- **A replacement** is then renamed over its destination, which publishes it
  atomically: concurrent readers see the previous content (or no file) or the
  new content, never a partial file. Neither the temporary file nor the
  rename is synced.
- **A write in place** is synced (`fsync`) before it is acknowledged. Nothing
  publishes it atomically; the sync puts its bytes on the virtual disk before
  the reply and reports a failed writeback to the writer.
- **A batch write** follows the same rules for each entry.
- **An interrupted upload** is never completed: only the end of its stream
  completes a write, and every hop between the caller and the guest treats a
  failed or cancelled input as a failure, never as that end. A replacement's
  temporary file is discarded, so the destination keeps its previous content
  (or stays absent). A write in place keeps the bytes it received; an
  overwrite in place has already truncated the destination, so it holds only
  that prefix. Host services and guests built before these hops and atomic
  replacement existed can commit the prefix instead.

The public contract (`files:write` in the API reference):

> Creating a file, or overwriting a regular file that the writing user owns
> with no other hard links and no access ACL, replaces the destination with a
> temporary file where it can, which publishes the new content atomically:
> readers see the previous content (or no file) or the new, never a partial
> file. A destination that cannot be renamed over (such as a bind mount), a
> directory that refuses the temporary file, appends and other files are
> written in place, without that guarantee, and synced before the write is
> acknowledged. Acknowledgement does not guarantee that a created or
> overwritten file persists across a sandbox crash, a forced stop or a host
> failure, and a writeback error can surface after it. An ordinary stop
> attempts to flush the sandbox's root filesystem first, but proceeds if that
> attempt fails or times out. No persistence deadline is promised.

What this means for a replacement:

- **Persistence** comes from the guest filesystem's own writeback or a
  lifecycle flush ([Lifecycle boundaries](#lifecycle-boundaries)). Until then
  a guest crash, forced stop or host failure can lose it, leaving the
  destination's previous content, or no new file.
- **Errors.** A direct write allocates its blocks, and ext4 reserves blocks
  for buffered data, when `write` is called, so running out of space fails
  the upload. An I/O error during later writeback does not reach the writer.
- **Crash ordering.** Data written direct is on the virtual disk before the
  rename. For buffered data, ext4's default ordered journaling writes a
  file's allocated data before committing the metadata that exposes it, and
  allocates a file's delayed blocks when it is renamed over an existing one,
  so a crash normally leaves the previous content or the complete
  replacement. A newly created file's buffered part has no such rename
  handling and can be missing after a crash.

Replacements are not synced because a sync forces an ext4 journal commit,
whose 4 KiB blocks are each their own disk request; for small uploads that
costs several times the requests of the data itself, against the drive's
operations limit. To make replacements persistent at acknowledgement
instead, sync the temporary file before the rename (`finalize_written_file`
in `crates/libs/spaced/guest/execution/src/filesystem/write.rs`), keep the
destination's directory open from `open_replacement`, and sync it after the
rename; the comment in `Replacement::commit` marks the place.

## Lifecycle boundaries

Persistence is handled where the sandbox's lifecycle changes, not per write.

- **Ordinary stop.** A stop terminates the VM without shutting the guest
  down, so sandbox-manager first asks the guest to `syncfs` its root
  filesystem. A paused VM is resumed for the flush: pause flushes before it
  freezes the VM, and a write still in progress then can complete after that
  flush, in the paused VM's memory. The whole attempt (reading the VM's
  state, resuming it, connecting, and retrying) has one 30 s budget, and
  node-manager bounds each Firecracker pause or resume on its side too; a
  flush already running in the guest may predate the latest writes, so a busy
  guest is retried until this attempt's own flush runs. If the attempt fails
  or times out, the stop proceeds and records the `stop_guest_flush_failed`
  event and the `sandbox.worker.stop.guest_flush` metric. A delete never
  flushes; it discards the disk.
- **Pause and suspend** prepare the guest for a VM snapshot, flushing its
  filesystems first, and fail rather than proceed without it. A successful
  suspend persists what the guest had written, with the snapshot.
- **Disk-only snapshots** flush the guest's filesystems and then the host's
  copy of the image.
- **A crash, forced stop or host failure** can lose writes that the guest had
  not yet written back.

## Reads

A read crosses the data plane, node-api, sandbox-manager, spaced-host, the
guest's vsock connection, the guest router and, for a user other than the
guest daemon's, that user's identity worker. Its cost is CPU per byte at each
hop.

- **Descriptor handoff.** The identity worker opens the file as the user (the
  `OpenRead` call, worker protocol version 3); the router duplicates the
  worker's descriptor with `pidfd_getfd` and streams the file itself, so the
  bytes do not pass through the worker. A worker without `OpenRead`, or a
  descriptor that cannot be duplicated, falls back to relaying the read
  through the worker. The read counts against its worker's per-user
  admission, which ends with the worker, so the read ends with it.
- **Chunks.** Files stream in 64 KiB chunks through a queue of four, so an
  open read buffers at most 256 KiB.
- **Decoding without copies.** `FileChunk.data` is generated as
  `bytes::Bytes`, so each hop's decoded chunk is a slice of the received
  buffer.
- **Frames.** Every SPACE gRPC server, and the file-path clients (data plane
  to node-api, node-api to sandbox-manager, sandbox-manager to spaced-host,
  spaced-host to the guest), advertise a 256 KiB maximum HTTP/2 frame, so a
  chunk crosses each hop in one DATA frame.

## Known limitations

- Journal commits and buffered writeback reach the disk one page per request.
  A device that takes several segments per request would merge them; that is
  a Firecracker change.
- Nothing bounds the combined bursts of many sandboxes on one host.
