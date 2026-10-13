# Sandbox benchmarks

Scripts in this directory benchmark sandbox lifecycle operations on one SPACE
worker node. Each `*.py` benchmark runs on the worker itself, or stages itself
over SSH when given a `target` (`deploy@worker-1` or `worker-1` with
`--ssh-user`). `--pull-results DIR` copies the compact results back.

## Balloon suspend/restore benchmark

`bench-sbx-balloon.py` compares snapshot size, suspend/restore cost, and guest
memory correctness across template variants, for example templates that differ
only in their Firecracker balloon device configuration. It does not configure
ballooning itself, and it does not require a balloon device. A template without
one, such as the baseline, records the balloon fields as `null`.

For each variant, the benchmark runs these steps one variant at a time, with
`--concurrency` sandboxes in parallel:

1. Create `--sandboxes` sandboxes and split them evenly across the workload
   profiles: `build`, `alloc-free`, `file-heavy`, and `idle`.
2. Write a correctness pattern in each guest. The pattern includes random data
   in guest tmpfs (`/dev/shm`) and the rootfs (`/var/tmp`). When `python3` is
   present, a resident process also holds an anonymous buffer. The benchmark
   records the SHA-256 for every part of the pattern.
3. Repeat the following steps for `--cycles` cycles (default 3):
   1. Run the profile workload.
   2. Sample the Firecracker process RSS and `GET /balloon/statistics`.
   3. Suspend the sandbox and measure the snapshot artifacts.
   4. Restore the sandbox, sample RSS again, and verify every checksum.

A checksum mismatch is a correctness failure. If suspend, restore, or
verification cannot run, the cycle is unverified.

| Profile | Guest work per cycle |
| --- | --- |
| `build` | Compiles generated C units with `cc`, or uses gzip when no compiler exists. Streams a blob through the page cache, then deletes all outputs. |
| `alloc-free` | Allocates 2–4 GiB of anonymous memory, fills it with incompressible data, then frees it. Uses tmpfs when `python3` is missing. |
| `file-heavy` | Writes and deletes 1 GiB of 4 MiB files on the rootfs. |
| `idle` | Sleeps for `--idle-secs` seconds. |

The benchmark writes `results.jsonl` with one row per cycle. Each row includes
`variant`, `template`, `profile`, `cycle`, `host`, and `sandbox_id`. A row with
`cycle` 0 records a sandbox that failed before its first cycle. `run.json`
records the arguments, host kernel, and SPACE version. Rows also include:

| Field | Source |
| --- | --- |
| `snapshot_mem_bytes`, `snapshot_mem_disk_bytes` | Apparent and allocated size of `<sandbox cache>/snapshot/memory`. |
| `snapshot_mem_compressed_bytes` | `zstd -3` output size. This is `null` when `zstd` is missing or `--no-compressed-size` is set. |
| `rootfs_snapshot_disk_bytes`, `rootfs_exclusive_bytes` | Allocated size of `snapshot/rootfs.ext4` and its `btrfs filesystem du` exclusive bytes. |
| `suspend_ms`, `restore_ms` | Wall time of `space-sbxmgr suspend/restore --blocking`. |
| `workload_ms`, `workload_guest_ms` | Host-measured exec time and guest-measured time of the profile workload. |
| `fc_rss_before_suspend_kib`, `fc_rss_after_restore_kib` | Firecracker `VmRSS` from the sandbox `fc.pid` link. |
| `balloon_config`, `balloon_stats_before_suspend` | `GET /balloon` and `/balloon/statistics` on the Firecracker API socket. These are `null` without a balloon device or statistics polling. |
| `host_disk_write_bytes_suspend/restore`, `host_btrfs_commits_suspend`, `host_writeback_kib_after_suspend` | Host `/proc/diskstats`, btrfs `commit_stats`, and `/proc/meminfo` `Dirty`/`Writeback`. These values cover the whole host. Use `--concurrency 1` to attribute them to a single sandbox. |
| `guest_kernel`, `firecracker_version`, `space_version` | Versions recorded for comparing runs. |
| `correctness_ok`, `correctness_failures` | `true`/`false` and the mismatched checks, or `null` when the cycle could not be verified. |

### Example: four variants on one worker

Build one template per variant first. The labels can be any names, and
`--baseline` in the summary selects the comparison point:

```sh
scripts/benchmarks/bench-sbx-balloon.sh worker-1 --sudo \
  --variant baseline=bench-balloon-baseline \
  --variant inflation=bench-balloon-inflation \
  --variant reporting=bench-balloon-reporting \
  --variant hinting=bench-balloon-hinting \
  --sandboxes 8 --cycles 3 --concurrency 4 \
  --output-dir /tmp/space-sbx-balloon-run1 \
  --pull-results ./bench-results

python3 scripts/benchmarks/bench-sbx-balloon-summary.py \
  ./bench-results/space-sbx-balloon-run1/results.jsonl --baseline baseline
```

The summary prints correctness counts and the median and p95 for each variant
and metric. `--by-profile` splits these values by workload, and `--json` writes
machine-readable output. The summary also prints the Stage 1 gate. A variant
passes when all of these conditions hold:

- Its median `snapshot_mem_disk_bytes` is at least 10% and at least 64 MiB
  smaller than the baseline median.
- It has zero correctness failures and zero unverified cycles.

Correctness failures or unverified cycles in the baseline invalidate the
comparison, so every variant fails. `--gate-metric`, `--min-reduction-pct`, and
`--min-reduction-mib` change the comparison. `--fail-on-gate` exits with status
1 unless every variant passes.

Measuring the compressed size reads the memory file after suspend and before
restore. Suspend has just written that file, so it is usually still in the host
page cache, but under memory pressure the read can warm pages that restore then
reads. Use `--no-compressed-size` for runs where restore timing matters more
than compressed size.

## Cross-node reconnect benchmark

`bench-sbx-xnode.py` is Stage 2 of the balloon experiment. It measures how long
it takes to reconnect to a suspended sandbox on a different worker, which has
to download the snapshot from object storage. Stage 1 cannot measure this
because it runs on one worker.

It runs from an operator machine on a small pool of dedicated workers. It needs
two things:

- An ECW API key (`$E2B_API_KEY`) and domain. Wake-ups go through ECW connect,
  the same path Computer uses, so the server-side
  `sandbox.reconnect.duration_ms` metric records every sample.
- A kubectl context for the pool's cluster. The harness cordons pool workers
  and reads their sandbox-manager logs.

Each block runs one variant, with one template, for `--block-minutes`:

1. Reservoir sandboxes are created, run their workload, and are suspended.
   After that the running population and bystanders are created. Every
   sandbox gets the Stage 1 correctness pattern.
2. One pool worker at a time is the source. It is cordoned, and only
   sandboxes suspended on it are woken, so each restore lands on another
   worker. The source rotates every `--rotate-seconds`. A sandbox is not
   woken until `--portable-wait-s` after its suspend. With `--database-url`,
   it also waits for the API's restore-portability check. Before the snapshot
   reaches object storage, the API sends the restore back to the source and
   ignores the cordon.
3. Wake-ups arrive at random at `--rate` per minute (`--load loaded`), or one
   at a time every `--quiet-interval-s` (`--load quiet`). Each wake-up is
   paired with the suspend of a sandbox on a target worker, right after its
   workload ends. This makes uploads compete with downloads and refills the
   reservoir.
4. Reconnect time runs from the ECW connect request to the first `true`
   command returning, timed on the operator machine. The guest pattern is then
   re-hashed (`verify_ms`, untimed). Bystanders run `true` every
   `--probe-interval-s` (foreground metric F1).
5. After the block, every worker is schedulable again. The harness reads the
   sandbox-manager `runtime_activated` events and records where each restore
   landed and whether it downloaded its snapshot (`wake_placement` rows). Then
   it deletes the block's sandboxes.

The harness only cordons nodes listed in `--nodes`. It refuses to start if one
of them is already cordoned. It writes `cordon.state` before cordoning a node,
and it uncordons on exit, including Ctrl-C. If a run dies, run it again with
the same `--output-dir` and `--repair`.

```sh
export E2B_API_KEY=...
scripts/benchmarks/bench-sbx-xnode.py \
  --context <kube-context> --nodes ip-10-0-1-1,ip-10-0-2-2,ip-10-0-3-3 \
  --domain <ecw-domain> --sandbox-namespace <ns> --node-namespace <ns> \
  --variant baseline=bench-balloon-baseline \
  --variant reporting=bench-balloon-reporting \
  --load loaded --rate 20 --blocks-per-variant 4 \
  --output-dir ./bench-results/xnode-run1 --dry-run

python3 scripts/benchmarks/bench-sbx-xnode-summary.py \
  ./bench-results/xnode-run1/results.jsonl --baseline baseline
```

`--dry-run` prints the block order and the expected number of measured wake-ups
for each variant. Blocks alternate forward and reversed passes, for example
baseline, reporting, reporting, baseline. Use `--order` to set the order by
hand. To calibrate `--rate`, run baseline-only blocks and raise the rate until
staging p95 reaches 15 s and writeback wait reaches 0.5 s in Datadog.

The summary counts only `cross_node_download` samples. It drops the first
`--warmup-minutes` of each block. It scores failures at the wake timeout in a
failure-inclusive p95. It compares each variant with the baseline using 98.3%
bootstrap intervals and the Metrics Spec decision rules:

- `stop_now`: a correctness failure.
- `regression_stop`: reconnect p99 or bystander p95 is more than 5% worse.
- `inconclusive`: fewer than 2,000 samples or 4 blocks, or the interval
  includes zero.
- `proceed_pending_cause`: p95 improves by at least max(10%, 1 s) and every
  variant block beats its nearest baseline block.
- `stop`: anything else.

`proceed_pending_cause` still needs one cause metric (bytes, codec wait,
writeback wait, or disk pressure) to move the same way in Datadog. The summary
cannot check that. Quiet runs are report-only.

The harness does not yet collect the fio probe (F2), balloon statistics,
per-VM memory, or disk and btrfs metrics.
