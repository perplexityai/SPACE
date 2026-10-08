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
