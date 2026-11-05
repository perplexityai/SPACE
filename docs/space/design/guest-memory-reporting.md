# Guest free-page reporting policy

Free-page reporting lets the guest return eligible free memory to the VMM
without reducing the guest's configured memory size. Reporting is not balloon
inflation: the balloon policy must explicitly enable reporting, while this
guest setting controls the smallest eligible buddy order.

For native template builds, the reporting device is requested separately in
the recipe:

```json
{
  "build_config": {
    "balloon": {"mode": "report", "stats_polling_interval_s": 1}
  }
}
```

The API preserves this explicit policy. Omitting `balloon` preserves the
existing build default; setting the guest order alone does not create a device.

## Seed-baked configuration

The seed builder writes order `0` into the guest's `spaced.service`
`ExecStartPre` configuration. At guest boot, systemd applies the setting to
`/sys/module/page_reporting/parameters/page_reporting_order` and verifies its
readback before starting the unchanged daemon. There is no new `spaced-svc`
flag, Rust configuration, environment variable, or maintenance API.

On a supported 4 KiB-page guest, order `0` permits 4 KiB extents, order `4`
permits 64 KiB extents, order `6` permits 256 KiB extents, and order `9` permits
2 MiB extents. Lower orders return more fragmented free memory, but can
increase reporting and subsequent allocation costs.

The seed-build container must not write this sysfs parameter directly: its
chroot shares the builder's kernel. Instead, the rootfs carries the startup
configuration, just as it carries the adjacent transparent-hugepage policy.
Kernels without the reporting parameter skip the setting; an exposed parameter
that cannot be written or does not read back as `0` fails service startup.
The setting does not itself attach a reporting device or change guest capacity.

Rebuild the seed and the derived template chain to adopt this configuration.
Previously captured snapshots do not rerun boot commands on restore and are
not changed by rebuilding a seed. Rebuild their lineage rather than assuming
an old snapshot acquired the setting. This patch does not publish or promote
templates, modify existing sandboxes, or deploy a new guest binary.

## Explicit post-setup cache release

After setup has finished and the application is healthy, a caller may explicitly
release clean guest setup cache:

```sh
sync && echo 3 > /proc/sys/vm/drop_caches
```

Run this ordinary command as root inside the guest, through an existing
guest-execution path, never on the worker host or in the seed-build chroot.
It synchronizes guest filesystems and writes `3` to the guest kernel's
`drop_caches`; no new guest-daemon command is needed. Keep it at the workload's
build/setup finalization boundary. A seed-build-time flush cannot clear cache
created by later Docker imports or workload setup.

Cache release is not scheduled or invoked automatically by daemon startup.
It affects all clean cache in that guest kernel, not just one application's
files. Use a deliberate setup boundary, not a periodic timer or an operation
concurrent with latency-sensitive work. Dirty data must be written out, and
later file reads can incur cold-cache I/O.

This does not release host rootfs cache, change VM memory limits, or require a
host-wide cache flush. File-backed charges outside reclaimed guest anonymous
memory can remain in the sandbox's host cgroup.

## Measured threshold tradeoffs

An isolated two-host `i4i.metal` experiment used 4-vCPU, 16 GiB guests and two
Docker application setup workloads. Each workload/configuration pair had
three fresh repetitions, for 30 setup runs across the five configurations.
The reporting configurations combined the stated order with one explicit
guest cache release after application readiness. The control did not enable
reporting or drop guest cache. No host cache release was used in these setup
measurements.

The workload-specific images are not included in this change, so these
aggregate observations are not a self-contained public benchmark. These
measurements do not constitute validation of every rebuilt seed lineage.

The following are descriptive ranges, not fleet percentiles or a memory cap.
Charged RAM is the sandbox's host cgroup `memory.current` after setup and health
checks, including guest-backed anonymous memory and remaining file charges.
The CPU window runs from just before cache release through 90 seconds of
settling; the inclusive cgroup CPU includes guest execution and VMM work.
Guest busy CPU comes from the guest kernel's CPU counters. Host-only VMM CPU
subtracts the VMM's accounted guest execution; neither CPU column should be
added to the inclusive column.

| Configuration | Workload A charged GiB | Workload B charged GiB | Inclusive CPU-seconds | Guest busy CPU-seconds | Host-only VMM CPU-seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control | 16.125 | 16.125 | 0.84–1.37 | 0.48–0.63 | 0.17–0.61 |
| Order 9, 2 MiB | 12.13–12.51 | 12.48–12.57 | 4.89–6.28 | 4.29–4.95 | 0.37–1.44 |
| Order 6, 256 KiB | 8.62–8.86 | 8.53–8.62 | 5.04–6.01 | 4.31–4.78 | 0.74–1.13 |
| Order 4, 64 KiB | 5.18–5.26 | 5.27–5.30 | 5.80–6.98 | 4.56–4.91 | 1.34–2.29 |
| Order 0, 4 KiB | 3.257–3.316 | 3.470–3.478 | 7.38–9.49 | 4.72–5.33 | 2.72–4.21 |

All 30 setup health checks passed. The order-0 result is approximately a
4.6–4.9× reduction from control, not a 10× reduction. Whole-setup inclusive CPU
ranges overlapped: 856–962 CPU-seconds for control and 899–931 CPU-seconds for
order 0. These samples do not establish a precise whole-task CPU overhead.

Subsequent allocation/free churn can increase the charged memory again.
For workload B, four fully touched 4 GiB allocation/free cycles left
4.88–4.97 GiB charged without a host cache release. Therefore the post-setup
3.26–3.48 GiB range must not be presented as a steady-state upper bound.
Fleet CPU effects, cold-cache latency, sustained pressure, and end-to-end
application throughput require a separate load test before selecting a broad
rollout policy.

## Fresh confirmation before the seed-only revision

A subsequent rerun used an experimental statically built guest CLI to perform
the same sysfs write and explicit guest cache release. That CLI is not part
of this seed-only change. Six new control guests and six new
order-0 guests repeated the same two application setup workloads three times
each. The reporting-enabled templates had been built through an isolated
native-build API canary implementing the explicit-policy mapping; the current
typed API mapping is separately covered by its before/after regression test.
This was not a deployment of a new shared API or template.

All 12 setup health checks and all 12 post-allocation health checks passed.
Guest and VM-cgroup OOM counters remained zero. No host cache hint was used.

| Configuration | Workload A post-health GiB | Workload B post-health GiB | Cleanup/settle inclusive CPU-seconds | Guest busy CPU-seconds | Host-only VMM CPU-seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control | 16.125 | 16.125 | 1.01–1.94 | 0.78–1.00 | 0.03–0.88 |
| Order 0 plus guest cache release | 3.264–3.306 | 3.490–3.503 | 8.28–9.99 | 5.23–5.43 | 3.10–4.22 |

This confirms a post-setup reduction of 4.60–4.94×. Whole-setup inclusive CPU
was 884–936 CPU-seconds for control and 894–965 for the candidate, again not
enough to establish a precise whole-task overhead percentage.

The four-cycle, fully touched 4 GiB allocation/free test had the following
six-run medians. Memory is sampled after the final 15-second recovery window,
before the subsequent application health check; CPU excludes that health check.

| Configuration | Charged GiB after churn | Inclusive CPU-seconds | Guest busy CPU-seconds | Host-only VMM CPU-seconds | Allocation/touch wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control | 16.125 | 12.96 | 12.78 | 1.23 | 10.85 |
| Order 0 plus guest cache release | 4.757–4.881 | 16.63 | 14.56 | 4.57 | 13.23 |

These results establish the tested kernel-policy/cache-release behavior, not
an end-to-end boot or rebuild validation of the revised seed configuration.
Validate guest readback and workload health on the rebuilt lineage before
promoting it; existing snapshots are not retroactively modified.

The candidate used 28.3% more inclusive CPU and 22.0% more allocation/touch
time in this rerun. These are descriptive microbenchmark medians, not
agent-task latency, fleet percentiles, or a sustained 5× reduction.
