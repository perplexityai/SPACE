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

## Opt-in configuration

Set `SPACE_FREE_PAGE_REPORTING_ORDER` in the guest daemon's environment before
starting `spaced-svc`. Values are integers from `0` through `9`. On a supported
4 KiB-page guest, order `0` permits 4 KiB extents, order `4` permits 64 KiB
extents, order `6` permits 256 KiB extents, and order `9` permits 2 MiB extents.
Lower orders return more fragmented free memory, but can increase reporting
and subsequent allocation costs.

An unset variable leaves the kernel or restored snapshot setting unchanged.
This patch does not change existing template defaults, enable a balloon policy,
publish a template, or deploy a new guest binary. An explicit invalid value,
missing kernel parameter, or failed write/readback fails daemon startup rather
than silently running with a different policy.

For an already-running guest, the same validation is available as a one-shot
operation:

```sh
SPACE_FREE_PAGE_REPORTING_ORDER=0 spaced-svc --configure-memory-reporting
```

The setting is guest-side, not a host environment toggle. A template can bake
the environment into its guest service before the daemon starts, or a caller
can use the explicit maintenance command inside a running guest. Restored
guests need their selected setting applied before the measured workload.

## Explicit post-setup cache release

After setup has finished and the application is healthy, a caller may explicitly
release clean guest setup cache:

```sh
spaced-svc --reclaim-setup-cache
```

This command synchronizes guest filesystems, writes `3` to the guest kernel's
`drop_caches`, and exits. If the order environment variable is also present, it
applies and verifies that setting first. Both maintenance operations require a
local VSOCK CID that identifies a guest; host, local, hypervisor, and wildcard
CIDs are rejected before changing the kernel setting or releasing cache.

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
aggregate observations are not a self-contained public benchmark. The unit
tests validate policy and safety behavior, not the reported memory reduction.

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

## Fresh compiled-CLI confirmation

A subsequent rerun used the statically built guest service from this change,
not shell-equivalent sysfs/cache writes. Six new control guests and six new
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

The candidate used 28.3% more inclusive CPU and 22.0% more allocation/touch
time in this rerun. These are descriptive microbenchmark medians, not
agent-task latency, fleet percentiles, or a sustained 5× reduction.
