# Net-gateway TCP bench and smoke harness

CPU, memory, and throughput per TCP workload for the per-sandbox net-gateway,
with a before/after comparison across two git revs of the gateway library.

Runs on any Linux box with root and `nftables`. Needs no Firecracker, no systemd,
and no SPACE services.

For the merge-blocking policy matrix, authenticated ingress, DNS, UDP, and
runtime policy changes against `space-netgtw-svc`, use the adjacent
[gateway traffic gate](../traffic/README.md). This directory owns performance
workloads and comparisons.

This replaced `scripts/netgtw/{bench,smoke,lib}.sh`, which drove the gateway
inside a real Firecracker sandbox on a worker. Their TCP measurement and their
allow/deny matrix are covered here, more finely. This performance harness does
not cover the following parts of those scripts:

- **The guest path.** They drove traffic from inside a Firecracker guest over
  `tap0`, so virtio-net and the sandbox lifecycle were in the measurement.
- **Runtime policy changes and real egress.** `smoke.sh` switched policy through
  the control socket (`space-sbxmgr network-policy --set open|close`), waited on
  the `network-state` ready transition, and reached the real internet — a DNS
  DNSKEY query to 1.1.1.1 large enough to exercise UDP reassembly, and a 1 MiB TLS
  fetch. Here the policy is installed once at startup and every destination is the
  bench upstream. The traffic gate covers runtime policy changes, local DNS/TLS
  peers, and reserved DNS via public DNS-over-TLS. Deployed smoke tests cover
  the complete guest internet path.
- **UDP measurement.** `bench.sh` timed UDP latency and bulk send. The UDP proxy
  runs here, so it is in the idle cost, but no workload drives datagrams through
  it. The traffic gate validates UDP policy behavior without benchmarking bulk
  throughput.

`scripts/bench-netgtw-throughput.sh` is a separate live-worker script that
compares throughput with TPROXY against the same path with the gateway bypassed.
It answers "what does the gateway cost at all", not "what did this change cost",
and is untouched by this.

## Correctness and performance, in one pass

Both, over the same registry, because a measurement of a broken path is worse
than no measurement:

- **Correctness** lives inside each workload as an `Err` return — byte-exact
  echoes, exact lengths, the upstream's own byte count, the ClientHello surviving
  a non-consuming peek, guardrailed destinations refused, a half-closed direction
  still draining. `smoke` runs each workload once and fails on any of them, so a
  workload cannot be measured without also being checked. Two run-level gates sit
  above that: a deny-all gateway must refuse the upstream (otherwise the harness
  could be measuring traffic that bypassed the proxy entirely), and the kernel's
  dropped-handshake counter must not move during the measured bench.
- **Performance** is CPU (nanoseconds, summed over the gateway's threads), peak
  RSS, throughput, wall-clock percentiles, and per-operation latencies — per
  workload, over K iterations, with a noise-aware before/after comparison.

## What is real and what is not

Real: `tcp_proxy::run` and `udp_proxy::run` as shipped, on `IP_TRANSPARENT`
sockets, reached through nftables TPROXY rules that mirror
`setup_netgtw_tproxy_rules`; the policy installed through the production
`GatewayNetworkState::refresh`; the `SO_MARK`ed upstream connect; the TLS
ClientHello peek, driven by real ClientHello bytes on the wire.

Not real, and deliberately:

- **No guest.** A network namespace plays the sandbox. Firecracker, virtio-net,
  and the tap device are absent, so absolute latency is lower than in production
  and absolute throughput is higher. Comparisons between two revs on this
  harness are meaningful; the absolute numbers are not production figures.
- **No host-control RPC.** The policy comes from a loader that returns a fixed
  document. Nothing else about the state machine is bypassed.
- **No gRPC control socket**, so no policy resweep, injection rules, or ingress.
- **No pre-TPROXY guardrail chain.** Production drops the platform hard-deny
  CIDRs in nftables before the gateway sees them. Here they reach the gateway so
  the `denied-destination` workload measures the gateway's own deny path rather
  than an nftables drop.
- **No real destinations.** Every workload talks to the bench upstream. Nothing
  here resolves a name, reaches the internet, or completes a TLS handshake.
- **No TLS interception.** The proxy is run with no injection rules and its MITM
  state at the default, which is the state a gateway is in until the control RPC
  moves it. So `tls-sni` measures the ClientHello peek on a passed-through flow,
  not a terminated one. Measuring the interception path needs a flag here and
  workloads that expect a terminated connection.
- **No UDP workloads.** The UDP proxy runs, so it is part of the idle cost and of
  the process shape, but nothing drives datagrams through it.

## Guarding the harness itself

Its Rust half has unit tests (`cargo test -p pplx-space-netgtw-bench`), including
regression tests for each way it once measured wrongly: the upstream's byte
accounting, its backlog depth against a burst nothing is accepting, the deny
probe returning rather than waiting on the peer, and the report's noise-aware
flag. Two invariants are compile-time `const` assertions rather than tests, so a
bad constant fails the build.

The shell harness owns the half that lives outside Rust or in constants shared
with the gateway, where drift can yield plausible numbers rather than an error:
the TPROXY ports, marks, and routing table must equal the gateway's own; the
chain priorities and guest device name must match production's ruleset;
`connection-limit` must remain sized against the real admission cap and listener
depth; the ephemeral-port settings and inter-wave pause must stay in place; the
rev cache key must cover the harness source; and `run.sh` must read
the namespace names from `topology.sh` rather than keeping a second copy.

## Running it

```bash
# Correctness only, fast: every workload once at 5% of its size.
sudo -E scripts/netgtw/bench/run.sh --smoke-only --scale 5

# Measure the working tree.
sudo -E scripts/netgtw/bench/run.sh --iterations 7

# Before and after a change, with a comparison table.
sudo -E scripts/netgtw/bench/run.sh --rev main~1 --rev main --iterations 7

# One workload, iterated hard.
sudo -E scripts/netgtw/bench/run.sh --workloads bulk-echo --iterations 25
```

Raw per-iteration samples land in `$OUT_DIR/<label>.json` (default
`/tmp/netgtw-bench`). Re-render or re-compare them at any time without
re-running:

```bash
target/release/netgtw-bench report --baseline /tmp/netgtw-bench/abc1234.json
target/release/netgtw-bench report --baseline before.json --candidate after.json
```

## The pieces

| | |
|---|---|
| `topology.sh` | the three namespaces, the TPROXY rules, the policy routing |
| `build-rev.sh` | one harness binary per git rev of the gateway library |
| `run.sh` | sequences: build, topology, enforcement check, smoke, bench, compare |
| `crates/apps/net/gateway/bench` | the harness itself — upstream, gateway, workloads, sampler, report |

```
[client]  198.18.0.2 --veth-- 198.18.0.1  [gw]  198.18.1.1 --veth-- 198.18.1.2  [upstream]
  guest                     tap0                 up0                    eth0
```

The client needs a namespace of its own because TPROXY only runs in the
prerouting hook, which locally generated packets never traverse. Its traffic
therefore has to *arrive* in the gateway's namespace, exactly as a guest's does
over the tap device. Forwarding stays off in the gateway namespace, so nothing
reaches the upstream except through the gateway's own sockets — a flow the
gateway denies cannot leak past it.

`run.sh` proves that before trusting any number: it starts a gateway holding a
default-deny policy and asserts it refuses the upstream. A harness that had
stopped intercepting would otherwise pass every workload, with traffic simply
reaching the destination directly.

## Registering a workload

One function and one line. In
[`crates/apps/net/gateway/bench/src/workload.rs`](../../../crates/apps/net/gateway/bench/src/workload.rs):

```rust
async fn slow_start_download(env: &Env) -> Result<Measured, WorkloadError> {
    let bytes = env.scaled(BULK_BYTES);
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Source, bytes)).await?;
    let received = drain(&mut stream).await?;
    if received != bytes {
        return Err(WorkloadError::ShortRead { expected: bytes, actual: received });
    }
    Ok(Measured {
        bytes_from_upstream: received,
        connections: 1,
        operations: 1,
        ..Measured::default()
    })
}
```

```rust
pub const REGISTRY: &[Workload] = &[
    // …
    workload!("slow-start-download", "what this exercises", slow_start_download),
];
```

That is the whole contract. The runner picks it up for both `smoke` and `bench`;
the report derives throughput and cost-per-unit from whatever the workload counts
in its `Measured`.

Two rules make the registry worth trusting:

- **A workload checks itself.** Correctness lives in the workload as an `Err`
  return, not in a separate assertion table, so a workload cannot be measured
  without also being verified. `smoke` is "run each one once; any `Err` fails".
- **A workload bounds its own waits.** Anything that waits on the far side
  closing must have a deadline, or a regression that makes the gateway hold flows
  open reads as a hang rather than as a failure. See `bytes_carried`.

If the workload needs the upstream to behave in a new way, add an `Op` to
[`wire.rs`](../../../crates/apps/net/gateway/bench/src/wire.rs) and a match arm
in [`upstream.rs`](../../../crates/apps/net/gateway/bench/src/upstream.rs).
Workloads whose first bytes are fixed — anything the ClientHello peek must see —
dial `env.raw_echo` instead, which echoes without reading a header.

## Reading the report

One row per workload:

| column | meaning |
|---|---|
| `wall p50` / `wall p99` | percentiles over the measured iterations |
| `MiB/s` | both directions summed, over the median iteration's wall time |
| `cpu ms/it` | gateway CPU per iteration, nanosecond resolution |
| `cpu us/MiB` | the number to watch for a relay change |
| `cpu us/op` | the number to watch for a per-connection or per-round-trip change |
| `op p50` / `op p99` | the workload's own per-operation latencies |
| `rss peak` | highest gateway RSS sampled inside the window |

A dash means the workload does not produce that quantity: `idle` and
`denied-destination` move no bytes, so they have no throughput, and reporting
`0.00 MiB/s` would read as a stalled relay rather than as "not applicable".

Gateway CPU is summed over `/proc/<pid>/task/*/schedstat`. The per-process
counters in `/proc/<pid>/stat` are quantised to 10 ms clock ticks, which is
coarser than several workloads take to run; the thread-group `schedstat` file
reports only the leader thread, which on a Tokio runtime is nearly idle.

The comparison flags a metric that moved more than 10% in the worse direction
with a trailing `!`. Treat it as a prompt to re-run, not a verdict: successive
runs on a shared devbox differ by a few percent on their own. `idle` is the
control — it should not move.

`rss peak` moves together across every workload in a run, because it tracks the
gateway process's resident set rather than anything a single workload allocated.
Read a change in it as one observation about the process, not as fourteen — and it
is reported but never flagged, for the same reason: it barely varies inside a run
and varies substantially between them, so the noise column cannot police it. Two
runs of the same pair of revs put it at +13% and at -4%.

The `noise` column in a comparison is the same figure for the series each metric
came from, taken as the larger of the two runs'. It is what makes the flag worth
reading: `concurrent-fanout` routinely shows a wall spread in the hundreds of
percent — its iterations are short and bursty — so its wall and throughput
columns cannot support a conclusion, while its CPU spread stays near 5% and its
`cpu us/operation` can. Read the noise before the delta.

## Listen backlogs bound the concurrency workloads

`connection-limit` opens more connections than the proxy's 1744-permit admission
semaphore allows, which is the one branch of the accept loop no other workload
reaches. Three things make it a measurement rather than a coin flip:

- **It opens every connection before any of them writes.** Driving the work
  concurrently would not exceed the cap, because a connection can finish before
  the next begins.
- **It dials in waves of 64, pausing between them.** The pause is the part that
  is easy to get wrong: a completed `connect` does not mean the proxy accepted,
  because the kernel finishes the handshake from the backlog, so without a pause
  the client runs thousands of connections ahead of the accept loop whatever the
  wave size. Overrun the gateway's 1024-deep listener — the production value,
  which this harness deliberately does not raise — and the handshakes the kernel
  drops surface as flows the gateway appears to have lost:
  `expected 4096 bytes back, got 0`.
- **It leaves 700 of that backlog spare.** The proxy holds a permit for a flow's
  whole life, so an iteration can begin with some still held by the previous one.

The bench upstream's backlog *is* raised, to 4096, because the upstream must never
be the constraint under test.

None of that is checked by arithmetic, because none of it is visible to
arithmetic. `run.sh` reads the kernel's own counter around every bench and fails
the run if it moved:

```
==> dropped handshakes during e72bad7e7: gateway=0 upstream=0
```

That gate is what found each of the three points above; sized at 2400
connections with no pause, a full run dropped ~300 handshakes while reporting
plausible numbers. To check by hand:

```bash
sudo ip netns exec ngb-gw       nstat -az | grep ListenOverflows
sudo ip netns exec ngb-upstream nstat -az | grep ListenOverflows
```

At reduced `--scale` the workload stays below the cap and measures plain fan-out
instead.

## Iterations wait for the gateway to go quiet

Before opening a measurement window the runner polls the gateway's CPU and waits
for four consecutive quiet 25 ms windows, capped at five seconds. A fixed sleep
cannot serve here: retiring one flow and retiring two thousand take different
amounts of time, and the workloads that open thousands are exactly the ones whose
neighbours the leftover teardown would be charged to. With `--gateway-pid`
omitted there is nothing to observe, and it falls back to a flat 250 ms.

## Ephemeral ports bound the churn workloads

Both the client and the gateway dial from a single address, so
`connect-churn`, `tls-sni`, `tls-sni-absent`, and `denied-destination` consume
ephemeral ports against TIME_WAIT's fixed 60-second timer. `topology.sh` widens
the range and enables `tcp_tw_reuse` in both namespaces for that reason.

It matters because the failure is graduated. Under port pressure, connect
latency inflates well before anything errors — a `--iterations 25` run of
`tls-sni` on the default settings showed a wall p50 of 485 ms against a true
29 ms, and flagged a 25% p99 regression that did not exist — and only then does
it fail outright with `Cannot assign requested address (os error 99)`. A
sudden jump in the wall time of a churn workload, or a flag that vanishes when
the iteration count drops, is this and not the gateway.

## Caveats

- Root only: creating namespaces, loading nftables rules, and setting
  `IP_TRANSPARENT` all require it. The gateway runs as root here; production
  runs it as the per-sandbox uid.
- `run.sh` tears the namespaces down on exit, including on failure. Pass
  `--keep-topology` to inspect them afterwards, then `topology.sh down`.
- The namespace and address constants are fixed. Two runs on one box at the same
  time will collide.
- Absolute figures are not production figures; see "What is real" above.
