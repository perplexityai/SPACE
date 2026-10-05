# Performance

A distillation of [The Rust Performance Book](https://nnethercote.github.io/perf-book/) keyed to a long-running networked service like SPACE. Optimize what profiling tells you to optimize, in this order.

---

## Zeroth: actually run a release build

A release build is **10–100×** faster than a dev build. This sounds obvious, but every quarter someone benchmarks a `cargo build` artifact and gets confused.

```
cargo build --release
cargo nextest run --workspace --release   # benchmarks only meaningful in release
```

For our binaries, the release profile in workspace `Cargo.toml` should set:

```toml
[profile.release]
codegen-units = 1
lto = "thin"          # "fat" if benchmarks justify the build-time cost
debug = "line-tables-only"  # symbols for Datadog, no full DWARF
strip = "debuginfo"
```

- `codegen-units = 1` enables more cross-CGU inlining; +5–15% runtime, +30% build time.
- `lto = "thin"` is the sweet spot for whole-program optimization; "fat" rarely beats it for the build-time cost. `lto = false` keeps the default thin-local LTO that release builds already do.
- `debug = "line-tables-only"` keeps stack traces useful in production without bloating the binary.

---

## Profile before you optimize

Tools and what each is good for:

- **`cargo flamegraph`** — sampling profiler, shows where CPU time goes. Start here.
- **`tokio-console`** — live view of Tokio tasks; finds the "task blocked for 800 ms" bug nothing else does.
- **DHAT** (Valgrind) — allocation profiling. Hot allocation sites, exact rates, average lifetimes. Use when `malloc`/`free` show up high in flamegraph.
- **`-Zprint-type-sizes`** (nightly) — when DHAT or flamegraph implicates `memcpy`, this prints the exact layout, padding, and per-variant size of every type.
- **Datadog APM** — production view; complements flamegraphs by showing per-handler latency distributions, not just CPU%.

Two profilers > one. Different tools surface different bottlenecks.

### Measuring a change

- **Measure cost per unit of work, per process.** CPU milliseconds per MiB
  served, or per request, show a saving even when the load generator or the
  network saturates first and throughput stays flat.
- **Alternate A/B runs** (A B A B) and keep an unchanged control in the same
  session. Shared clusters drift by tens of percent over hours; back-to-back
  runs mistake drift for effect.
- **Run past every burst allowance.** A token-bucket rate limiter serves its
  stored burst before its sustained rate, so a short run, or the second of two
  back-to-back runs, can still be spending credit. Run until the rate settles.
- **Validate the measurement itself.** A negative duration, or a rate above a
  hard limit, means the harness is wrong (it sampled the wrong process, or
  started late), not that the code got faster.
- **Measure sizes and threading choices instead of assuming.** Bigger chunks
  or a dedicated blocking thread are not automatically faster; the
  allocation, copying and scheduling they change can cost more than they
  save.

---

## Type sizes

- Types > 128 bytes are copied with `memcpy` instead of inline. If you see `memcpy` in profiles, that is the first knob.
- A surprisingly fat type is usually:
  - an `enum` with one outsized variant (the whole enum is sized by its largest variant — `Box` the rare giant variant),
  - a struct full of `String`/`Vec` where many are usually empty (try `Cow<'_, str>` or `SmallVec`),
  - a `Result<T, E>` where `E` is huge (`Box` the error).

Use `-Zprint-type-sizes` on the type in question:

```
RUSTFLAGS=-Zprint-type-sizes cargo +nightly build --release
```

---

## Allocation hygiene

A heap allocation involves a global lock, allocator data-structure manipulation, and sometimes a syscall. Small allocations are not necessarily cheap. Avoidance hierarchy:

1. **Don't allocate.** `&str` instead of `String`, slices instead of `Vec`, references instead of `Arc`.
2. **Reserve up front.** `Vec::with_capacity(n)`, `String::with_capacity(n)`. The default growth is "quasi-doubling" (0, 4, 8, 16, 32 …); preallocation eliminates the reallocs.
3. **Reuse buffers.** A long-lived `Vec` cleared between iterations beats allocating a new one each iteration. `clear()` keeps capacity.
4. **Use `SmallVec<[T; N]>` / `arrayvec`** when 95% of cases are short and you want stack storage.
5. **Use `Cow<'_, str>`** when most values are borrowed and only edge cases need owning.
6. **`Box` a large variant** of an enum to make the whole enum smaller. Worth it when the giant variant is rare.
7. **`Arc` vs `Rc` vs `Box`.** `Arc::clone` is a single atomic increment, no allocation. `Box::clone` requires `T: Clone` and allocates. If a value is shared widely and immutable, `Arc<T>` is usually right.

DHAT will tell you where allocations are hot. A 10-allocations-per-million-instructions reduction is a measurable speedup in practice.

---

## `HashMap` and `HashSet` hashers

Default `std::collections::HashMap` uses SipHash 1-3, which is HashDoS-safe but slow. For internal maps where HashDoS is not a concern (most of SPACE: keys come from our own ids, not user input):

- **`rustc-hash::FxHashMap` / `FxHashSet`** — fastest for integer keys; used by `rustc` itself. Drop-in replacement.
- **`ahash::AHashMap`** — fast, uses AES instructions where available. Slightly higher quality than Fx.
- **`fnv::FnvHashMap`** — good for very small keys.

Rule:

- **Public API or stores user-controlled keys** → default `HashMap`.
- **Internal cache keyed by `SandboxId` / integer / our own ids** → `FxHashMap`.

Verify with a microbench before changing a hot map; in rustc, switching `fxhash → ahash` actually *regressed* by 1–4%. Hashing performance is workload-dependent.

---

## I/O

- **Buffer.** `BufReader` / `BufWriter` for files and sockets. Default Rust I/O is unbuffered; every small `write!` is a syscall.
- **Lock once.** `println!` locks stdout per call. If you print in a loop, `let mut out = io::stdout().lock();` once.
- **Skip UTF-8 validation** for byte-oriented protocols: `BufRead::read_until` on bytes, not `read_line` into `String`.
- **Async I/O** uses the same buffering story: `tokio::io::BufReader`/`BufWriter`. The `tokio_util::codec::Framed` family handles message-boundary buffering for you.
- **Count requests, not just bytes,** where a device or a rate limiter charges per request. A request may cover only physically contiguous memory, and every `fsync` adds the filesystem's journal commit, so buffer layout and sync frequency can cost more than the data itself.

---

## Data paths: copies, hops and buffers

Bulk bytes that cross several processes pay per byte at every hop. Keep each
hop from copying them, and keep hops that only forward them off the path.

- **Relay payload fields as `Bytes`.** For a protobuf field that carries bulk
  data through several hops, generate it as `bytes::Bytes`
  (`.bytes(".pkg.Message.field")` in `tonic-prost-build`). Each hop then
  decodes the field as a slice of the received buffer instead of a new `Vec`.
- **Read into the buffer you send.** Read into the message's own buffer
  (`Vec::with_capacity` and `read_buf`) and move it into the message, with no
  staging buffer and no second copy.
- **Pass a handle, not the bytes.** A process that only forwards a file or a
  socket can hand the consumer the descriptor (`SCM_RIGHTS`, `pidfd_getfd`)
  instead of relaying its contents. Keep the resource accounted for where it
  was admitted, and end the transfer when that accounting ends.
- **A queue's depth is memory per stream.** Size a per-stream channel to keep
  the pipe busy, a few chunks, not generously: every open stream holds that
  many chunks.

---

## Network transport

- **Set `TCP_NODELAY` on accepted and dialled connections.** With Nagle's
  algorithm on, a small response written in two parts (headers, then body)
  waits for the peer's delayed ACK, tens of milliseconds per request.
- **Size HTTP/2 frames for bulk streams.** The default maximum frame is
  16 KiB, so a 64 KiB message crosses every hop as five DATA frames. The
  receiver advertises the limit: raise it on servers (`Server::max_frame_size`)
  and on clients (`Endpoint::max_frame_size`) of channels that carry bulk
  data.
- **Reuse channels.** Keep one long-lived `tonic::transport::Channel` per peer
  instead of dialling per call. Let concurrent callers share one in-flight
  dial, and do not cache a failed one.

---

## Iterators, branches, common-case fast paths

- Iterators usually optimize to the same machine code as hand-written loops. Don't replace `.iter().sum()` with a manual loop "for performance"; benchmark first.
- For collections that are usually `0`/`1`/`2` elements, a special case is often a win: an `if v.len() == 1` branch beats the generic iterator machinery for the common case. The performance book calls this out explicitly with examples.
- For lookup tables where one value dominates, a compact representation for the common value plus a fallback table for unusual values can be a large win (data compression as a performance technique).

---

## Concurrency

- **`Arc` is not free.** Each clone is an atomic increment; if you're cloning an `Arc` in a tight loop, hoist it out.
- **`std::sync::Mutex` is faster than `tokio::sync::Mutex`** for short critical sections that don't span `.await`. See `async-and-tokio.md`.
- **Reader-writer locks (`RwLock`) are usually slower than `Mutex`** unless you have many simultaneous readers and rare writers. Benchmark; don't reach for `RwLock` reflexively.
- **`parking_lot`** offers faster `Mutex`/`RwLock` implementations than `std`. Use it when contention shows up in profiles.
- **For concurrent maps, reach for `dashmap::DashMap` before `Mutex<HashMap<_, _>>` or `RwLock<HashMap<_, _>>`.** DashMap shards the table internally; lookups and inserts on different keys don't contend. The naive `Mutex<HashMap>` is the wrong default — it serializes every operation on the whole table, even when keys are independent. Use the lock-around-`HashMap` pattern only when you actually need to hold a guard across multiple operations *and* across `.await` points (in which case `tokio::sync::Mutex` plus a deliberate critical section is the right answer).
- **Per-CPU sharding** of state beats a single contended `Mutex` for high-throughput counters / accumulators. `crossbeam`-style.

---

## Async-specific perf notes

- A `tokio::task::yield_now().await` between large CPU chunks is the cheap way to keep the runtime responsive without `spawn_blocking`.
- `tokio::join!(a, b, c)` runs futures concurrently on *one* worker. For parallelism, `tokio::spawn` each and `await` the handles. (`join_all`, `JoinSet`.)
- For fan-out, use `futures::StreamExt::buffer_unordered(N)` to cap parallelism instead of spawning unbounded tasks.

---

## When *not* to optimize

- Cold code. Profile-guided optimization (PGO) is wasted on the 95% of code that isn't hot.
- Tests. Test readability > test speed, until tests are demonstrably slow.
- Anything you haven't measured. "I think this is faster" is the #1 cause of code that is, in fact, slower.

---

## Closing checklist (the perf 30-second review)

- [ ] `--release` build with `codegen-units = 1`, `lto = "thin"`.
- [ ] No allocations in obvious hot loops; `with_capacity`/buffer reuse where applicable.
- [ ] `BufReader`/`BufWriter` around any `File` or socket I/O.
- [ ] Internal hash maps keyed by our own ids use `FxHashMap` (or have a comment justifying default).
- [ ] No `tokio::sync::Mutex` where `std::sync::Mutex` would do; no `RwLock` where `Mutex` would do.
- [ ] Bulk payload fields relayed across processes are `Bytes`, and no hop copies them into a fresh buffer.
- [ ] Accepted and dialled TCP connections set `TCP_NODELAY`; bulk-streaming gRPC channels raise the HTTP/2 maximum frame size.
- [ ] gRPC channels to a peer are reused, not dialled per call.
- [ ] If we changed code "for performance", a benchmark or profile is referenced in the PR description, measured as cost per unit of work with alternated runs.
