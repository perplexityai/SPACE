
# Async and Tokio at the SPACE Rust boundary

You are writing Rust in `perplexityai/SPACE`. The runtime is Tokio. The reviewer will reject blocking-the-runtime, bespoke shutdown channels, and `Arc<Mutex<T>>` where the actor pattern fits. This reference is the recipe; the *why* lives in `docs/rust/async-and-tokio.md` and `docs/rust/principles.md` § 4.

## The one rule

**Async code never spends a long time without reaching an `.await`.** Rule of thumb: ≤ 10–100 µs between awaits.

If your code violates this, the Tokio worker thread is stuck and no other task on it makes progress. Symptom: latency cliffs under load, `tokio-console` shows tasks `Idle` while CPU is pegged.

## When to read this reference

- You are inside `async fn` or you are about to spawn one.
- You are designing or modifying a long-running background task / reconciler / poller.
- You see `std::sync::Mutex::lock()` followed (anywhere in the lock guard's lifetime) by `.await`.
- You see `std::fs::*`, `std::thread::sleep`, `ureq`, `reqwest::blocking`, or any sync DB driver inside `async fn`.
- You are adding shutdown handling.
- You are wiring `Arc<Mutex<T>>` to share state across many tasks.

## The recipe

### 1. Don't block the runtime

| You wrote                                  | Replace with                                |
| ------------------------------------------ | ------------------------------------------- |
| `std::fs::read(...)`                       | `tokio::fs::read(...).await`                |
| `std::thread::sleep(d)`                    | `tokio::time::sleep(d).await`               |
| `ureq::get(url).call()`                    | async HTTP client (`reqwest`)               |
| Heavy CPU > ~100 µs                        | `tokio::task::spawn_blocking(move || ...)` |
| Sync DB driver                             | Async equivalent (`sqlx`, `tokio-postgres`) |
| `std::sync::Mutex` held across `.await`    | Drop guard before await, **or** use `tokio::sync::Mutex` (last resort) |

`spawn_blocking` runs the closure on a separate threadpool (default 512 threads). Pass owned data in (`'static + Send`); don't spawn unbounded numbers of long-running blocking tasks.

### 2. `std::sync::Mutex` is the default

Default to `std::sync::Mutex`. It is faster, and its guard is `!Send` — the compiler *refuses* to let you hold it across an `.await`, which is exactly what you want.

Reach for `tokio::sync::Mutex` only when:

- you genuinely must hold the lock across an `.await` (rare; usually a design smell — see actor pattern below), or
- contention is high enough that a true async yield matters.

The pattern that works almost always:

```rust
let value = {
    let guard = state.lock().expect("poisoned");
    compute_with(&*guard)
}; // guard dropped here
do_async_thing(value).await;
```

### 3. Shared state across many tasks → actor pattern

When state is touched by many async callers, prefer **one task that owns the state, accessed via a handle that hides a channel**, over `Arc<Mutex<T>>` plus contention.

```rust
use tokio::sync::{mpsc, oneshot};

// actor
struct CounterActor {
    receiver: mpsc::Receiver<CounterMsg>,
    next_id: u32,
}

enum CounterMsg {
    NextId { respond_to: oneshot::Sender<u32> },
}

async fn run_actor(mut a: CounterActor) {
    while let Some(msg) = a.receiver.recv().await {
        match msg {
            CounterMsg::NextId { respond_to } => {
                a.next_id += 1;
                let _ = respond_to.send(a.next_id);
            }
        }
    }
}

// handle (the only thing the rest of the code sees)
#[derive(Clone)]
pub struct CounterHandle { sender: mpsc::Sender<CounterMsg> }

impl CounterHandle {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(64); // bounded — backpressure!
        tokio::spawn(run_actor(CounterActor { receiver: rx, next_id: 0 }));
        Self { sender: tx }
    }

    pub async fn next_id(&self) -> u32 {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(CounterMsg::NextId { respond_to: tx }).await;
        rx.await.expect("actor task died")
    }
}
```

Rules:

- **Bounded channel.** `mpsc::channel(N)` — never `unbounded_channel` for cross-task communication. `unbounded_channel` removes backpressure and turns every memory leak into an OOM.
- **Handle is `Clone`, actor is not.** Clone handles freely.
- **`run_actor` is a free function**, not a method, so it can be moved into `tokio::spawn` without borrowing.
- **`send().await`, not `try_send`.** A full mailbox should slow down the sender, not silently drop.
- **`recv()` returning `None`** = every handle has been dropped = shutdown signal for the actor.

### 4. One `CancellationToken` per service

The `space-rust-development` entrypoint requires one `CancellationToken` per
service instead of bespoke `oneshot`, `broadcast`, or `AtomicBool` shutdown
channels. Name it `shutdown_token`, use child tokens for sub-scopes, and give
every spawned task a clone.

```rust
use tokio_util::{sync::CancellationToken, task::TaskTracker};

let shutdown_token = CancellationToken::new();
let tracker = TaskTracker::new();

for cfg in worker_configs {
    let token = shutdown_token.child_token();
    tracker.spawn(run_worker(cfg, token));
}

tokio::select! {
    _ = tokio::signal::ctrl_c()    => {}
    _ = shutdown_token.cancelled() => {}
}
shutdown_token.cancel();
tracker.close();
tracker.wait().await;
```

Inside each spawned task:

```rust
async fn run_worker(cfg: Cfg, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            biased;                                  // poll shutdown first
            _ = shutdown.cancelled() => break,
            msg = next_message(&cfg) => handle(msg).await,
        }
    }
    // cleanup *after* cancellation: flush, drain in-flight work, etc.
}
```

### 5. `select!` gotchas

- **Cancellation safety.** Each branch may be dropped mid-flight if a sibling fires. `mpsc::Receiver::recv()` is cancellation-safe; partial HTTP reads usually aren't. If unsure, pin the future *outside* `select!` and re-use it across iterations.
- **`biased;`** — by default branches are selected pseudo-randomly. If you need a priority order (shutdown first, then work), `biased;` is the first line in the macro.
- **No work between `.await`s in a branch** — same "don't block the runtime" rule.

### 6. `spawn` vs `JoinSet` vs `TaskTracker`

| Tool                    | Use when                                                                 |
| ----------------------- | ------------------------------------------------------------------------ |
| `tokio::spawn`          | Fire-and-forget; only when failure or completion truly does not matter   |
| `JoinSet<T>`            | Dynamic set of tasks returning `T`, you need each result                 |
| `TaskTracker`           | Dynamic background tasks; you only need "all done?" — typical for shutdown |
| `tokio::join!`          | Fixed-arity concurrency on one task (no parallelism)                     |
| `futures::join_all`     | Dynamic-arity, fixed-up-front list; collects results in order            |

A `tokio::spawn` with no retained handle dies silently. That is the most common cause of "why did my reconciler stop?".

### 7. Where new long-running work goes (SPACE-specific)

Place long-running work according to its cardinality:

- **Cluster-singleton** background loop → `crates/l40_api/cluster/controller/server/` (the `space-cluster-controller` library). Add a `tokio::spawn(...)` in `run_reconcilers()` plus config under `clusterController.reconcilers.<name>`. **Do not** create a new binary, Dockerfile, ECR repo, or Helm release.
- **Per-node** background work → `crates/l40_api/node/ctl/server/` (the `space-node-ctl` DaemonSet library).
- **Stateless API service** must remain stateless: no background threads, no controller loops in the API process. Persist intent and enqueue.
- **API handlers** are latency-bounded to ~10 s. Anything longer is queued for background work.

## Self-review before opening the PR

- [ ] No `std::fs`, `std::thread::sleep`, sync HTTP/DB, or `std::sync::Mutex` guard held across `.await` inside `async fn`.
- [ ] CPU work > ~100 µs is in `spawn_blocking` (or `rayon`).
- [ ] Every spawned task either (a) is short-lived and the handle is awaited, or (b) receives a `CancellationToken` clone and obeys it.
- [ ] All `mpsc::channel(...)` capacities are bounded; backpressure flows via `send().await`.
- [ ] `select!` branches are cancellation-safe; `biased;` is present when ordering matters.
- [ ] Shared mutable state is owned by an actor (preferred) or behind a `Mutex` whose guard is never held across `.await`.
- [ ] No new top-level binary / Dockerfile / Helm release was created for a reconciler — it's wired into `space-cluster-controller` or the node DaemonSet.
- [ ] If this is an API handler, it returns within ~10 s; long work is queued.

## When you need more

| You're doing this                                | Read                                             |
| ------------------------------------------------ | ------------------------------------------------ |
| Designing a new long-running service             | `docs/rust/async-and-tokio.md`                   |
| Picking between `Mutex`, `RwLock`, actor pattern | `docs/rust/async-and-tokio.md` § actor pattern + `docs/rust/performance.md` § Concurrency |
| Tuning a hot async path                          | `docs/rust/performance.md`                       |
| Designing the error returned from `async fn`     | [errors](errors.md)                              |
| Modeling the values that flow across tasks       | [types](types.md)                                |
| Self-reviewing async code before opening a PR    | [review discipline](review-discipline.md) + `docs/rust/space-review-lessons.md` § 5, § 6 |
