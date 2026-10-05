# Async and Tokio

Async Rust is cooperative scheduling. Anything between two `.await`s is a single, uninterruptible piece of work from the runtime's perspective. Most async bugs in this repo follow from forgetting that.

---

## The one rule

> Async code should never spend a long time without reaching an `.await`. — Alice Ryhl

Rule of thumb: **10–100 µs** between awaits. Above that, you are starving every other task on the same runtime worker.

---

## What counts as blocking

| Operation                                               | Blocking?                                  | Fix                                          |
| ------------------------------------------------------- | ------------------------------------------ | -------------------------------------------- |
| `std::fs::*`                                            | Yes                                        | `tokio::fs::*` or `spawn_blocking`            |
| `std::thread::sleep`                                    | Yes                                        | `tokio::time::sleep`                          |
| `reqwest::blocking`, `ureq`                             | Yes                                        | `reqwest` async API                          |
| CPU work > ~100 µs                                      | Yes                                        | `tokio::task::spawn_blocking` or `rayon`     |
| `std::sync::Mutex::lock()` held across `.await`         | **Subtle bug**: not blocking the thread, but holds the lock during a swap, deadlocking other tasks that need it | Drop guard before `.await`, or `tokio::sync::Mutex` |
| `tokio::sync::Mutex::lock().await`                      | No (yields properly)                       | —                                            |
| `serde_json` on a large payload                         | Often yes                                  | `spawn_blocking` for large bodies            |
| Heavy regex / crypto on hot path                        | Often yes                                  | `spawn_blocking`                              |
| Synchronous DB driver                                   | Yes                                        | Async driver (`sqlx`, `tokio-postgres`)       |

---

## `std::sync::Mutex` vs `tokio::sync::Mutex`

Default to **`std::sync::Mutex`**. It is faster and the lock guard is `!Send`, which means the compiler refuses to let you accidentally hold it across an `.await`.

Reach for **`tokio::sync::Mutex`** only when:

- you genuinely *need* to hold the lock across an `.await` (rare; usually a sign of design smell), or
- contention is high enough that you want the wait to be a true async yield, not a thread block.

Most of the time, holding a `std::sync::Mutex` for a quick read-modify-write *between* awaits is the right answer.

---

## Spawning blocking work

```rust
let result = tokio::task::spawn_blocking(move || {
    expensive_cpu_thing(&input)
}).await?;
```

Notes:

- `spawn_blocking` runs on a separate, larger threadpool (default 512 threads). Don't use it for unbounded numbers of long-running tasks; you'll exhaust the pool.
- The closure must be `Send + 'static`. Pass owned data in.
- For interruptible blocking work that we want to cancel, the closure must poll a flag/`AtomicBool` itself — the threadpool cannot cancel arbitrary CPU work.

---

## The actor pattern

When state needs to be shared across many async tasks, prefer **one task that owns the state, accessed via a handle that hides a channel**, over `Arc<Mutex<T>>` plus contention.

```rust
// === actor ===
struct CounterActor {
    receiver: mpsc::Receiver<CounterMsg>,
    next_id: u32,
}

enum CounterMsg {
    NextId { respond_to: oneshot::Sender<u32> },
}

impl CounterActor {
    fn new(rx: mpsc::Receiver<CounterMsg>) -> Self { Self { receiver: rx, next_id: 0 } }

    fn handle(&mut self, msg: CounterMsg) {
        match msg {
            CounterMsg::NextId { respond_to } => {
                self.next_id += 1;
                let _ = respond_to.send(self.next_id); // ignore: caller may have dropped
            }
        }
    }
}

async fn run_actor(mut a: CounterActor) {
    while let Some(msg) = a.receiver.recv().await {
        a.handle(msg);
    }
}

// === handle (the only thing the rest of the code sees) ===
#[derive(Clone)]
pub struct CounterHandle {
    sender: mpsc::Sender<CounterMsg>,
}

impl CounterHandle {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(64); // bounded — backpressure!
        tokio::spawn(run_actor(CounterActor::new(rx)));
        Self { sender: tx }
    }

    pub async fn next_id(&self) -> u32 {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(CounterMsg::NextId { respond_to: tx }).await;
        rx.await.expect("actor task died")
    }
}
```

Five non-obvious rules from Ryhl:

1. **Bound the channel.** `mpsc::channel(64)` — `unbounded_channel` removes backpressure and turns every memory leak into an OOM. Pick a capacity that reflects "how far behind can this actor get before something is wrong".
2. **Handle is `Clone`, actor is not.** The handle is `Arc`-shaped; clone freely.
3. **`run_actor` is a free function**, not a method, so it doesn't borrow the actor and the actor can be moved into `tokio::spawn`.
4. **Backpressure is `send().await`, not `try_send`.** A full mailbox should slow down the sender, not silently drop messages.
5. **The actor's `recv()` returning `None` is the shutdown signal** — happens when every handle has been dropped. Combine with `CancellationToken` for explicit shutdown (below).

> Source: Alice Ryhl, *Actors with Tokio*, https://ryhl.io/blog/actors-with-tokio/.

---

## Graceful shutdown — one token per service

Three parts to every shutdown:

1. **Detect.** `tokio::signal::ctrl_c()` and any service-internal triggers.
2. **Tell.** Cancel a `CancellationToken` that every task is watching.
3. **Wait.** Join the spawned tasks (`JoinSet` / `TaskTracker`) until they exit.

```rust
use tokio_util::{sync::CancellationToken, task::TaskTracker};

let shutdown = CancellationToken::new();
let tracker = TaskTracker::new();

for cfg in worker_configs {
    let token = shutdown.child_token();
    tracker.spawn(run_worker(cfg, token));
}

tokio::select! {
    _ = tokio::signal::ctrl_c() => {}
    _ = shutdown.cancelled()     => {}
}
shutdown.cancel();
tracker.close();
tracker.wait().await;
```

And inside each spawned task:

```rust
async fn run_worker(cfg: Cfg, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            biased;  // poll shutdown first
            _ = shutdown.cancelled() => break,
            msg = next_message(&cfg) => handle(msg).await,
        }
    }
    // any cleanup here — log flushes, in-flight work drain — runs *after* cancellation.
}
```

**This is non-negotiable for SPACE.** As `AGENTS.md` already says: no bespoke `oneshot`/`broadcast`/`AtomicBool` shutdown channels. One `CancellationToken` per service, named `shutdown_token`, child tokens for sub-scopes, every spawned task receives a clone.

> Source: https://tokio.rs/tokio/topics/shutdown.

---

## `select!` gotchas

- **Cancellation safety.** Each branch's future may be dropped mid-flight if a sibling fires. Make sure the branches you wrote tolerate that. Reading from an `mpsc::Receiver` is cancellation-safe; reading partway through an HTTP response usually is not.
- **`biased;`** — by default, `select!` picks a ready branch pseudo-randomly. If you have a priority order (e.g., always service shutdown first), put `biased;` at the top.
- **No work between `.await`s in a `select!` branch**, for the same blocking-the-runtime reason as everywhere else.
- **Inside a loop**, the futures are re-created on each iteration. If a future is expensive to construct (like a stream subscription), pin it outside the loop and `&mut` it in.

---

## `spawn` vs `JoinSet` vs `TaskTracker`

| Tool                    | Use when                                                                 |
| ----------------------- | ------------------------------------------------------------------------ |
| `tokio::spawn`          | Fire-and-forget tasks where you don't need to await completion           |
| `JoinSet<T>`            | A dynamic, growing set of tasks that all return `T`, you want each result |
| `TaskTracker`           | A dynamic set of background tasks where you only need "are they all done?" — typical for graceful shutdown |
| `tokio::join!`          | Fixed-arity, same-task concurrency (no parallelism on single-threaded rt) |
| `futures::join_all`     | Dynamic-arity, fixed-up-front list — collects results in order            |

Avoid `tokio::spawn` without retaining a handle for any task whose failure or completion you care about. "Background task that silently dies" is the #1 source of "why did my service stop reconciling?" incidents.

---

## Bursts, waits and shared connections

- **Never stop reading a stream on a shared HTTP/2 connection to wait for a
  local resource.** HTTP/2 flow control is per connection as well as per
  stream: a handler that stops consuming one stream's frames until a buffer
  or slot frees eventually stalls every other stream on that connection.
  Fall back to a path that needs no wait, or reject.
- **Queue bursts briefly instead of rejecting them at once.** An admission
  limit that fails a request whenever every slot is busy turns a short burst
  into errors the caller can only retry. Let a request take a free slot or
  wait for one alongside a capped number of others, for a capped time, and
  reject only past those bounds. Waiting requests should not be decoded yet,
  so the slots still bound memory.
- **Poll with a short first delay and exponential backoff to a cap.** A long
  fixed first interval adds most of itself to every operation that finishes
  quickly; start at about 100 ms and double up to a cap.

---

## I/O and serialization on the hot path

- **Buffer** all file/network I/O via `BufReader`/`BufWriter` (or `tokio::io::BufReader`/`BufWriter`). Default Rust I/O is unbuffered.
- For text protocols, prefer `tokio_util::codec::Framed` with a codec over hand-rolled read loops.
- For very large JSON or protobuf payloads on a request path, `spawn_blocking` the parse. The default Tokio worker thread is not the place for a 50 ms parse.

> Source: *Rust Performance Book*, I/O chapter.

---

## Closing checklist (the async 60-second review)

- [ ] No `std::fs`, `std::thread::sleep`, `std::sync::Mutex::lock()` held across `.await`, no synchronous IO crates inside `async fn`.
- [ ] CPU work > ~100 µs is in `spawn_blocking` (or `rayon` for parallelism).
- [ ] Every long-running task takes a `CancellationToken` (cloned or child of the service's `shutdown_token`).
- [ ] Channels are bounded; backpressure flows via `send().await`.
- [ ] No handler stops reading a stream on a shared connection to wait for a local resource.
- [ ] Bursty admission waits briefly in a bounded queue before it rejects; polling backs off from a short first interval.
- [ ] `select!` branches are cancellation-safe; `biased;` is used when ordering matters.
- [ ] Shared mutable state is owned by an actor (preferred) or behind a `Mutex` whose guard is never held across an `.await`.
