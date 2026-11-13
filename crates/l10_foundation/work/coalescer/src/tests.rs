use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::{JoinError, Role, WorkCoalescer, WorkOutcome};

type TestCoalescer = WorkCoalescer<&'static str, u32, io::Error>;

fn far_deadline() -> Instant {
    Instant::now() + Duration::from_secs(3600)
}

/// Yield enough times on the current-thread runtime that every spawned task
/// has run up to its next await point.
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn owner_runs_work_exactly_once_and_joiners_share_the_value() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());

    let mut callers = Vec::new();
    for _ in 0..32 {
        let coalescer = Arc::clone(&coalescer);
        let runs = Arc::clone(&runs);
        let gate = Arc::clone(&gate);
        callers.push(tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), move || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    gate.notified().await;
                    Ok(7)
                })
                .await
        }));
    }

    // Let every caller register (owner spawned, joiners subscribed) before
    // the run is allowed to publish, so none of them arrive post-publish and
    // start a second run.
    settle().await;
    assert_eq!(coalescer.live_runs(), 1);
    gate.notify_waiters();

    let mut owned = 0;
    let mut joined = 0;
    for caller in callers {
        match caller.await.expect("caller task panicked") {
            Ok(WorkOutcome::Owned(value)) => {
                assert_eq!(value, 7);
                owned += 1;
            }
            Ok(WorkOutcome::Joined(value)) => {
                assert_eq!(value, 7);
                joined += 1;
            }
            other => panic!("expected a shared success, got {other:?}"),
        }
    }
    assert_eq!(owned, 1, "exactly one caller owns the run");
    assert_eq!(joined, 31, "every other caller joins it");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the work ran once");
    assert_eq!(coalescer.live_runs(), 0, "the entry retired after publish");
}

#[tokio::test]
async fn run_survives_owner_cancellation() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());

    let owner = {
        let coalescer = Arc::clone(&coalescer);
        let runs = Arc::clone(&runs);
        let gate = Arc::clone(&gate);
        tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), move || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    gate.notified().await;
                    Ok(7)
                })
                .await
        })
    };
    settle().await;
    assert_eq!(coalescer.live_runs(), 1);

    // Cancel the owner mid-wait. The run is detached, so it must keep
    // driving toward publish for the joiner below.
    owner.abort();
    settle().await;
    assert_eq!(
        coalescer.live_runs(),
        1,
        "cancelling the owner keeps the run live"
    );

    let joiner = {
        let coalescer = Arc::clone(&coalescer);
        tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), || async {
                    panic!("a live run exists; no caller may start a second one")
                })
                .await
        })
    };
    settle().await;
    gate.notify_waiters();

    match joiner.await.expect("joiner task panicked") {
        Ok(WorkOutcome::Joined(value)) => assert_eq!(value, 7),
        other => panic!("expected to join the orphaned run, got {other:?}"),
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(coalescer.live_runs(), 0);
}

#[tokio::test(start_paused = true)]
async fn deadline_abandons_the_wait_but_never_the_run() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let gate = Arc::new(Notify::new());

    let short_deadline = Instant::now() + Duration::from_secs(1);
    let owner_wait = {
        let gate = Arc::clone(&gate);
        coalescer.join_or_start("wake", short_deadline, move || async move {
            gate.notified().await;
            Ok(7)
        })
    };
    let abandoned = owner_wait.await;
    assert!(
        matches!(abandoned, Err(JoinError::DeadlineElapsed)),
        "the owner's wait times out, got {abandoned:?}"
    );
    assert_eq!(
        coalescer.live_runs(),
        1,
        "the run outlives the owner's abandoned wait"
    );

    // A later caller with its own budget still joins the same run.
    let joiner = {
        let coalescer = Arc::clone(&coalescer);
        tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), || async {
                    panic!("a live run exists; no caller may start a second one")
                })
                .await
        })
    };
    settle().await;
    gate.notify_waiters();

    match joiner.await.expect("joiner task panicked") {
        Ok(WorkOutcome::Joined(value)) => assert_eq!(value, 7),
        other => panic!("expected to join the still-live run, got {other:?}"),
    }
    assert_eq!(coalescer.live_runs(), 0);
}

#[tokio::test]
async fn failure_reports_roles_and_frees_the_key_for_a_fresh_run() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());

    let owner = {
        let coalescer = Arc::clone(&coalescer);
        let runs = Arc::clone(&runs);
        let gate = Arc::clone(&gate);
        tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), move || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    gate.notified().await;
                    Err(io::Error::other("restore lease lapsed"))
                })
                .await
        })
    };
    let joiner = {
        let coalescer = Arc::clone(&coalescer);
        tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), || async {
                    panic!("a live run exists; no caller may start a second one")
                })
                .await
        })
    };
    settle().await;
    gate.notify_waiters();

    match owner.await.expect("owner task panicked") {
        Ok(WorkOutcome::Failed { role, error }) => {
            assert_eq!(role, Role::Owner);
            assert_eq!(error.to_string(), "restore lease lapsed");
        }
        other => panic!("expected the owner to see its run fail, got {other:?}"),
    }
    match joiner.await.expect("joiner task panicked") {
        Ok(WorkOutcome::Failed { role, error }) => {
            assert_eq!(role, Role::Joiner);
            assert_eq!(error.to_string(), "restore lease lapsed");
        }
        other => panic!("expected the joiner to share the failure, got {other:?}"),
    }
    assert_eq!(coalescer.live_runs(), 0, "the failed run retired its entry");

    // A retry after the failure re-races: it becomes a fresh owner instead of
    // rejoining the dead run.
    let retry = coalescer
        .join_or_start("wake", far_deadline(), {
            let runs = Arc::clone(&runs);
            move || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(9)
            }
        })
        .await;
    match retry {
        Ok(WorkOutcome::Owned(value)) => assert_eq!(value, 9),
        other => panic!("expected the retry to own a fresh run, got {other:?}"),
    }
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn panicking_run_reports_abandonment_and_frees_the_key() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let gate = Arc::new(Notify::new());

    let owner = {
        let coalescer = Arc::clone(&coalescer);
        let gate = Arc::clone(&gate);
        tokio::spawn(async move {
            coalescer
                .join_or_start("wake", far_deadline(), move || async move {
                    gate.notified().await;
                    panic!("driver died mid-run")
                })
                .await
        })
    };
    settle().await;
    assert_eq!(coalescer.live_runs(), 1);
    gate.notify_waiters();

    let abandoned = owner.await.expect("owner task panicked");
    assert!(
        matches!(abandoned, Err(JoinError::RunAbandoned)),
        "waiters observe the dropped sender as abandonment, got {abandoned:?}"
    );
    assert_eq!(coalescer.live_runs(), 0, "the drop guard retired the entry");

    // The key accepts a fresh run after the wreck.
    let retry = coalescer
        .join_or_start("wake", far_deadline(), || async { Ok(11) })
        .await;
    match retry {
        Ok(WorkOutcome::Owned(value)) => assert_eq!(value, 11),
        other => panic!("expected a fresh owned run, got {other:?}"),
    }
}

/// A spawned run future can be dropped before its first poll (runtime
/// shutdown drops queued tasks unpolled). The entry must still retire, or
/// the key is permanently occupied by a closed channel: every later caller
/// would observe `RunAbandoned`, re-enter, and hit the same dead entry until
/// the process restarts.
#[test]
fn run_dropped_before_first_poll_retires_its_entry_and_frees_the_key() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    runtime.block_on(async {
        let mut join = Box::pin(coalescer.join_or_start("wake", far_deadline(), || async {
            panic!("the run future must never be polled in this test")
        }));
        // Poll the caller exactly once: it inserts the entry, spawns the run,
        // and parks on the watch channel. Control never returns to the
        // scheduler, so the queued run future is never polled.
        std::future::poll_fn(|cx| {
            assert!(join.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    });
    assert_eq!(coalescer.live_runs(), 1, "the entry is live while queued");

    // Shutting the runtime down drops the queued run future without polling
    // it. Retirement must not depend on the run body ever executing.
    drop(runtime);
    assert_eq!(
        coalescer.live_runs(),
        0,
        "a run dropped before its first poll must retire its entry"
    );

    // The key accepts a fresh run instead of serving the dead channel.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let retry = runtime.block_on(async {
        coalescer
            .join_or_start("wake", far_deadline(), || async { Ok(11) })
            .await
    });
    match retry {
        Ok(WorkOutcome::Owned(value)) => assert_eq!(value, 11),
        other => panic!("expected a fresh owned run, got {other:?}"),
    }
}

#[tokio::test]
async fn distinct_keys_run_independently() {
    let coalescer: Arc<TestCoalescer> = Arc::new(WorkCoalescer::new());
    let gate = Arc::new(Notify::new());

    let mut callers = Vec::new();
    for key in ["wake:a", "wake:b"] {
        let coalescer = Arc::clone(&coalescer);
        let gate = Arc::clone(&gate);
        callers.push(tokio::spawn(async move {
            coalescer
                .join_or_start(key, far_deadline(), move || async move {
                    gate.notified().await;
                    Ok(key.len() as u32)
                })
                .await
        }));
    }
    settle().await;
    assert_eq!(coalescer.live_runs(), 2, "unrelated keys never coalesce");
    gate.notify_waiters();

    for caller in callers {
        match caller.await.expect("caller task panicked") {
            Ok(WorkOutcome::Owned(_)) => {}
            other => panic!("each key's sole caller owns its run, got {other:?}"),
        }
    }
    assert_eq!(coalescer.live_runs(), 0);
}

#[tokio::test]
async fn sequential_calls_after_publish_start_fresh_runs() {
    let coalescer: TestCoalescer = WorkCoalescer::new();
    let runs = Arc::new(AtomicUsize::new(0));

    for expected in 1..=3 {
        let outcome = coalescer
            .join_or_start("wake", far_deadline(), {
                let runs = Arc::clone(&runs);
                move || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok(7)
                }
            })
            .await;
        assert!(
            matches!(outcome, Ok(WorkOutcome::Owned(7))),
            "a caller after publish owns a fresh run, got {outcome:?}"
        );
        assert_eq!(runs.load(Ordering::SeqCst), expected);
    }
}

#[derive(Clone, Default)]
struct SpanGraph(Arc<std::sync::Mutex<Vec<SpanNode>>>);

struct SpanNode {
    id: tracing::span::Id,
    name: &'static str,
    parent: Option<tracing::span::Id>,
    links: Vec<tracing::span::Id>,
}

impl<S> tracing_subscriber::Layer<S> for SpanGraph
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent().map(|parent| parent.id()));
        self.0
            .lock()
            .expect("span capture lock poisoned")
            .push(SpanNode {
                id: id.clone(),
                name: attrs.metadata().name(),
                parent,
                links: Vec::new(),
            });
    }

    fn on_follows_from(
        &self,
        id: &tracing::span::Id,
        follows: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut spans = self.0.lock().expect("span capture lock poisoned");
        spans
            .iter_mut()
            .find(|span| span.id == *id)
            .unwrap()
            .links
            .push(follows.clone());
    }
}

#[tokio::test]
async fn detached_run_keeps_parent_after_cancellation_and_links_joiner() {
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;

    let graph = SpanGraph::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(graph.clone()));
    let coalescer = Arc::new(TestCoalescer::new());
    let gate = Arc::new(Notify::new());
    let owner_coalescer = coalescer.clone();
    let owner_gate = gate.clone();
    let owner_span = tracing::info_span!("owner_request");
    let owner_id = owner_span.id().unwrap();
    let owner = tokio::spawn(
        async move {
            owner_coalescer
                .join_or_start("wake", far_deadline(), || async move {
                    owner_gate.notified().await;
                    // Emitted only after the initiating request was cancelled.
                    let _materialize = tracing::info_span!("materialize").entered();
                    Ok(7)
                })
                .await
        }
        .instrument(owner_span),
    );
    settle().await;
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());

    let follower_span = tracing::info_span!("follower_request");
    let follower_id = follower_span.id().unwrap();
    let follower = tokio::spawn(
        async move {
            coalescer
                .join_or_start("wake", far_deadline(), || async {
                    panic!("follower must not run work");
                })
                .await
        }
        .instrument(follower_span),
    );
    settle().await;
    gate.notify_one();
    assert!(matches!(
        follower.await.unwrap().unwrap(),
        WorkOutcome::Joined(7)
    ));

    let spans = graph.0.lock().expect("span capture lock poisoned");
    let run = spans
        .iter()
        .find(|span| span.name == "work_coalescer.run")
        .unwrap();
    assert_eq!(run.parent, Some(owner_id));
    let materialize = spans
        .iter()
        .find(|span| span.name == "materialize")
        .unwrap();
    assert_eq!(materialize.parent.as_ref(), Some(&run.id));
    let follower_wait = spans
        .iter()
        .find(|span| {
            span.name == "work_coalescer.wait" && span.parent.as_ref() == Some(&follower_id)
        })
        .unwrap();
    assert_eq!(follower_wait.links, vec![run.id.clone()]);
}
