use super::{
    ReconcilerRuntimeError, ReconcilerTasks, SHUTDOWN_DRAIN_TIMEOUT, ShutdownOutcome, Supervision,
    TerminatorKind, build_if_enabled, spawn_reconciler_child, supervise_reconcilers,
};
use std::future::pending;
use tokio::{sync::oneshot, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

#[test]
fn disabled_reconciler_does_not_initialize_optional_dependencies() {
    let result: Result<Option<()>, &'static str> =
        build_if_enabled(false, || Err("optional dependency was initialized"));

    assert_eq!(result, Ok(None));
}

/// The shutdown token firing is a normal stop even while tasks run.
#[tokio::test]
async fn supervise_returns_shutdown_when_token_cancelled() {
    let mut set = JoinSet::new();
    set.spawn(pending());
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        supervise_reconcilers(&mut set, &token).await,
        Supervision::Shutdown
    );
}

/// A clean return (how a disabled reconciler exits) is NOT fatal: it's
/// drained and supervision keeps watching, so it stays pending (not
/// `TaskExited`) until shutdown. Deterministic: the clean task is driven to
/// completion first, then a single poll must be `Pending` — if the clean
/// return regressed to fatal, that poll would resolve to `TaskExited`.
#[tokio::test]
async fn supervise_ignores_clean_task_return() {
    let mut set = JoinSet::new();
    // Disabled reconciler: signals on `done_tx` as its final statement, then
    // returns. `yield_now()` is not a completion guarantee (the scheduler may
    // repoll the yielding task without running the spawned one to the end), so
    // gate on the signal instead: the receiver below cannot wake until the
    // task has executed the send and returned, so by the time `done_rx`
    // resolves on the current-thread test runtime the task is complete and
    // `join_next` observes it on the first poll.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    set.spawn(async move {
        let _ = done_tx.send(());
    });
    done_rx
        .await
        .expect("clean task must signal completion before returning");

    let token = CancellationToken::new(); // NOT cancelled yet
    let fut = supervise_reconcilers(&mut set, &token);
    tokio::pin!(fut);
    // The clean return is drained (`continue`) and the (now empty) set leaves
    // supervision awaiting shutdown — pending, not a fatal `TaskExited`.
    assert!(futures_util::poll!(fut.as_mut()).is_pending());
    // Once shutdown fires it resolves to a normal stop.
    token.cancel();
    assert_eq!(fut.await, Supervision::Shutdown);
}

/// A panicking task is fatal.
#[tokio::test]
async fn supervise_returns_task_exited_when_a_task_panics() {
    let mut set = JoinSet::new();
    set.spawn(async { panic!("reconciler blew up") });
    let token = CancellationToken::new();
    assert_eq!(
        supervise_reconcilers(&mut set, &token).await,
        Supervision::TaskExited
    );
}

/// An empty set has nothing to supervise — a normal stop once shutdown fires.
#[tokio::test]
async fn supervise_returns_shutdown_when_set_empty() {
    let mut set = JoinSet::new();
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        supervise_reconcilers(&mut set, &token).await,
        Supervision::Shutdown
    );
}

#[tokio::test(start_paused = true)]
async fn reconciler_and_its_child_stop_before_release_after_the_drain_deadline() {
    let mut tasks = ReconcilerTasks::default();
    let (started, ready) = oneshot::channel();
    let (parent, mut parent_stopped) = oneshot::channel::<()>();
    let (child, mut child_stopped) = oneshot::channel::<()>();
    tasks.spawn(async move {
        let mut children = JoinSet::new();
        spawn_reconciler_child(&mut children, async move {
            started.send(()).unwrap();
            pending::<()>().await;
            drop(child);
        });
        // Neither task observes the cooperative shutdown token.
        pending::<()>().await;
        drop((children, parent));
    });
    ready.await.unwrap();

    let shutdown_at = Instant::now();
    assert_eq!(tasks.stop().await, ShutdownOutcome::Stopped);
    assert!(shutdown_at.elapsed() >= SHUTDOWN_DRAIN_TIMEOUT);
    assert!(matches!(
        parent_stopped.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    assert!(matches!(
        child_stopped.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
}

#[test]
fn terminator_kind_accepts_canonical_names_and_defaults_to_aws() {
    assert_eq!(TerminatorKind::parse(None).unwrap(), TerminatorKind::Aws);
    assert_eq!(
        TerminatorKind::parse(Some("")).unwrap(),
        TerminatorKind::Aws
    );
    assert_eq!(
        TerminatorKind::parse(Some(" aws ")).unwrap(),
        TerminatorKind::Aws
    );
    assert_eq!(
        TerminatorKind::parse(Some("kube-delete")).unwrap(),
        TerminatorKind::KubeDelete
    );
    assert_eq!(
        TerminatorKind::parse(Some("noop")).unwrap(),
        TerminatorKind::Noop
    );
}

/// A typo must not silently select real instance termination.
#[test]
fn terminator_kind_rejects_unrecognized_value() {
    let err = TerminatorKind::parse(Some("kube_delete")).unwrap_err();
    assert!(matches!(
        err,
        ReconcilerRuntimeError::InvalidTerminatorKind(ref value) if value == "kube_delete"
    ));
}
