use super::{
    ReconcilerRuntimeError, Supervision, TerminatorKind, build_if_enabled, supervise_reconcilers,
};
use std::future::Future;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

type SupervisedSet = tokio::task::JoinSet<Result<(), JoinError>>;

#[test]
fn disabled_reconciler_does_not_initialize_optional_dependencies() {
    let result: Result<Option<()>, &'static str> =
        build_if_enabled(false, || Err("optional dependency was initialized"));

    assert_eq!(result, Ok(None));
}

/// Spawn `fut` as its own task and wrap it into `set` the same way
/// `run_reconcilers` does — preserving the task's join result so a panic is
/// distinguishable from a clean return.
fn spawn_wrapped<F>(set: &mut SupervisedSet, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    set.spawn(tokio::spawn(fut));
}

/// The shutdown token firing is a normal stop even while tasks run.
#[tokio::test]
async fn supervise_returns_shutdown_when_token_cancelled() {
    let mut set = SupervisedSet::new();
    spawn_wrapped(&mut set, std::future::pending::<()>());
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
    let mut set = SupervisedSet::new();
    // Disabled reconciler: signals on `done_tx` as its final statement, then
    // returns. `yield_now()` is not a completion guarantee (the scheduler may
    // repoll the yielding task without running the spawned one to the end), so
    // gate on the signal instead: the receiver below cannot wake until the
    // task has executed the send and returned, so by the time `done_rx`
    // resolves on the current-thread test runtime the task is complete and
    // `join_next` observes it on the first poll.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    spawn_wrapped(&mut set, async move {
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
    let mut set = SupervisedSet::new();
    spawn_wrapped(&mut set, async { panic!("reconciler blew up") });
    let token = CancellationToken::new();
    assert_eq!(
        supervise_reconcilers(&mut set, &token).await,
        Supervision::TaskExited
    );
}

/// An empty set has nothing to supervise — a normal stop once shutdown fires.
#[tokio::test]
async fn supervise_returns_shutdown_when_set_empty() {
    let mut set = SupervisedSet::new();
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        supervise_reconcilers(&mut set, &token).await,
        Supervision::Shutdown
    );
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
