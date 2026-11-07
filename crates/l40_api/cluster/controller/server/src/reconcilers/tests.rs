use super::{
    ReconcilerRuntimeError, Supervision, TerminatorKind, build_if_enabled,
    dead_node_replaces_the_gone_node_loops, drain_v2_enabled, supervise_reconcilers,
};
use crate::dead_node::{self, AuthorityKind};
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

/// The dead-node verb takes over from the loops it replaces only `live`, and
/// only with EC2 to judge by: a verb that can only answer unknown leaves them
/// running.
#[test]
fn the_dead_node_verb_replaces_the_gone_node_loops_only_live_with_ec2() {
    let mut env = pplx_space_test_env::TestEnv::default();
    for (mode, authority, replaces) in [
        (None, AuthorityKind::Ec2, false),
        (Some("dry-run"), AuthorityKind::Ec2, false),
        (Some("live"), AuthorityKind::None, false),
        (Some("live"), AuthorityKind::Ec2, true),
    ] {
        match mode {
            Some(mode) => env.set(dead_node::MODE_ENV, mode),
            None => env.remove(dead_node::MODE_ENV),
        }
        let config = dead_node::ReconcilerConfig::from_environment(&env);
        assert_eq!(
            dead_node_replaces_the_gone_node_loops(config.as_ref(), authority),
            replaces,
            "{mode:?} {authority:?}"
        );
    }
}

/// The drain's v2 starts only with the dead-node verb replacing the gone-node
/// loops; off, it starts whatever the verb does.
#[test]
fn the_drain_v2_runs_only_with_the_dead_node_verb_live() {
    let mut env = pplx_space_test_env::TestEnv::default();
    for (flag, dead_node_live, expected) in [
        (None, false, Some(false)),
        (Some("false"), false, Some(false)),
        (Some("true"), false, None),
        (Some("true"), true, Some(true)),
    ] {
        match flag {
            Some(flag) => env.set(
                pplx_space_api::routes::admin::nodes::DRAIN_V2_ENABLED_ENV,
                flag,
            ),
            None => env.remove(pplx_space_api::routes::admin::nodes::DRAIN_V2_ENABLED_ENV),
        }
        let got = drain_v2_enabled(&env, dead_node_live);
        match expected {
            Some(enabled) => assert_eq!(got.ok(), Some(enabled), "{flag:?} {dead_node_live}"),
            None => assert!(
                matches!(got, Err(ReconcilerRuntimeError::DrainV2NeedsDeadNodeLive)),
                "{flag:?} {dead_node_live}"
            ),
        }
    }
}
