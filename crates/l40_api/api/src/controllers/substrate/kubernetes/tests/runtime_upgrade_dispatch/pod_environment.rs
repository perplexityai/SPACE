//! The environment a runtime upgrade's pod records and, while the
//! environment placement gate is on, requires a node label for: the
//! upgrade's target, which the pod boots, never the environment the sandbox
//! last ran under, which the upgrade moves off.

use super::super::environment_placement::created_pod_environment;
use super::super::runtime_upgrade_fixture::worker_node;
use super::*;
use crate::controllers::substrate::environment_placement::EnvironmentPlacementGate;
use pplx_space_sandbox_pod::{ENVIRONMENT_USABLE_LABEL_VALUE, environment_label_key};

#[derive(Clone, Copy, Debug)]
enum Activation {
    /// A user resumes a suspended sandbox whose checkpoint E0 produced.
    Resume,
    /// A user starts a stopped sandbox that last ran under E0.
    Start,
    /// [`Self::Start`] with E0 deprecated rather than blocked.
    StartDeprecated,
    /// [`Self::Resume`] with E0's catalog row certifying a memory edge to E1,
    /// which restores the checkpoint under E1 rather than upgrading it.
    ResumeByMemoryEdge,
    /// The stopped sandbox of [`Self::Start`] is upgraded without being
    /// started (`upgrade_stopped_runtime`).
    StayStopped,
    /// [`Self::StayStopped`] with E0 deprecated rather than blocked.
    StayStoppedDeprecated,
}

/// The node the fixture places every pod on, publishing `environments`.
fn worker_node_publishing(environments: &[&str]) -> k8s_openapi::api::core::v1::Node {
    let mut node = worker_node();
    let labels = node.metadata.labels.get_or_insert_with(Default::default);
    for environment in environments {
        labels.insert(
            environment_label_key(&environment.parse().unwrap()),
            ENVIRONMENT_USABLE_LABEL_VALUE.to_string(),
        );
    }
    node
}

/// What [`activate`] left: its answer, the database it ran against and
/// the checkpoint annotation and environment of every pod it created.
struct Activated {
    result: Result<(), SubstrateError>,
    db: pplx_space_db_test_fixtures::TestDb,
    sandbox_id: SandboxId,
    checkpoints: Vec<Option<String>>,
    environments: Vec<(Option<String>, bool)>,
}

/// Run `activation` off E0 (blocked, or deprecated for the `*Deprecated`
/// activations) to the cluster default E1 under `gate`,
/// with the node publishing `node_publishes`.
async fn activate(
    activation: Activation,
    gate: EnvironmentPlacementGate,
    node_publishes: &[&str],
) -> Result<Activated, BoxError> {
    let (db, sandbox_id, cluster) = match activation {
        Activation::Resume | Activation::ResumeByMemoryEdge => {
            let db = pplx_space_db_test_fixtures::pg_test_db().await?;
            let sandbox_id = SandboxId::new();
            let (_, cluster) = user_restore_subject(&db.pool, sandbox_id).await?;
            block_e0_for_e1(&db.pool, &[E1]).await?;
            if matches!(activation, Activation::ResumeByMemoryEdge) {
                sqlx::query(sql!(
                    "UPDATE runtime_environments SET memory_edges = ARRAY[$2::text]
                      WHERE environment_ref = $1"
                ))
                .bind(E0)
                .bind(E1)
                .execute(&db.pool)
                .await?;
            }
            (db, sandbox_id, cluster)
        }
        Activation::Start
        | Activation::StartDeprecated
        | Activation::StayStopped
        | Activation::StayStoppedDeprecated => {
            let deprecated = matches!(
                activation,
                Activation::StartDeprecated | Activation::StayStoppedDeprecated
            );
            let (db, sandbox_id, _, cluster) = start_subject("stopped", deprecated).await?;
            (db, sandbox_id, cluster)
        }
    };
    // The subjects' own substrates cache a node that publishes nothing.
    let mut substrate =
        cluster.substrate_with_nodes(&db.pool, vec![worker_node_publishing(node_publishes)]);
    substrate.k8s.runtime_upgrade_at_activation = RuntimeUpgradeAtActivation::Enabled;
    substrate.k8s.environment_placement = gate;
    let result = match activation {
        Activation::Resume | Activation::ResumeByMemoryEdge => {
            restore(&substrate, sandbox_id).await.map(drop)
        }
        Activation::Start | Activation::StartDeprecated => {
            start(&substrate, sandbox_id).await.map(drop)
        }
        // Nothing publishes the sandbox stopped under the target, so a placed
        // upgrade runs to its deadline; only its pod is read, so that's short.
        Activation::StayStopped | Activation::StayStoppedDeprecated => substrate
            .upgrade_stopped_runtime(sandbox_id, Duration::from_millis(500))
            .await
            .map(drop),
    };
    let environments = cluster
        .pods
        .created
        .lock()
        .unwrap()
        .iter()
        .map(created_pod_environment)
        .collect();
    Ok(Activated {
        result,
        db,
        sandbox_id,
        checkpoints: cluster
            .pods
            .created_annotations(SANDBOX_CHECKPOINT_ANNOTATION),
        environments,
    })
}

/// The pod renders the upgrade's target, E1, both where shadow records it
/// and where on requires it. A node that publishes only the target takes the
/// pod: the environment the sandbox moves off needn't be usable there.
#[rstest::rstest]
#[case::resume_shadow(Activation::Resume, EnvironmentPlacementGate::Shadow)]
#[case::resume_on(Activation::Resume, EnvironmentPlacementGate::On)]
#[case::start_shadow(Activation::Start, EnvironmentPlacementGate::Shadow)]
#[case::start_on(Activation::Start, EnvironmentPlacementGate::On)]
#[case::stay_stopped_shadow(Activation::StayStopped, EnvironmentPlacementGate::Shadow)]
#[case::stay_stopped_on(Activation::StayStopped, EnvironmentPlacementGate::On)]
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_pod_renders_the_upgrades_target_environment(
    #[case] activation: Activation,
    #[case] gate: EnvironmentPlacementGate,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let activated = activate(activation, gate, &[E1]).await?;

    let result = &activated.result;
    assert!(!activated.checkpoints.is_empty(), "no pod: {result:?}");
    assert!(
        activated
            .checkpoints
            .iter()
            .all(|checkpoint| checkpoint.as_deref() == Some(RUNTIME_UPGRADE_CHECKPOINT)),
        "{activation:?} should boot by the upgrade: {:?}",
        activated.checkpoints
    );
    assert!(
        activated
            .environments
            .iter()
            .all(|environment| *environment == (Some(E1.to_string()), gate.refuses())),
        "{activation:?} under {gate}: {:?}",
        activated.environments
    );
    Ok(())
}

/// A pod pinned to the node that holds the sandbox's disk or checkpoint skips
/// kube-scheduler, so with the gate on the API checks that node's labels for
/// the upgrade's target before any pod exists: a node that publishes only the
/// environment the sandbox moves off can't boot the upgrade. A start or a
/// resume is refused as a conflict when its upgrade is decided, before it
/// claims the execution, so the sandbox is back where it was. An upgrade without
/// a start checks the node with its other disk-node checks, before it claims
/// the execution or records the upgrade, and answers a retryable 503.
#[rstest::rstest]
#[case::resume(Activation::Resume)]
#[case::start(Activation::Start)]
#[case::stay_stopped(Activation::StayStopped)]
// A deprecated environment's upgrade without a start still upgrades, so its
// node is checked as a blocked one's is; only a user's start falls back.
#[case::stay_stopped_deprecated(Activation::StayStoppedDeprecated)]
#[tokio::test(flavor = "current_thread")]
async fn a_pinned_upgrade_is_refused_before_its_pod_where_the_node_lacks_the_target(
    #[case] activation: Activation,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let activated = activate(activation, EnvironmentPlacementGate::On, &[E0]).await?;

    let error = activated
        .result
        .as_ref()
        .expect_err("the node doesn't publish the upgrade's target");
    match activation {
        Activation::Resume | Activation::Start => {
            assert!(
                matches!(error, SubstrateError::Conflict { message } if message.contains(E1)),
                "{error:?}"
            );
            // The refusal releases the activation: no claim is left live and
            // the sandbox is back where the user can retry it.
            assert_released(&activated, activation).await?;
        }
        Activation::StartDeprecated | Activation::ResumeByMemoryEdge => {
            unreachable!("not a case of this test")
        }
        Activation::StayStopped | Activation::StayStoppedDeprecated => {
            assert!(
                matches!(error, SubstrateError::Unavailable { message } if message.contains(E1)),
                "{error:?}"
            );
            let recorded: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1
                    AND (runtime_upgrade_operation_id IS NOT NULL
                         OR activation_attempt_id IS NOT NULL)"
            ))
            .bind(activated.sandbox_id)
            .fetch_one(&activated.db.pool)
            .await?;
            assert_eq!(recorded, 0, "nothing is claimed or recorded");
        }
    }
    assert!(
        activated.checkpoints.is_empty(),
        "{:?}",
        activated.checkpoints
    );
    Ok(())
}

/// The sandbox `activated` refused is back in the lifecycle `activation`
/// found it in, on the execution it had, with nothing recorded. A refused
/// start also leaves no live activation claim. A refused resume's attempt
/// stays on the old execution until its lease ends, as after any refusal
/// past the claim on that path, so its claim isn't asserted.
async fn assert_released(activated: &Activated, activation: Activation) -> Result<(), BoxError> {
    let pool = &activated.db.pool;
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(activated.sandbox_id)
            .fetch_one(pool)
            .await?;
    let (expected, resumes) = match activation {
        Activation::Resume | Activation::ResumeByMemoryEdge => (SandboxLifecycle::Suspended, true),
        Activation::Start
        | Activation::StartDeprecated
        | Activation::StayStopped
        | Activation::StayStoppedDeprecated => (SandboxLifecycle::Stopped, false),
    };
    assert_eq!(lifecycle, expected.as_str(), "{activation:?}");
    let (active, recorded, live_claims): (i64, i64, i64) = sqlx::query_as(sql!(
        "SELECT COUNT(*) FILTER (WHERE active),
                COUNT(*) FILTER (WHERE runtime_upgrade_operation_id IS NOT NULL),
                COUNT(*) FILTER (WHERE active
                                   AND activation_attempt_id IS NOT NULL
                                   AND activation_attempt_expires_at > NOW())
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1"
    ))
    .bind(activated.sandbox_id)
    .fetch_one(pool)
    .await?;
    assert!(
        active <= 1,
        "{activation:?} left {active} active executions"
    );
    assert_eq!(recorded, 0, "{activation:?} recorded an upgrade");
    if !resumes {
        assert_eq!(live_claims, 0, "{activation:?} left a live claim");
    }
    Ok(())
}

/// A deprecated environment still executes, so with the gate on a user's
/// start of a sandbox whose disk node doesn't publish the default starts
/// under the deprecated environment, as recorded, rather than being refused
/// for the upgrade it needn't make. A node that publishes the default still
/// takes the upgrade, and so does any node under `shadow`, which only records
/// the target.
#[rstest::rstest]
#[case::node_lacks_the_default(EnvironmentPlacementGate::On, &[E0], false)]
#[case::node_publishes_the_default(EnvironmentPlacementGate::On, &[E0, E1], true)]
#[case::shadow(EnvironmentPlacementGate::Shadow, &[E0], true)]
#[tokio::test(flavor = "current_thread")]
async fn a_deprecated_start_boots_as_recorded_where_its_node_lacks_the_default(
    #[case] gate: EnvironmentPlacementGate,
    #[case] node_publishes: &[&str],
    #[case] upgrades: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let activated = activate(Activation::StartDeprecated, gate, node_publishes).await?;

    let result = &activated.result;
    assert!(!activated.checkpoints.is_empty(), "no pod: {result:?}");
    assert!(
        activated
            .checkpoints
            .iter()
            .all(
                |checkpoint| (checkpoint.as_deref() == Some(RUNTIME_UPGRADE_CHECKPOINT))
                    == upgrades
            ),
        "{:?}",
        activated.checkpoints
    );
    let environment = if upgrades { E1 } else { E0 };
    assert!(
        activated
            .environments
            .iter()
            .all(|recorded| *recorded == (Some(environment.to_string()), gate.refuses())),
        "{:?}",
        activated.environments
    );
    Ok(())
}

/// A resume through a memory edge loads its checkpoint under the default,
/// while its pod records the checkpoint's producer. While the gate evaluates
/// it would place and count the pod as an environment it doesn't boot, so
/// the API refuses it before any pod, releasing the resume. With the gate
/// off the resume restores as before.
#[rstest::rstest]
#[case::off(EnvironmentPlacementGate::Off)]
#[case::shadow(EnvironmentPlacementGate::Shadow)]
#[case::on(EnvironmentPlacementGate::On)]
#[tokio::test(flavor = "current_thread")]
async fn a_memory_edge_resume_is_refused_while_the_gate_evaluates(
    #[case] gate: EnvironmentPlacementGate,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let activated = activate(Activation::ResumeByMemoryEdge, gate, &[E0, E1]).await?;

    if gate.evaluates() {
        let error = activated
            .result
            .as_ref()
            .expect_err("refused under the gate");
        assert!(
            matches!(error, SubstrateError::Unsupported { message }
                if message.contains(E0) && message.contains(E1)),
            "{error:?}"
        );
        assert!(
            activated.checkpoints.is_empty(),
            "{:?}",
            activated.checkpoints
        );
        assert_released(&activated, Activation::ResumeByMemoryEdge).await?;
    } else {
        let result = &activated.result;
        assert!(!activated.checkpoints.is_empty(), "no pod: {result:?}");
        assert!(
            activated
                .checkpoints
                .iter()
                .all(|checkpoint| checkpoint.as_deref() != Some(RUNTIME_UPGRADE_CHECKPOINT)),
            "{:?}",
            activated.checkpoints
        );
    }
    Ok(())
}
