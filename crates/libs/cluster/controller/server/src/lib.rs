//! The SPACE cluster controller: the cluster-singleton reconciler set and the
//! leader election that decides which replica runs it.
//!
//! This crate is the whole service. The `space-cluster-controller` binary is a
//! thin `main` that parses flags, initializes telemetry, and calls [`run`], so
//! a deployment that needs private wiring can supply its own `main` against
//! this library without copying any of the service.

use pplx_space_api::controllers::substrate::{Substrate, SubstrateAdapter};
use pplx_space_api::egress_profiles::{EgressProfiles, EgressProfilesConfigError};
use pplx_space_api::tls_interception_exemptions::{
    TlsInterceptionExemptions, TlsInterceptionExemptionsConfigError,
};
use tokio_util::sync::CancellationToken;

mod admin_audit_retention;
mod data_plane_session_retention;
mod drained_node_terminator;
mod env_config;
mod execution_materialization_backfill;
mod failed_restorable_repair;
mod gone_node_write_off;
mod kube_node_sync;
mod leader_election;
#[cfg(test)]
mod lifecycle_writer_test_support;
mod lost_started_sandboxes;
mod node_drain;
mod node_group_labeler;
mod node_group_mirror;
mod node_service_readiness;
mod observed_projection_gc;
mod orphaned_nodes;
mod orphaned_restore_pod_gc;
mod orphaned_sandboxes;
mod pod;
mod public_operation_dispatcher;
mod public_operation_retention;
mod reconcile_queue_gc;
mod reconcilers;
mod snapshot_retention_sweeper;
mod snapshot_storage_sweeper;
mod started_sandbox_repair;
mod stuck_container_creating_gc;
mod stuck_snapshot_reaper;
mod suspended_execution_reaper;
mod tombstone_retention;
mod transient_lifecycle_sweeper;
mod warm_pool;
mod warm_pool_orphan_sweep;
mod warn_pacer;

/// The service name the controller reports to telemetry and tags its database
/// pools with.
pub const SERVICE_NAME: &str = "space-cluster-controller";

/// The Kubernetes namespace the leadership `Lease` lives in when the downward
/// API does not supply one.
pub const DEFAULT_LEASE_NAMESPACE: &str = "space";

/// Everything [`run`] needs from the process that hosts it. Reconciler and
/// leader-election tuning stays in the environment, read by the modules that
/// own it, so a private `main` only has to supply what the binary's flags do.
#[derive(Debug, Clone)]
pub struct ClusterControllerConfig {
    /// PostgreSQL connection string for the `space_user` and `space_creds` pools.
    pub database_url: String,
    /// Namespace of the leadership `Lease`; this pod's own namespace, where its
    /// RBAC is granted.
    pub lease_namespace: String,
}

/// Why [`run`] returned without a clean shutdown. Every variant is a reason for
/// the process to exit non-zero so Kubernetes restarts it, except that the
/// caller decides what a non-zero exit means for its own supervisor.
#[derive(Debug, thiserror::Error)]
pub enum RunClusterControllerError {
    #[error("invalid egress profiles: {0}")]
    EgressProfiles(#[from] EgressProfilesConfigError),
    #[error("invalid TLS interception exemptions: {0}")]
    TlsInterceptionExemptions(#[from] TlsInterceptionExemptionsConfigError),
    #[error("failed to initialize the cluster-controller space_user pool: {0}")]
    SpaceUserPool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("failed to initialize the cluster-controller space_creds pool: {0}")]
    CredentialsPool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("substrate init failed: {0}")]
    Substrate(#[source] pplx_space_api::controllers::substrate::SubstrateError),
    #[error("failed to build the leadership lease client: {0}")]
    LeaseClient(#[source] kube::Error),
    /// The leadership lease could not be reached for longer than the liveness
    /// budget. Retrying in-process would not recover a wedged client.
    #[error("{SERVICE_NAME} leadership lease unreachable; exiting to restart")]
    LeaseUnreachable,
    /// A reconciler task exited or panicked before shutdown was requested.
    #[error("{SERVICE_NAME} reconcilers failed: {0}")]
    Reconcilers(#[source] reconcilers::ReconcilerRuntimeError),
    /// The reconcilers stopped without a process shutdown: the lease was lost
    /// mid-flight. Exiting lets the replica re-contend as a standby instead of
    /// idling as a leaderless, reconciler-less process.
    #[error("{SERVICE_NAME} lost its leadership lease; exiting to re-contend")]
    LeadershipLost,
}

/// Run the cluster controller until `shutdown_token` fires or a fatal
/// condition ends it. Telemetry must already be initialized by the caller.
pub async fn run(
    config: ClusterControllerConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunClusterControllerError> {
    // Validate on standbys too, before leader election can mark a replica Ready.
    let tls_interception_exemptions = TlsInterceptionExemptions::from_env()?;
    let egress_profiles = EgressProfiles::from_env()?;
    let pool = pplx_space_common_svc::sql::connect_space_user_pool_lazy(
        &config.database_url,
        SERVICE_NAME,
    )
    .map_err(RunClusterControllerError::SpaceUserPool)?;
    let credentials_pool = pplx_space_common_svc::sql::CredentialsPool::connect_lazy(
        &config.database_url,
        SERVICE_NAME,
    )
    .map_err(RunClusterControllerError::CredentialsPool)?;
    tracing::info!("prepared lazy space_user and space_creds cluster-controller database pools");

    // Initialize the substrate up front, in every replica — leader and standby
    // alike — *before* contending for leadership. This is what makes the
    // readiness gate meaningful under RollingUpdate: a pod that can't build the
    // substrate exits here, before it ever holds the lease or signals Ready, so
    // maxUnavailable:0 stalls the rollout with the working pods intact rather
    // than replacing them with pods that can't run the reconcilers. Standbys
    // keep the reflector caches warm, so winning the lease doesn't pay a cold
    // start. The substrate is read-only infrastructure (the API server already
    // runs one per replica), so multiple instances are safe.
    tracing::info!("{SERVICE_NAME} initializing substrate");
    let substrate = Substrate::from_env(&pool)
        .await
        .map_err(RunClusterControllerError::Substrate)?;

    // Contend for cluster-controller leadership when the process runs in a
    // Kubernetes pod, where an apiserver hosts the `Lease` and a Deployment may
    // run several replicas, whichever substrate places the sandboxes, or when
    // the substrate drives the Kubernetes API, whose client reaches that
    // apiserver from anywhere (`LeaderElectionConfig::election` owns the rule).
    // Only the lease holder runs the reconciler set: two concurrent runners
    // would double-drive every dispatcher, sweeper and CAS loop. A Direct
    // process outside a pod runs the reconcilers unconditionally, which
    // assumes it is the only instance; likewise the kill-switch (which then
    // assumes a single replica).
    let le_config = leader_election::LeaderElectionConfig::from_env();
    // Bound out here, not inside the branch that creates it: the readiness
    // marker's writer task lives only as long as a handle does, and a
    // branch-scoped binding would stop it the moment the branch ended.
    // Underscore-prefixed rather than `let _ =`, which would drop it
    // immediately.
    let mut _readiness = None;
    let authority: leader_election::ReconcilerAuthority<leader_election::KubeLeaseStore> =
        match le_config.election(
            leader_election::ProcessHost::from_env(),
            substrate.capabilities(),
        ) {
            leader_election::Election::Contend => {
                // Renewals and standby polls are the idle steady state, so the
                // lease gets a client that records spans only for failures.
                // The renewal in flight at shutdown is abandoned, not failed.
                let lease_client =
                    pplx_space_common_svc::kube_client::failure_traced_kube_client_from_env(
                        shutdown_token.clone(),
                    )
                    .await
                    .map_err(RunClusterControllerError::LeaseClient)?;
                let store = std::sync::Arc::new(leader_election::KubeLeaseStore::new(
                    lease_client,
                    &config.lease_namespace,
                ));
                tracing::info!(
                    identity = %le_config.identity,
                    liveness_budget_secs = le_config
                        .lease_failure_grace
                        .map(|g| g.get().as_secs_f64()),
                    "{SERVICE_NAME} contending for leadership"
                );
                // `resolve` owns the outcome-to-action mapping so it can be
                // tested; the arms below only carry it out. The liveness exit
                // must be non-zero: retrying in-process won't recover a wedged
                // client, and a NotReady-forever standby that nothing restarts
                // is the gap this closes. The error log that precedes it already
                // carries the identity, the measured window and the budget, so
                // this only has to be the exit.
                match leader_election::campaign(store, le_config, shutdown_token.clone())
                    .await
                    .resolve()
                {
                    leader_election::CampaignAction::ExitZero => {
                        tracing::info!(
                            "{SERVICE_NAME} asked to shut down before winning leadership"
                        );
                        return Ok(());
                    }
                    leader_election::CampaignAction::ExitNonZero => {
                        return Err(RunClusterControllerError::LeaseUnreachable);
                    }
                    leader_election::CampaignAction::Lead(leadership) => {
                        tracing::info!("{SERVICE_NAME} elected leader; starting reconcilers");
                        leader_election::ReconcilerAuthority::Leader(leadership)
                    }
                }
            }
            leader_election::Election::Skip(reason) => {
                tracing::info!(
                    %reason,
                    "{SERVICE_NAME} running reconcilers without leader election ({reason}); \
                     this assumes a single replica"
                );
                _readiness = Some(leader_election::signal_ready(&le_config));
                leader_election::ReconcilerAuthority::ElectionDisabled
            }
        };

    // Drive the reconcilers off the leadership token (cancelled on process
    // shutdown *and* on lease loss, so they stop before any standby takes over)
    // or, where election doesn't apply, off the process shutdown token directly.
    let reconciler_token = authority.reconciler_token(&shutdown_token);
    let leadership = authority.into_leadership();
    let result = reconcilers::run_reconcilers(
        pool,
        credentials_pool,
        substrate,
        tls_interception_exemptions,
        egress_profiles,
        reconciler_token,
    )
    .await;

    // Only *release* the lease (fast standby takeover) when the reconcilers
    // drained cleanly — otherwise a wedged tick may still be detached and
    // running, so the lease is abandoned and left to expire at its TTL, by which
    // point this process has exited and that task is dead. A reconciler panic is
    // treated the same way (conservative). `relinquish` owns that choice so it
    // can be tested; this only reports which outcome happened.
    if let Some(leadership) = leadership {
        let drained_cleanly = matches!(&result, Ok(reconcilers::ShutdownOutcome::Drained));
        leadership.relinquish(drained_cleanly).await;
    }

    // A reconciler panic is fatal — exit non-zero so Kubernetes restarts us.
    result.map_err(RunClusterControllerError::Reconcilers)?;

    // Distinguish a clean shutdown from a lost-leadership stop: if the
    // reconcilers returned without a global shutdown, we lost the lease
    // mid-flight.
    if shutdown_token.is_cancelled() {
        tracing::info!("{SERVICE_NAME} shut down");
        Ok(())
    } else {
        Err(RunClusterControllerError::LeadershipLost)
    }
}
