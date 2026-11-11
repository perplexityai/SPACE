//! A request's connection to its sandbox's runtime, with what reaching that
//! runtime again takes.

use super::*;

impl SandboxController {
    /// Connect to `sandbox`'s runtime for one request, waking it as the
    /// request's `activity` allows: the drain gate, the activation charge and
    /// the wake run here, in that order. The grant names the runtime and
    /// carries the route token minted over its authoritative identity.
    pub(crate) async fn spaced_runtime_grant(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        metric_identity: SandboxApiMetricIdentity,
        activity: Option<ExecutionActivityKind>,
    ) -> Result<SpacedRuntimeGrant, ControllerError> {
        spaced_runtime_grant(
            &self.substrate,
            self.cluster_admission.as_ref(),
            true, // initial connect → charge the dormant entrypoint wake
            self.route_ed25519_authority.as_deref(),
            pool,
            sandbox,
            metric_identity,
            activity,
        )
        .await
    }
}

/// A connected runtime and what reaching it again takes: the sandbox with
/// its authoritative route identity, and the route token minted over it.
pub(crate) struct SpacedRuntimeGrant {
    pub(crate) client: RuntimeClient,
    pub(crate) sandbox: Sandbox,
    pub(crate) space_api_route_token: Option<SpaceApiRouteEd25519Token>,
}

/// [`connect_spaced_runtime_client`], then the sandbox's authoritative route
/// identity, read after the connect so it names the runtime the connect
/// reached, and the bridge route token over it.
#[expect(
    clippy::too_many_arguments,
    reason = "connect_spaced_runtime_client's inputs plus the signing authority"
)]
pub(super) async fn spaced_runtime_grant(
    substrate: &Substrate,
    admission: Option<&std::sync::Arc<crate::cluster_admission::ClusterSandboxAdmission>>,
    charge_entrypoint: bool,
    authority: Option<&SpaceApiRouteEd25519Authority>,
    pool: &PgPool,
    sandbox: &Sandbox,
    metric_identity: SandboxApiMetricIdentity,
    activity: Option<ExecutionActivityKind>,
) -> Result<SpacedRuntimeGrant, ControllerError> {
    let client = connect_spaced_runtime_client(
        substrate,
        admission,
        charge_entrypoint,
        pool,
        sandbox,
        metric_identity,
        activity,
    )
    .await?;
    let sandbox = fetch_authoritative_route_sandbox(pool, sandbox).await?;
    let space_api_route_token = space_api_bridge_route_token(authority, &sandbox)?;
    Ok(SpacedRuntimeGrant {
        client,
        sandbox,
        space_api_route_token,
    })
}
