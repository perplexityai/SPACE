use super::activation_support::{
    IdentityFreeDirectActivationAuthority, internal_request, space_api_route_identity_from_wire,
    space_api_route_identity_to_wire,
};
use super::cleanup_support::{
    ActivationCleanupSpec, CleanupCompletion, CleanupLockTryResult, OwnedCleanupFenceGuard,
};
use super::create_support::*;
use super::network_policy::template_network_policy_from_proto;
use super::network_runtime::{
    NetGatewayRouteIdentityPushOutcome, create_network_policy_authority,
    create_network_policy_generation, network_policy_refresh_error_to_status, push_route_identity,
    push_route_verification_keys, refresh_network_policy, require_local_authority_peer,
    set_network_no_identity,
};
use super::validation::{
    reject_incompatible_template_arch, reject_incompatible_template_spaced_compatibility,
};
use super::*;

pub(super) async fn run_pool_donor_gateway_rollback<IF, IFut, PF, PFut>(
    restore_identity: IF,
    restore_policy: PF,
) -> Result<(), Status>
where
    IF: FnOnce() -> IFut,
    IFut: std::future::Future<Output = Result<(), Status>>,
    PF: FnOnce() -> PFut,
    PFut: std::future::Future<Output = Result<(), Status>>,
{
    let identity_result = restore_identity().await;
    let policy_result = restore_policy().await;
    match (identity_result, policy_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(identity_error), Err(policy_error)) => Err(Status::internal(format!(
            "pool donor gateway rollback failed: identity: {}; policy: {}",
            identity_error.message(),
            policy_error.message()
        ))),
    }
}

pub(super) async fn finish_pool_donor_rollback<RF, RFut>(
    disposition: crate::db::PoolDonorRestoreDisposition,
    hydration_result: Result<(), Status>,
    retire_donor: RF,
) -> Result<(), Status>
where
    RF: FnOnce() -> RFut,
    RFut: std::future::Future<Output = Result<(), Status>>,
{
    match hydration_result {
        Ok(()) => Ok(()),
        Err(hydration_error) if disposition == crate::db::PoolDonorRestoreDisposition::Restored => {
            if let Err(retire_error) = retire_donor().await {
                return Err(pool_adoption_rollback_failure(
                    hydration_error,
                    retire_error,
                ));
            }
            Err(hydration_error)
        }
        Err(hydration_error) => Err(hydration_error),
    }
}

pub(super) fn require_applied_pool_route_identity(
    outcome: NetGatewayRouteIdentityPushOutcome,
) -> Result<u32, Status> {
    match outcome {
        NetGatewayRouteIdentityPushOutcome::Applied {
            identity_profile_version,
        } => Ok(identity_profile_version),
        incompatible => Err(Status::failed_precondition(format!(
            "net-gateway did not apply pool route identity: {incompatible:?}"
        ))),
    }
}

#[derive(Debug)]
pub(super) struct PoolAdoptionFailure {
    status: Status,
    retire_donor: bool,
}

impl PoolAdoptionFailure {
    fn rollback(status: Status) -> Self {
        Self {
            status,
            retire_donor: false,
        }
    }

    fn retire(status: Status) -> Self {
        Self {
            status,
            retire_donor: true,
        }
    }

    #[cfg(test)]
    pub(super) fn code(&self) -> tonic::Code {
        self.status.code()
    }

    pub(super) fn retires_donor(&self) -> bool {
        self.retire_donor
    }
}

/// How many of a build's oldest donors adoption considers. Pools are small,
/// and a node refuses at most a few distinct bindings among them. Past the
/// bound a create boots without the pool, which is slower but correct.
const POOL_DONOR_SCAN_LIMIT: u32 = 32;

/// What the node said about each donor execution this adoption asked about.
#[derive(Default)]
struct DonorAdmissions {
    admitted: Vec<pplx_space_common_core::types::ExecutionMetadata>,
    refused: Vec<(pplx_space_common_core::types::ExecutionMetadata, Status)>,
}

/// The donor adoption may claim, or the node's refusal of every one.
enum DonorChoice {
    Admitted(Box<crate::db::SandboxRow>),
    Refused(Status),
}

/// What adoption did with the donor it claimed.
pub(super) enum PoolAdoption {
    Adopted(Box<Response<AdoptPoolSandboxResponse>>),
    /// No donor could be claimed: the node refused every donor's binding, or
    /// concurrent adoptions took the rest. None was claimed by this call.
    NoUsableDonor(Status),
}

impl From<Status> for PoolAdoptionFailure {
    fn from(status: Status) -> Self {
        Self::rollback(status)
    }
}

pub(super) fn pool_adoption_identity_profile(
    outcome: NetGatewayRouteIdentityPushOutcome,
) -> Result<u32, PoolAdoptionFailure> {
    match outcome {
        NetGatewayRouteIdentityPushOutcome::Applied {
            identity_profile_version,
        } => Ok(identity_profile_version),
        NetGatewayRouteIdentityPushOutcome::Diverged => Err(PoolAdoptionFailure::retire(
            Status::unavailable("pool donor net-gateway route identity diverged; retiring donor"),
        )),
        incompatible => Err(PoolAdoptionFailure::rollback(Status::failed_precondition(
            format!("net-gateway did not apply pool route identity: {incompatible:?}"),
        ))),
    }
}

pub(super) async fn run_pool_adoption_gateway_hydration<IF, IFut, PF, PFut, KF, KFut>(
    push_identity: IF,
    refresh_policy: PF,
    push_verification_keys: KF,
) -> Result<(), PoolAdoptionFailure>
where
    IF: FnOnce() -> IFut,
    IFut: std::future::Future<Output = Result<NetGatewayRouteIdentityPushOutcome, Status>>,
    PF: FnOnce() -> PFut,
    PFut: std::future::Future<Output = Result<(), Status>>,
    KF: FnOnce(u32) -> KFut,
    KFut: std::future::Future<Output = Result<(), Status>>,
{
    let identity_profile_version =
        pool_adoption_identity_profile(push_identity().await.map_err(|status| {
            if status.code() == tonic::Code::Unimplemented {
                PoolAdoptionFailure::retire(status)
            } else {
                PoolAdoptionFailure::rollback(status)
            }
        })?)?;
    refresh_policy()
        .await
        .map_err(PoolAdoptionFailure::rollback)?;
    push_verification_keys(identity_profile_version)
        .await
        .map_err(PoolAdoptionFailure::rollback)?;
    Ok(())
}

impl SandboxManagerService {
    pub(super) async fn adopt_pool_sandbox(
        &self,
        request: Request<AdoptPoolSandboxRequest>,
    ) -> Result<Response<AdoptPoolSandboxResponse>, Status> {
        match self.adopt_pool_sandbox_outcome(request).await? {
            PoolAdoption::Adopted(response) => Ok(*response),
            // Callers treat an exhausted pool as "boot without it".
            PoolAdoption::NoUsableDonor(status) => Err(Status::resource_exhausted(format!(
                "no usable pool sandbox: {}",
                status.message()
            ))),
        }
    }

    /// The oldest donor whose saved binding the node executes. Each distinct
    /// binding is asked about once; a warm pool fill restores through
    /// whichever loader the node selects, so donors of one build can differ.
    /// Admission comes before the claim, so a refused donor is never touched
    /// and stays for a node whose policy admits it again. A node refusal
    /// (`FailedPrecondition`) skips the donor; any other failure to ask, such
    /// as an unreachable node-manager, is returned as is.
    async fn first_admitted_donor(
        &self,
        candidates: Vec<crate::db::SandboxRow>,
        admissions: &mut DonorAdmissions,
    ) -> Result<DonorChoice, Status> {
        for candidate in candidates {
            let Some(execution) = candidate.execution_metadata.as_ref() else {
                return Ok(DonorChoice::Admitted(Box::new(candidate)));
            };
            if admissions.admitted.contains(execution) {
                return Ok(DonorChoice::Admitted(Box::new(candidate)));
            }
            if admissions.refused.iter().any(|(seen, _)| seen == execution) {
                continue;
            }
            match self.admit_exact_execution(execution).await {
                Ok(()) => {
                    admissions.admitted.push(execution.clone());
                    return Ok(DonorChoice::Admitted(Box::new(candidate)));
                }
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    tracing::info!(
                        sandbox_id = %candidate.id,
                        environment = ?execution.environment(),
                        %status,
                        "skipping a pool donor the node does not admit"
                    );
                    admissions.refused.push((execution.clone(), status));
                }
                Err(status) => return Err(status),
            }
        }
        Ok(DonorChoice::Refused(
            admissions
                .refused
                .last()
                .map(|(_, status)| status.clone())
                .unwrap_or_else(|| Status::internal("pool donor scan returned no candidates")),
        ))
    }

    /// Adoption, telling an unusable pool apart so Create can boot without it.
    pub(super) async fn adopt_pool_sandbox_outcome(
        &self,
        request: Request<AdoptPoolSandboxRequest>,
    ) -> Result<PoolAdoption, Status> {
        let peer = self.authorize_peer(&request, "AdoptPoolSandbox", ALLOW_SANDBOX_POOL_ADOPT)?;
        let req = request.get_ref();
        let template = &req.template;
        let owner = &req.owner;
        let sandbox_owner = Self::authorize_sandbox_owner_label(&peer, owner)?;
        let process_env: BTreeMap<String, String> = req.env.clone().into_iter().collect();
        let process_env = sanitize_process_env(process_env, "pool_adopt");
        let requested_policy = req
            .auto_lifecycle
            .as_ref()
            .map(policy_from_proto)
            .transpose()?;
        let requested_auto_lifecycle_authority = if req.auto_lifecycle.is_some() {
            Some(
                crate::db::PolicyAuthority::try_from(req.auto_lifecycle_authority)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        } else {
            None
        };
        let network_policy_override =
            super::network_policy::optional_network_policy_from_proto(req.network_policy.as_ref())?;
        if network_policy_override.is_some() {
            let authority = crate::db::PolicyAuthority::try_from(req.network_policy_authority)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            require_local_authority_peer(peer.principal(), authority)?;
        }
        if let Some(authority) = requested_auto_lifecycle_authority {
            require_local_authority_peer(peer.principal(), authority)?;
        }
        let template_network_policy = template_network_policy_from_proto(
            req.template_network_policy.as_ref(),
            req.template_network_policy_generation,
            req.template_network_policy_authority,
        )?;
        let requested_route_identity =
            space_api_route_identity_from_wire(req.route_identity.as_ref())?;

        if let Some(policy) = template_network_policy.as_ref() {
            self.template_network_policies
                .require_hint(template, policy, network_policy_override.is_none())
                .await?;
        }

        let required_user = if Self::is_template_manager_peer(&peer) {
            Some(&TEMPLATE_MANAGER_SANDBOX_OWNER)
        } else {
            None
        };
        // Selection is only a hint: the claim rechecks the chosen row under
        // SQLite. A concurrent adoption can win that row while admission is
        // awaited, so a lost claim moves on to the next donor; admissions are
        // remembered, so a retry asks the node nothing new.
        let mut admissions = DonorAdmissions::default();
        let mut lost: Vec<String> = Vec::new();
        let (sandbox, _wake) = loop {
            let candidates: Vec<_> = {
                let db = self.db_conn()?;
                crate::db::scan::pool_donors(
                    &db,
                    template,
                    req.content_hash.as_deref(),
                    required_user,
                    POOL_DONOR_SCAN_LIMIT,
                )
                .map_err(|e| Status::internal(format!("db error: {e}")))?
            }
            .into_iter()
            .filter(|candidate| !lost.contains(&candidate.id))
            .collect();
            if candidates.is_empty() {
                // None of this build, or every one taken while this call ran:
                // Create boots without the pool, as for any ineligible donor.
                return Ok(PoolAdoption::NoUsableDonor(if lost.is_empty() {
                    Status::resource_exhausted(format!(
                        "no available pool sandboxes for template '{template}'"
                    ))
                } else {
                    Status::unavailable(
                        "every pool donor was taken by a concurrent adoption or lifecycle operation",
                    )
                }));
            }
            let candidate = match self
                .first_admitted_donor(candidates, &mut admissions)
                .await?
            {
                DonorChoice::Admitted(candidate) => *candidate,
                DonorChoice::Refused(refused) => return Ok(PoolAdoption::NoUsableDonor(refused)),
            };
            // Hold the same canonical fence as cloning through hydration and rollback.
            match self.claim_pool_donor(
                &candidate.id,
                template,
                req.content_hash.as_deref(),
                required_user,
                &sandbox_owner,
                &process_env,
            ) {
                Ok(claimed) => break claimed,
                Err(status) if status.code() == tonic::Code::Unavailable => {
                    tracing::info!(sandbox_id = %candidate.id, %status, "pool donor taken concurrently; trying the next");
                    lost.push(candidate.id);
                }
                Err(status) => return Err(status),
            }
        };
        let id = sandbox.id.clone();
        let adoption_result: Result<Response<AdoptPoolSandboxResponse>, PoolAdoptionFailure> =
            async {
                let mut route_identity = self
                    .resolve_runtime_activation_identity_with_authority(
                        &id,
                        requested_route_identity,
                        false,
                        IdentityFreeDirectActivationAuthority::LocalProducer,
                    )?
                    .identity;
                let sandbox_id = id
                    .parse::<SandboxId>()
                    .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
                route_identity.sandbox_id = Some(sandbox_id);
                RouteStreamEpochIdentity::from_runtime_update(sandbox_id, route_identity.clone())?
                    .ok_or_else(|| {
                        Status::failed_precondition("pool adoption route identity is absent")
                    })?;
                let resolved_template = if req.auto_lifecycle.is_none() {
                    Some(
                        resolve_template_paths(
                            self.template_resolver.as_ref(),
                            pplx_space_template_mgr_client::proto::TemplateRef {
                                name: sandbox.template.clone(),
                                content_hash: sandbox.template_hash.clone(),
                                tag: None,
                            },
                            template_lookup_scope(&sandbox.user, Some(&sandbox.id))?,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                let policy = if let Some(policy) = requested_policy {
                    policy
                } else {
                    resolved_template
                        .as_ref()
                        .ok_or_else(|| Status::internal("pool adoption template defaults are missing"))?
                        .default_auto_lifecycle
                        .clone()
                };
                let auto_lifecycle_authority =
                    if let Some(authority) = requested_auto_lifecycle_authority {
                        authority
                    } else {
                        resolved_template
                            .as_ref()
                            .ok_or_else(|| Status::internal("pool adoption template authority is missing"))?
                            .default_auto_lifecycle_authority
                    };
                validate_v1_policy(&policy).map_err(Status::invalid_argument)?;
                let mut runtime_state = AutoLifecycleRuntimeState::default();
                recompute_runtime_state(&mut runtime_state, &policy);
                let network_policy_authority = create_network_policy_authority(
                    network_policy_override.as_ref(),
                    req.network_policy_authority,
                    sandbox.network_policy_authority,
                )?;
                let network_policy_generation = create_network_policy_generation(
                    network_policy_override.as_ref(),
                    req.network_policy_generation,
                    network_policy_authority,
                );
                let route_identity_changed = sandbox.space_api_route_identity != route_identity;
                self.apply_route_identity_mutation(sandbox_id, "pool_adoption", || {
                    let db = self.db_conn()?;
                    let transaction = crate::db::Transaction::deferred(&db)
                        .map_err(|e| Status::internal(format!("db transaction failed: {e}")))?;
                    crate::db::update_auto_lifecycle(
                        &transaction,
                        &id,
                        &policy,
                        auto_lifecycle_authority,
                    )
                    .map_err(|e| Status::internal(format!("db update_auto_lifecycle: {e}")))?;
                    crate::db::update_auto_lifecycle_state(&transaction, &id, &runtime_state)
                        .map_err(|e| {
                            Status::internal(format!("db update_auto_lifecycle_state: {e}"))
                        })?;
                    crate::db::set_network_policy_fields(
                        &transaction,
                        &id,
                        network_policy_override.as_ref(),
                        network_policy_generation,
                        network_policy_authority,
                    )
                    .map_err(|e| Status::internal(format!("db update_network_policy: {e}")))?;
                    let retired_receipt =
                        crate::db::update_space_api_route_identity_in_transaction(
                        &transaction,
                        sandbox_id,
                        &route_identity,
                        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::for_activation_identity(
                            &route_identity,
                        ),
                    )
                    .map_err(map_runtime_fields_update_error)?;
                    transaction
                        .commit()
                        .map_err(|e| Status::internal(format!("db commit failed: {e}")))?;
                    crate::db::emit_auto_lifecycle_delete_receipt_superseded_if_present(
                        sandbox_id,
                        retired_receipt,
                    );
                    Ok::<_, Status>(((), route_identity_changed))
                })?;

                let adopted = self.lookup_sandbox(&id)?;
                if Self::sandbox_has_live_net_gateway(&adopted) {
                    let effective_policy = self.effective_network_policy(&adopted).await?;
                    let effective_policy_ref =
                        effective_network_policy_ref(&adopted, &effective_policy)?;
                    let route_verification_bundle =
                        self.route_ed25519_verification_bundle.load_full();
                    let route_key_sandbox_id = id.clone();
                    run_pool_adoption_gateway_hydration(
                        || push_route_identity(&id, &adopted.space_api_route_identity),
                        || async {
                            refresh_network_policy(
                                &self.db,
                                &id,
                                Some(&effective_policy_ref),
                                "pool-adoption",
                            )
                            .await
                            .map_err(network_policy_refresh_error_to_status)
                        },
                        move |identity_profile_version| async move {
                            if let Some(bundle) = route_verification_bundle.as_deref() {
                                push_route_verification_keys(
                                    &route_key_sandbox_id,
                                    Some(identity_profile_version),
                                    bundle,
                                )
                                .await?;
                            }
                            Ok(())
                        },
                    )
                    .await?;
                }

                Ok(Response::new(AdoptPoolSandboxResponse {
                    sandbox_id: id.clone(),
                    route_identity: space_api_route_identity_to_wire(
                        &adopted.space_api_route_identity,
                    ),
                }))
            }
            .await;

        match adoption_result {
            Ok(response) => Ok(PoolAdoption::Adopted(Box::new(response))),
            Err(failure) if failure.retires_donor() => {
                let error = failure.status;
                if let Err(retire_error) = self.retire_failed_pool_donor(&id).await {
                    tracing::error!(
                        sandbox_id = %id,
                        original_code = ?error.code(),
                        original_error = %error,
                        retire_error = %retire_error,
                        "pool adoption failed and stale donor retirement was incomplete"
                    );
                    return Err(pool_adoption_rollback_failure(error, retire_error));
                }
                Err(error)
            }
            Err(failure) => {
                let error = failure.status;
                if let Err(rollback_error) = self.rollback_pool_adoption(&id, &sandbox).await {
                    tracing::error!(
                        sandbox_id = %id,
                        original_code = ?error.code(),
                        original_error = %error,
                        rollback_error = %rollback_error,
                        "pool adoption failed and compensation was incomplete"
                    );
                    return Err(pool_adoption_rollback_failure(error, rollback_error));
                }
                Err(error)
            }
        }
    }

    fn claim_pool_donor(
        &self,
        id: &str,
        template: &str,
        content_hash: Option<&str>,
        required_user: Option<&SandboxOwner>,
        owner: &SandboxOwner,
        process_env: &BTreeMap<String, String>,
    ) -> Result<
        (
            crate::db::SandboxRow,
            crate::auto_lifecycle::LifecycleMutexGuard,
        ),
        Status,
    > {
        let wake = self
            .auto_lifecycle
            .wake_lock(id)
            .try_lock()
            .map_err(|_| Status::unavailable("pool donor has an active lifecycle operation"))?;
        let db = self.db_conn()?;
        let donor = crate::db::claim_pool_sandbox(
            &db,
            id,
            template,
            content_hash,
            required_user,
            owner,
            process_env,
        )
        .map_err(|error| Status::internal(format!("claim pool donor: {error}")))?
        .ok_or_else(|| Status::unavailable("pool donor changed before adoption"))?;
        Ok((donor, wake))
    }

    fn lock_pool_clone_donor(
        &self,
        peer: &AuthorizedUdsPeer,
        id: &str,
    ) -> Result<
        (
            crate::db::SandboxRow,
            crate::auto_lifecycle::LifecycleMutexGuard,
            OwnedCleanupFenceGuard,
        ),
        Status,
    > {
        let wake = self
            .auto_lifecycle
            .wake_lock(id)
            .try_lock()
            .map_err(|_| Status::unavailable("pool donor has an active lifecycle operation"))?;
        let CleanupLockTryResult::Acquired(fence) = self.try_cleanup_fence(id) else {
            return Err(Status::unavailable("pool donor cleanup is active"));
        };
        let donor = self.lookup_sandbox(id)?;
        self.authorize_template_manager_sandbox(peer, &donor, "CloneFromDonor")?;
        Ok((donor, wake, fence))
    }

    async fn retire_failed_pool_donor(&self, id: &str) -> Result<(), Status> {
        self.delete(internal_request(DeleteRequest {
            sandbox_id: id.to_string(),
            template_filter: None,
            blocking: false,
        }))
        .await?;
        Ok(())
    }

    async fn rollback_pool_adoption(
        &self,
        id: &str,
        donor: &crate::db::SandboxRow,
    ) -> Result<(), Status> {
        let sandbox_id = id
            .parse::<SandboxId>()
            .map_err(|error| Status::internal(format!("invalid sandbox ID: {error}")))?;
        let disposition =
            self.apply_route_identity_mutation(sandbox_id, "pool_adoption_rollback", || {
                let db = self.db_conn()?;
                let disposition = crate::db::restore_pool_donor_if_adopted(&db, sandbox_id, donor)
                    .map_err(|e| Status::internal(format!("db rollback pool donor: {e}")))?;
                Ok::<_, Status>((
                    disposition,
                    disposition == crate::db::PoolDonorRestoreDisposition::Restored,
                ))
            })?;
        match disposition {
            crate::db::PoolDonorRestoreDisposition::Restored => {}
            crate::db::PoolDonorRestoreDisposition::NotAdopted => tracing::warn!(
                sandbox_id = %id,
                "pool adoption rollback lost its row compare-and-swap; hydrating current gateway state"
            ),
            crate::db::PoolDonorRestoreDisposition::ReceiptOwned => tracing::info!(
                sandbox_id = %id,
                "pool adoption rollback deferred to the pending delete receipt"
            ),
            crate::db::PoolDonorRestoreDisposition::LifecycleOwned => tracing::warn!(
                sandbox_id = %id,
                "pool adoption rollback left the lifecycle-owned sandbox with its current owner"
            ),
        }

        if disposition == crate::db::PoolDonorRestoreDisposition::LifecycleOwned {
            return finish_pool_donor_rollback(disposition, Ok(()), || {
                self.retire_failed_pool_donor(id)
            })
            .await;
        }

        let current = match self.lookup_sandbox(id) {
            Ok(current) => current,
            Err(status) if status.code() == tonic::Code::NotFound => {
                return set_network_no_identity(id, "pool-adoption-rollback-missing-row").await;
            }
            Err(status) => return Err(status),
        };
        let hydration_result = self.hydrate_pool_gateway_to_current_row(id, &current).await;
        finish_pool_donor_rollback(disposition, hydration_result, || {
            self.retire_failed_pool_donor(id)
        })
        .await
    }

    async fn hydrate_pool_gateway_to_current_row(
        &self,
        id: &str,
        current: &crate::db::SandboxRow,
    ) -> Result<(), Status> {
        if !Self::sandbox_has_live_net_gateway(current) {
            return Ok(());
        }
        run_pool_donor_gateway_rollback(
            || async {
                if current.space_api_route_identity.is_absent() {
                    set_network_no_identity(id, "pool-adoption-rollback").await
                } else {
                    push_route_identity(id, &current.space_api_route_identity)
                        .await
                        .and_then(require_applied_pool_route_identity)
                        .map(|_| ())
                }
            },
            || async {
                let effective_policy = self.effective_network_policy(current).await?;
                let effective_policy_ref =
                    effective_network_policy_ref(current, &effective_policy)?;
                refresh_network_policy(
                    &self.db,
                    id,
                    Some(&effective_policy_ref),
                    "pool-adoption-rollback",
                )
                .await
                .map_err(network_policy_refresh_error_to_status)
            },
        )
        .await
    }

    pub(super) async fn wait(
        &self,
        request: Request<WaitRequest>,
    ) -> Result<Response<WaitResponse>, Status> {
        self.authorize_peer(&request, "Wait", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let id = sandbox.id.clone();
        if req.timeout_secs == Some(0) {
            return Err(Status::invalid_argument(
                "timeout_secs must be greater than 0 when set",
            ));
        }
        let timeout = req
            .timeout_secs
            .map(|secs| std::time::Duration::from_secs(secs as u64));

        let start = tokio::time::Instant::now();

        loop {
            // Check current state from DB
            let state = {
                let db = self.db_conn()?;
                crate::db::read::sandbox(&db, &id)
                    .map_err(|e| Status::internal(format!("db error: {e}")))?
                    .map(|row| row.state)
            };

            if let Some(state) = state {
                if state.completes_wait() {
                    return Ok(Response::new(WaitResponse {
                        sandbox_id: id,
                        lifecycle: Some(crate::proto::SandboxLifecycleState::from(state) as i32),
                    }));
                }

                // Check timeout
                if let Some(timeout) = timeout
                    && start.elapsed() >= timeout
                {
                    return Err(Status::deadline_exceeded(format!(
                        "sandbox {id} did not reach terminal state within {}s (current: {})",
                        timeout.as_secs(),
                        state.as_str()
                    )));
                }
            } else {
                return Err(Status::not_found(format!("sandbox {id} no longer exists")));
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    pub(super) async fn clone_from_donor(
        &self,
        request: Request<CloneFromDonorRequest>,
    ) -> Result<Response<CloneFromDonorResponse>, Status> {
        self.clone_from_donor_with_node(request, connect_clone_node(&self.node_manager))
            .await
    }

    async fn clone_from_donor_with_node(
        &self,
        request: Request<CloneFromDonorRequest>,
        connect: impl std::future::Future<
            Output = Result<pplx_space_node_mgr_client::NodeManagerGrpcClient, Status>,
        >,
    ) -> Result<Response<CloneFromDonorResponse>, Status> {
        let peer =
            self.authorize_peer(&request, "CloneFromDonor", ALLOW_SANDBOX_ADMIN_OR_TMPLMGR)?;
        // Bound connect and capability lookup together. Reuse this channel for
        // creation; never cache capability across a node-manager reconnect.
        let mut node_client = tokio::time::timeout(Duration::from_secs(5), async {
            let mut client = connect.await?;
            let capabilities = client
                .get_clone_capabilities(
                    pplx_space_node_mgr_client::proto::CloneCapabilitiesRequest {},
                )
                .await
                .map_err(|error| Status::new(error.code(), "node clone capability lookup failed"))?
                .into_inner();
            if !capabilities.prepared_clone_disposal {
                return Err(Status::unimplemented(
                    "node does not support prepared clone disposal",
                ));
            }
            Ok::<_, Status>(client)
        })
        .await
        .map_err(|_| Status::unavailable("node clone capability lookup timed out"))??;
        let req = request.get_ref();
        let donor_id = &req.donor_sandbox_id;
        let template = &req.template;
        let pool_template = req.pool_template.as_deref().filter(|s| !s.is_empty());

        // Verify donor exists and is paused
        let donor = self.lookup_sandbox(donor_id)?;
        self.authorize_template_manager_sandbox(&peer, &donor, "CloneFromDonor")?;
        let sandbox_owner = Self::authorize_sandbox_owner_label(&peer, &req.owner)?;
        let donor_id = &donor.id;
        let (donor, _wake, _donor_fence) = self.lock_pool_clone_donor(&peer, donor_id)?;
        let template_ref = pool_clone_template_ref(&donor, template)?;

        // Generate new sandbox ID
        let new_id = uuid::Uuid::now_v7().to_string();
        let new_sandbox_id = SandboxId::from_str(&new_id)
            .map_err(|e| Status::internal(format!("generated invalid sandbox id: {e}")))?;
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(new_sandbox_id);

        // Resolve template and compute hash
        let resolved_template = resolve_template_paths(
            self.template_resolver.as_ref(),
            template_ref,
            template_lookup_scope(&donor.user, Some(&donor.id))?,
        )
        .await?;
        reject_incompatible_template_arch(
            template,
            resolved_template
                .content_hash
                .as_deref()
                .unwrap_or("<unhashed>"),
            &resolved_template.arch,
        )?;
        reject_incompatible_template_spaced_compatibility(
            template,
            resolved_template
                .content_hash
                .as_deref()
                .unwrap_or("<unhashed>"),
            resolved_template.spaced_version.as_deref(),
            resolved_template.spaced_compatibility_family.as_deref(),
        )?;
        let template_hash = template_create_hash(
            &resolved_template.template_dir,
            resolved_template.content_hash.as_deref(),
        )?;

        // A clone runs its donor's exact binding, which the serving node must
        // still execute. The saved binding carries the clone's identity for
        // activation, retries and upgrades; its first warm start still selects
        // a consumer from the producer record the clone copies.
        // Admission reuses the clone's channel, with its own 5 second budget
        // like connect and capability lookup, since the donor stays fenced.
        let execution = donor.execution_metadata.as_ref();
        if let Some(execution) = execution {
            tokio::time::timeout(
                Duration::from_secs(5),
                super::environment_selection::admit_exact_with_client(&mut node_client, execution),
            )
            .await
            .map_err(|_| Status::unavailable("pool donor environment admission timed out"))??;
        }

        let cleanup_id = SandboxCleanupEpisodeId::new();
        // Arm disposal before any node resources can be created.
        {
            let db = self.db_conn()?;
            crate::db::insert_sandbox_with_cleanup_episode_and_route_evidence(
                &db,
                crate::db::InsertSandboxParams {
                    guest_image: Default::default(),
                    id: &new_id,
                    uuid8: &uuid8,
                    state: SandboxState::Creating,
                    template,
                    template_hash: Some(&template_hash),
                    user: &sandbox_owner,
                    runtime: &donor.runtime,
                    snapshots_enabled: false,
                    rootfs_size_mb: donor.rootfs_size_mb,
                    disk_io_limits: donor.disk_io_limits,
                    pool_template,
                    // Pool members are always plaintext: encrypted sandboxes
                    // bypass warm pools at the API and cold-create.
                    volume_encryption: false,
                    process_env: &donor.process_env,
                    auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                    network_policy_override: donor.network_policy_override.as_ref(),
                    network_policy_generation: donor.network_policy_generation,
                    network_policy_authority: donor.network_policy_authority,
                    auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                },
                crate::db::CleanupEpisodeSpec {
                    id: cleanup_id,
                    completion: CleanupCompletion::DeletePoolClone,
                    keep_filesystem: false,
                    entered_from_state: None,
                    activation_state: SandboxState::Creating,
                    execution_generation: None,
                    activation_attempt_id: None,
                },
                None,
                None,
                execution,
            )
            .map_err(|e| Status::internal(format!("db insert: {e}")))?;
        }

        self.publish_pool_clone(&new_id, &uuid8, cleanup_id, async {
            if let Some(execution) = &donor.execution_metadata {
                let db = self.db_conn()?;
                crate::db::upsert_sandbox_execution_metadata(&db, &new_id, execution).map_err(
                    |error| Status::internal(format!("persist clone execution: {error}")),
                )?;
            }
            use std::os::unix::fs::MetadataExt;
            let prepared = prepare_clone_root(Path::new(CACHE_DIR), &new_id).map_err(|error| {
                Status::failed_precondition(format!("prepare clone destination: {error}"))
            })?;
            let identity = prepared
                .metadata()
                .map_err(|error| Status::internal(format!("stat clone destination: {error}")))?;
            {
                let db = self
                    .db
                    .lock()
                    .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
                if !crate::db::bind_prepared_clone_root(
                    &db,
                    &new_id,
                    cleanup_id,
                    crate::db::PreparedCloneRoot {
                        device: identity.dev(),
                        inode: identity.ino(),
                    },
                )
                .map_err(|error| {
                    Status::internal(format!("bind clone disposal identity: {error}"))
                })? {
                    return Err(Status::aborted("pool clone disposal was superseded"));
                }
            }
            node_client
                .clone_sandbox_into_prepared_root(
                    pplx_space_node_mgr_client::proto::CloneSandboxRequest {
                        donor_sandbox_id: donor_id.clone(),
                        new_sandbox_id: new_id.clone(),
                        template: template.to_string(),
                        prepared_destination: Some(
                            pplx_space_node_mgr_client::proto::CloneDirectoryIdentity {
                                device: identity.dev(),
                                inode: identity.ino(),
                            },
                        ),
                    },
                )
                .await
                .map_err(pool_clone_rpc_error)?;
            Ok(())
        })
        .await?;

        Ok(Response::new(CloneFromDonorResponse { sandbox_id: new_id }))
    }

    async fn publish_pool_clone(
        &self,
        sandbox_id: &str,
        uuid8: &SandboxRuntimeLabel,
        cleanup_id: SandboxCleanupEpisodeId,
        clone_resources: impl std::future::Future<Output = Result<(), Status>>,
    ) -> Result<(), Status> {
        let mut cleanup = self.activation_cleanup_guard(
            sandbox_id,
            uuid8,
            ActivationCleanupSpec {
                cleanup_id,
                completion: CleanupCompletion::DeletePoolClone,
                context: "cleanup after interrupted pool clone",
                interrupted_reason_code: SandboxQuarantineReasonCode::CreateInterrupted,
                interrupted_reason: "pool clone did not complete",
            },
        )?;

        let CleanupLockTryResult::Acquired(_fence) = self.try_cleanup_fence(sandbox_id) else {
            return Err(cleanup.failure(Status::aborted(
                "pool clone cleanup already owns the sandbox",
            )));
        };
        let current = {
            let db = self.db_conn()?;
            crate::db::read::sandbox_view(&db, sandbox_id)
                .cleanup_episode()
                .strict()
                .map_err(|error| {
                    cleanup.failure(Status::internal(format!(
                        "read pool clone episode: {error}"
                    )))
                })?
        };
        if !current
            .is_some_and(|episode| episode.id == cleanup_id && episode.requested_at.is_none())
            || self.lookup_sandbox(sandbox_id)?.state != SandboxState::Creating
        {
            return Err(cleanup.failure(Status::aborted(
                "pool clone was superseded before preparation",
            )));
        }

        clone_resources
            .await
            .map_err(|error| cleanup.failure(error))?;

        let completed = {
            let db = self.db_conn()?;
            crate::db::complete_pool_clone(&db, sandbox_id, cleanup_id).map_err(|error| {
                cleanup.failure(Status::internal(format!("complete pool clone: {error}")))
            })?
        };
        if !completed {
            return Err(cleanup.failure(Status::aborted(
                "pool clone cleanup episode was no longer current",
            )));
        }
        cleanup.complete_success();
        mark_sandbox_dirty(&self.db, sandbox_id);
        Ok(())
    }
}

async fn connect_clone_node(
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<pplx_space_node_mgr_client::NodeManagerGrpcClient, Status> {
    node_manager.connect().await.map_err(|error| match error {
        pplx_space_node_mgr_client::ConnectError::InvalidUri(_) => {
            Status::internal("node-manager endpoint is invalid")
        }
        pplx_space_node_mgr_client::ConnectError::Transport(_)
        | pplx_space_node_mgr_client::ConnectError::Connect(_) => {
            Status::unavailable("cannot connect to node-manager")
        }
    })
}

pub(super) fn pool_adoption_rollback_failure(original: Status, rollback: Status) -> Status {
    Status::new(
        original.code(),
        format!(
            "pool adoption failed: {}; rollback failed: {}",
            original.message(),
            rollback.message()
        ),
    )
}

fn prepare_clone_root(cache: &Path, id: &str) -> std::io::Result<std::fs::File> {
    use nix::fcntl::{OFlag, open, openat};
    use nix::sys::stat::{Mode, mkdirat};
    use std::os::unix::fs::MetadataExt;
    const FLAGS: OFlag = OFlag::O_RDONLY
        .union(OFlag::O_DIRECTORY)
        .union(OFlag::O_NOFOLLOW)
        .union(OFlag::O_CLOEXEC);
    let cache = std::fs::File::from(open(cache, FLAGS, Mode::empty())?);
    // Never adopt an existing entry. A freshly prepared root is private until
    // cloning has finished, so another cache writer cannot populate its tree.
    mkdirat(&cache, id, Mode::from_bits_truncate(0o700))?;
    let root = std::fs::File::from(openat(&cache, id, FLAGS, Mode::empty())?);
    let metadata = root.metadata()?;
    let mut contents = nix::dir::Dir::openat(&root, ".", FLAGS, Mode::empty())?;
    let empty = contents.iter().try_fold(true, |empty, entry| {
        let entry = entry?;
        Ok::<_, nix::errno::Errno>(empty && matches!(entry.file_name().to_bytes(), b"." | b".."))
    })?;
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.mode() & 0o777 != 0o700
        || !empty
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "clone preparation root was replaced",
        ));
    }
    Ok(root)
}

fn pool_clone_rpc_error(error: Status) -> Status {
    // Preserve capability/retry classification through cleanup, without
    // promoting a peer's free-form message into durable failure evidence.
    Status::new(
        error.code(),
        format!("clone sandbox failed (status {:?})", error.code()),
    )
}

fn pool_clone_template_ref(
    donor: &crate::db::SandboxRow,
    template: &str,
) -> Result<pplx_space_template_mgr_client::proto::TemplateRef, Status> {
    if donor.state != SandboxState::Paused || donor.template != template || donor.volume_encryption
    {
        return Err(Status::failed_precondition(
            "pool donor must be paused and match the plaintext template",
        ));
    }
    let hash = donor
        .template_hash
        .as_ref()
        .filter(|hash| !hash.is_empty())
        .ok_or_else(|| Status::failed_precondition("pool donor has no immutable template hash"))?;
    Ok(pplx_space_template_mgr_client::proto::TemplateRef {
        name: donor.template.clone(),
        content_hash: Some(hash.clone()),
        tag: None,
    })
}

#[cfg(test)]
#[path = "grpc_pool_wait_clone/tests.rs"]
mod tests;
