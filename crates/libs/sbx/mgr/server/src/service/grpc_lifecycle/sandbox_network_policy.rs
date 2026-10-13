//! `GetNetworkPolicy`, `SetNetworkPolicy`, and `TemplateNetworkPolicyUpdated`
//! RPCs.

use super::*;

impl SandboxManagerService {
    pub(in crate::service) async fn get_network_policy(
        &self,
        request: Request<GetNetworkPolicyRequest>,
    ) -> Result<Response<GetNetworkPolicyResponse>, Status> {
        self.authorize_peer(&request, "GetNetworkPolicy", ALLOW_SANDBOX_OBSERVER)?;
        let req = request.get_ref();
        let sandbox_ids = request_sandbox_ids(&req.sandbox_ids);
        let mut response = GetNetworkPolicyResponse::default();

        for sandbox_id in sandbox_ids {
            let sandbox = self.lookup_sandbox(&sandbox_id)?;
            let effective_policy = self.effective_network_policy(&sandbox).await?;
            let detail = NetworkPolicyDetail {
                sandbox_id: sandbox.id.clone(),
                effective_policy: Some(network_policy_to_proto(&effective_policy.policy)),
                inherited: effective_policy.inherited,
                source_template: if effective_policy.inherited {
                    sandbox.template.clone()
                } else {
                    String::new()
                },
                effective_generation: effective_policy.generation,
                applied_generation: sandbox.applied_network_policy_generation,
                applied_source: sandbox.applied_network_policy_source.as_proto(),
                authority: effective_policy.authority.as_proto_i32(),
            };
            response.policies.push(detail);
        }

        Ok(Response::new(response))
    }

    pub(in crate::service) async fn set_network_policy(
        &self,
        request: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            "SetNetworkPolicy",
            ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR,
        )?;
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        // Serialize against the pre-wake network-policy gate. The resume path
        // holds this sandbox's wake lock across [gate confirm -> VM thaw] and
        // re-reads the desired policy fresh inside the gate. Taking the same
        // lock here forces a policy update to land either before the gate reads
        // it (the gate then confirms and thaws under the new policy) or after
        // the wake fully completes (a legitimate post-resume update the
        // reconciler converges). It can no longer commit inside the
        // confirm->thaw window and leave a live VM running under a stale gateway
        // policy. Lock order matches the wake path -- wake lock before the brief
        // db lock, and this handler never takes the artifact lock -- so it adds
        // no cycle.
        //
        // Key the lock on the canonical `sandbox.id` resolved by the lookup
        // above, not the raw `req.sandbox_id`: `lookup_sandbox` accepts a UUID
        // prefix or short label, and the resume path keys its wake lock on the
        // canonical id. Locking on the request spelling would take a different
        // map entry for the same sandbox and silently lose the mutual exclusion.
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox.id);
        let _guard = wake_lock.lock().await;
        // Re-read under the lock: an in-flight wake we queued behind may have
        // changed the sandbox state or policy while we waited to acquire it.
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        // Reject a synchronous refresh for a pool member up front, before any DB
        // mutation. Pool members (pre-adoption) intentionally run with no network
        // identity — the reconciler installs no_identity and the async enqueue
        // guard excludes pool_template-set sandboxes. A synchronous refresh would
        // bypass that and push a real policy to the gateway before adoption.
        // Validating here (rather than after the policy-override write) keeps a
        // rejected request from leaving behind mutated desired-policy state.
        if req.sync_to_gateway && Self::sandbox_is_pool_member(&sandbox) {
            return Err(Status::failed_precondition(
                "synchronous network policy update is not valid for a pool member; adopt the sandbox first",
            ));
        }
        let policy_override =
            network_policy_override_from_set_request(req.inherit_template, req.policy.as_ref())?;
        let publish_node_state = !should_skip_node_publish(&request);
        let requested_authority = crate::db::PolicyAuthority::try_from(req.authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        require_local_authority_peer(peer.principal(), requested_authority)?;
        let desired_generation = set_network_policy_desired_generation(
            &sandbox,
            &policy_override,
            req.policy_generation,
        );
        let event_details = || {
            SandboxEventDetails::default()
                .operation(if policy_override.is_some() {
                    "set_override"
                } else {
                    "inherit_template_default"
                })
                .network_policy_source(if policy_override.is_some() {
                    "override"
                } else {
                    "template"
                })
                .network_policy_authority(requested_authority.as_str())
                .network_policy_generation(desired_generation.to_string())
        };
        let outcome = {
            let db = self.db_conn()?;
            if requested_authority == crate::db::PolicyAuthority::Local && policy_override.is_none()
            {
                crate::db::clear_local_network_policy(
                    &db,
                    &sandbox.id,
                    Self::sandbox_has_live_net_gateway(&sandbox),
                    publish_node_state,
                )
            } else {
                crate::db::update_network_policy_override(
                    &db,
                    &sandbox.id,
                    policy_override.as_ref(),
                    desired_generation,
                    requested_authority,
                    Self::sandbox_has_live_net_gateway(&sandbox),
                    publish_node_state,
                )
            }
            .map_err(|e| Status::internal(format!("db network_policy update failed: {e}")))?
        };
        if outcome.decision == crate::db::NetworkPolicyOverrideUpdate::Conflict {
            pplx_space_metrics::reconcile::apply_conflict(FACET_NETWORK_POLICY);
            emit_sandbox_event(
                &sandbox.id,
                SandboxEventType::NetworkPolicyUpdateFailed,
                SandboxEventSource::SbxManager,
                event_details()
                    .error("network policy generation conflict")
                    .routine(),
            );
            return Err(Status::failed_precondition(
                "network policy generation conflict",
            ));
        }
        match outcome.decision {
            crate::db::NetworkPolicyOverrideUpdate::Apply => {
                pplx_space_metrics::reconcile::apply_applied(FACET_NETWORK_POLICY, "apply");
            }
            crate::db::NetworkPolicyOverrideUpdate::Repair => {
                pplx_space_metrics::reconcile::apply_applied(FACET_NETWORK_POLICY, "repair");
            }
            crate::db::NetworkPolicyOverrideUpdate::Skip => {
                pplx_space_metrics::reconcile::apply_skipped(FACET_NETWORK_POLICY, "idempotent");
            }
            crate::db::NetworkPolicyOverrideUpdate::Conflict => unreachable!("handled above"),
        }

        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        let effective_policy = self.effective_network_policy(&sandbox).await?;
        let mut refreshed_sandbox = None;
        if req.sync_to_gateway {
            // Pool-member rejection happened up front, before the DB mutation.
            if !Self::sandbox_has_live_net_gateway(&sandbox) {
                return Err(Status::unavailable(
                    "synchronous network policy update requires a live net-gateway",
                ));
            }
            let effective_policy_ref = effective_network_policy_ref(&sandbox, &effective_policy)?;
            match tokio::time::timeout(
                NETWORK_POLICY_SYNC_REFRESH_TIMEOUT,
                refresh_network_policy(
                    &self.db,
                    &sandbox.id,
                    Some(&effective_policy_ref),
                    "set-policy-sync",
                ),
            )
            .await
            {
                Ok(Ok(())) => {
                    // Record the applied state, mirroring the periodic reconciler
                    // (refresh + mark_network_policy_applied). Without this the
                    // applied_* columns stay stale: the response would misreport
                    // applied_generation/applied_source, and downstream
                    // idempotency/repair logic (keyed off applied_*) would later
                    // conclude the policy isn't applied and redundantly re-push.
                    self.mark_network_policy_applied(
                        &sandbox.id,
                        effective_policy.inherited,
                        effective_policy.generation,
                    )?;
                    // The durable outbox row written by the DB update (when
                    // queued_live_gateway) is left for the in-memory reconciler to
                    // drain rather than deleted here. Deleting it inline can't be
                    // done race-free: the row is keyed only by sandbox_id (second-
                    // resolution timestamps give no unique token), so a concurrent
                    // SetNetworkPolicy that re-enqueues between our read and delete
                    // would be clobbered. Handing it to the reconciler is safe: the
                    // reconciler reads the *latest* desired policy at drain time
                    // (correctly applying any concurrent newer update), finds the
                    // gateway already matches ours (idempotent), and deletes the row
                    // via its normal post-drain cleanup.
                    if outcome.queued_live_gateway {
                        self.enqueue_network_policy_reconcile(sandbox.id.clone());
                    }
                    refreshed_sandbox = Some(self.lookup_sandbox(&sandbox.id)?);
                }
                Ok(Err(error)) => {
                    // The desired policy is committed but the gateway push
                    // failed. Fall back to the async retry path so a caller that
                    // doesn't tear the sandbox down (or a slow rollback) still
                    // converges, then surface the failure so sync callers can
                    // roll back.
                    if outcome.queued_live_gateway {
                        self.enqueue_network_policy_reconcile(sandbox.id.clone());
                    }
                    return Err(Status::unavailable(format!(
                        "synchronous network policy refresh failed: {error}"
                    )));
                }
                Err(_) => {
                    if outcome.queued_live_gateway {
                        self.enqueue_network_policy_reconcile(sandbox.id.clone());
                    }
                    return Err(Status::unavailable(format!(
                        "synchronous network policy refresh timed out after {}ms",
                        NETWORK_POLICY_SYNC_REFRESH_TIMEOUT.as_millis()
                    )));
                }
            }
        } else if outcome.queued_live_gateway {
            self.enqueue_network_policy_reconcile(sandbox.id.clone());
        }
        let decision = match outcome.decision {
            crate::db::NetworkPolicyOverrideUpdate::Apply => "apply",
            crate::db::NetworkPolicyOverrideUpdate::Repair => "repair",
            crate::db::NetworkPolicyOverrideUpdate::Skip => "skip",
            crate::db::NetworkPolicyOverrideUpdate::Conflict => "conflict",
        };
        if outcome.decision != crate::db::NetworkPolicyOverrideUpdate::Skip {
            emit_sandbox_event(
                &sandbox.id,
                SandboxEventType::NetworkPolicyUpdateRequested,
                SandboxEventSource::SbxManager,
                event_details(),
            );

            let mut details = event_details().result(decision);
            if let Ok(effective_policy_ref) =
                effective_network_policy_ref(&sandbox, &effective_policy)
            {
                details = details.expected_network_policy_ref(effective_policy_ref.to_string());
            }
            emit_sandbox_event(
                &sandbox.id,
                SandboxEventType::NetworkPolicyUpdateCompleted,
                SandboxEventSource::SbxManager,
                details,
            );
        }
        let response_sandbox = refreshed_sandbox.as_ref().unwrap_or(&sandbox);

        #[allow(deprecated)]
        Ok(Response::new(SetNetworkPolicyResponse {
            effective_policy: Some(network_policy_to_proto(&effective_policy.policy)),
            inherited: effective_policy.inherited,
            source_template: if effective_policy.inherited {
                sandbox.template.clone()
            } else {
                String::new()
            },
            effective_generation: effective_policy.generation,
            applied_generation: response_sandbox.applied_network_policy_generation,
            applied_source: response_sandbox.applied_network_policy_source.as_proto(),
            apply_decision: apply_decision_to_proto(match outcome.decision {
                crate::db::NetworkPolicyOverrideUpdate::Apply => ApplyDecisionKind::Apply,
                crate::db::NetworkPolicyOverrideUpdate::Repair => ApplyDecisionKind::Repair,
                crate::db::NetworkPolicyOverrideUpdate::Skip => ApplyDecisionKind::Skip,
                crate::db::NetworkPolicyOverrideUpdate::Conflict => ApplyDecisionKind::Conflict,
            }),
            authority: effective_policy.authority.as_proto_i32(),
            // Only a confirmed live-gateway sync refreshes the sandbox row.
            applied_synchronously: refreshed_sandbox.is_some(),
        }))
    }

    pub(in crate::service) async fn template_network_policy_updated(
        &self,
        request: Request<TemplateNetworkPolicyUpdatedRequest>,
    ) -> Result<Response<TemplateNetworkPolicyUpdatedResponse>, Status> {
        self.authorize_peer(
            &request,
            "TemplateNetworkPolicyUpdated",
            ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR,
        )?;
        let req = request.get_ref();
        let template = req.template.trim();
        if template.is_empty() {
            return Err(Status::invalid_argument("template is required"));
        }
        req.policy
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("policy is required"))
            .and_then(network_policy_from_proto)?;
        crate::db::PolicyAuthority::try_from(req.authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.template_network_policies.invalidate(template).await;

        let sandboxes = {
            let db = self.db_conn()?;
            crate::db::list_sandboxes(&db)
                .map_err(|error| {
                    Status::internal(format!(
                        "db list failed: {}",
                        crate::db::describe_error(&error)
                    ))
                })?
                .into_iter()
                .filter(|sandbox| {
                    sandbox.template == template
                        && !Self::sandbox_is_pool_member(sandbox)
                        && sandbox.network_policy_override.is_none()
                })
                .collect::<Vec<_>>()
        };

        let mut queued_gateways = 0;
        let mut repair_gateways = 0;
        for sandbox in &sandboxes {
            if !Self::sandbox_has_live_net_gateway(sandbox) {
                continue;
            }
            let queued = {
                let db = self.db_conn()?;
                crate::db::enqueue_network_policy_reconcile_if_live(&db, &sandbox.id).map_err(
                    |e| Status::internal(format!("db enqueue network_policy failed: {e}")),
                )?
            };
            if queued {
                self.enqueue_network_policy_reconcile(sandbox.id.clone());
                queued_gateways += 1;
                if sandbox.applied_network_policy_source
                    == crate::db::NetworkPolicyAppliedSource::Template
                    && sandbox.applied_network_policy_generation == req.generation
                {
                    repair_gateways += 1;
                }
            }
        }

        Ok(Response::new(TemplateNetworkPolicyUpdatedResponse {
            matched_sandboxes: sandboxes.len() as u32,
            notified_gateways: queued_gateways,
            apply_decision: apply_decision_to_proto(if queued_gateways > repair_gateways {
                ApplyDecisionKind::Apply
            } else if repair_gateways > 0 {
                ApplyDecisionKind::Repair
            } else {
                ApplyDecisionKind::Skip
            }),
        }))
    }
}
