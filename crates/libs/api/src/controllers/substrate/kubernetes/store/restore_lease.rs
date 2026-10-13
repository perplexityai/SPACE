//! User restore lease: beginning, observing, and releasing restore ownership,
//! rollback reverts, and preparing a sandbox for wake.

use super::*;

impl K8sStore {
    /// Post-CAS-loss snapshot of the restore gate. Fresh execution work keeps
    /// ownership live after the original lease deadline so an already-issued
    /// Kubernetes or worker request cannot overlap a replacement owner.
    pub(in crate::controllers::substrate::kubernetes) async fn fetch_restore_ownership_snapshot(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(SandboxLifecycle, Option<String>, f64), SubstrateError> {
        let (lifecycle, marker, lease_remaining_secs, execution_remaining_secs): (
            String,
            Option<String>,
            f64,
            f64,
        ) = sqlx::query_as(sql!(
            "SELECT lifecycle,
                    evacuation_restore_target,
                    GREATEST(
                        EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                        0.0
                    )::float8,
                    GREATEST(
                        CASE
                          WHEN e.state IN ('creating', 'created', 'recovering', 'running')
                          THEN EXTRACT(EPOCH FROM (
                              e.updated_at + make_interval(secs => $2) - NOW()
                          ))
                          ELSE 0.0
                        END,
                        0.0
                    )::float8
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to query sandbox restore ownership")?
        .ok_or_else(|| SubstrateError::NotFound {
            message: format!("sandbox {sandbox_id} not found"),
        })?;
        let lifecycle =
            lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse sandbox restore lifecycle: {err}"),
                })?;
        let ownership_remaining_secs = if lifecycle == SandboxLifecycle::Restoring {
            lease_remaining_secs.max(execution_remaining_secs)
        } else {
            lease_remaining_secs
        };
        Ok((lifecycle, marker, ownership_remaining_secs))
    }

    pub(super) fn restore_ownership_collision(
        sandbox_id: SandboxId,
        ownership_remaining_secs: f64,
    ) -> SubstrateError {
        SubstrateError::restore_in_flight_with_lease(
            format!(
                "sandbox restore is already in flight for sandbox {sandbox_id}; \
                 retry the same sandbox after its current ownership window expires"
            ),
            Duration::from_secs_f64(ownership_remaining_secs),
        )
    }

    /// Observe the outcome of a user restore whose lease another request
    /// holds. One snapshot query so the lifecycle, lease, marker, and active
    /// execution routability are read consistently.
    pub(in crate::controllers::substrate::kubernetes) async fn observe_user_restore(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<UserRestoreObservation, SubstrateError> {
        self.observe_user_restores(&[sandbox_id])
            .await?
            .remove(&sandbox_id)
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox {sandbox_id} not found"),
            })
    }

    /// Observe all locally-joined user restores in one database round trip.
    /// The API process owns one polling loop which supplies this complete set;
    /// request futures subscribe to its per-sandbox watch channels instead of
    /// issuing independent queries while a restore is in flight.
    pub(in crate::controllers::substrate::kubernetes) async fn observe_user_restores(
        &self,
        sandbox_ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, UserRestoreObservation>, SubstrateError> {
        if sandbox_ids.is_empty() {
            return Ok(HashMap::new());
        }

        type Row = (
            Uuid,
            String,
            Option<String>,
            f64,
            f64,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let row = sqlx::query_as::<_, Row>(sql!(
            "SELECT s.id,
                    s.lifecycle,
                    s.evacuation_restore_target,
                    GREATEST(EXTRACT(EPOCH FROM (s.restore_lease_expires_at - NOW())), 0.0)::float8,
                    GREATEST(
                        CASE
                          WHEN e.state IN ('creating', 'created', 'recovering', 'running')
                          THEN EXTRACT(EPOCH FROM (
                              e.updated_at + make_interval(secs => $2) - NOW()
                          ))
                          ELSE 0.0
                        END,
                        0.0
                    )::float8,
                    e.generation,
                    e.state,
                    e.pod_name,
                    host(e.pod_ip)
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.id = ANY($1)"
        ))
        .bind(
            sandbox_ids
                .iter()
                .map(|sandbox_id| sandbox_id.as_uuid())
                .collect::<Vec<_>>(),
        )
        .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to batch-observe user restores")?;

        row.into_iter()
            .map(
                |(
                    sandbox_id,
                    lifecycle,
                    marker,
                    lease_remaining_secs,
                    execution_remaining_secs,
                    generation,
                    state,
                    pod_name,
                    pod_ip,
                )| {
                    let sandbox_id = SandboxId::from_uuid(sandbox_id);
                    let lifecycle = lifecycle.parse::<SandboxLifecycle>().map_err(|err| {
                        SubstrateError::Failed {
                            message: format!(
                                "failed to parse user restore lifecycle for {sandbox_id}: {err}"
                            ),
                        }
                    })?;
                    let ownership_remaining_secs = if lifecycle == SandboxLifecycle::Restoring {
                        lease_remaining_secs.max(execution_remaining_secs)
                    } else {
                        lease_remaining_secs
                    };
                    let observation = if lifecycle == SandboxLifecycle::Started {
                        match (generation, state, pod_name, pod_ip) {
                            (Some(generation), Some(state), Some(pod_name), Some(pod_ip))
                                if state.parse::<ActiveExecutionState>().map_err(|err| {
                                    SubstrateError::Failed {
                                        message: format!(
                                            "failed to parse user restore execution state for \
                                             {sandbox_id}: {err}"
                                        ),
                                    }
                                })? == ActiveExecutionState::Running =>
                            {
                                UserRestoreObservation::Routable {
                                    generation,
                                    pod_name,
                                    pod_ip,
                                }
                            }
                            _ => UserRestoreObservation::StartedUnroutable,
                        }
                    } else if matches!(
                        lifecycle,
                        SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                    ) && marker.is_none()
                        && ownership_remaining_secs > 0.0
                    {
                        UserRestoreObservation::InFlight {
                            lease_remaining: Duration::from_secs_f64(ownership_remaining_secs),
                        }
                    } else {
                        UserRestoreObservation::Claimable
                    };
                    Ok((sandbox_id, observation))
                },
            )
            .collect()
    }

    /// Best-effort lifecycle revert for the restore/wake rollback paths in
    /// [`super::super::execution`]. Reverts the row to `lifecycle` (in practice
    /// `"suspended"`) when a Kubernetes restore or wake fails mid-flight.
    /// CAS-guarded against `('failed', 'quarantined', 'deleted', 'cleanup_required')` so
    /// a rollback can't overwrite a terminal lifecycle the
    /// transient-lifecycle sweeper just claimed (the sweeper decided the
    /// operation was wedged; the rollback's "revert to my last known
    /// state" intent is wrong once the row has been retired).
    ///
    /// **Returns Ok regardless of whether the write applied.** The
    /// rollback is best-effort by design — `failed` is the correct
    /// resting state for a swept row, and callers don't act on the
    /// no-write case. The function name encodes this contract; do
    /// **not** call this from paths that depend on "the row is now in
    /// `lifecycle` after this returns." Use a dedicated CAS-guarded
    /// UPDATE inline if you need to detect race-loss (see the
    /// post-restore / post-wake CAS in
    /// `crates/libs/api/src/controllers/sandbox/lifecycle.rs` for the
    /// pattern).
    /// Best-effort revert of a sandbox's lifecycle (used by the restore
    /// rollback path to put a failed restore back to `suspended`).
    ///
    /// `restore_lease_token` token-guards the revert: when `Some`, the UPDATE
    /// only matches if the row still holds that exact lease token. This closes
    /// a stale-rollback race the restore lease introduced — a fresh attempt's
    /// clean-failure rollback could be delayed past the lease TTL, by which
    /// point a retry has claimed a *new* token and is actively restoring;
    /// without the guard the stale rollback would flip the newer restore back
    /// to `suspended` (and the clear-trigger would drop the newer lease),
    /// orphaning live work. `None` skips the guard (evacuation reverts, which
    /// are marker-gated and tokenless, and test-only lifecycle setup).
    pub(in crate::controllers::substrate::kubernetes) async fn revert_lifecycle_after_rollback_best_effort(
        &self,
        sandbox_id: SandboxId,
        lifecycle: &str,
        restore_lease_token: Option<Uuid>,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = $1
              WHERE id = $2
                AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')
                AND ($3::uuid IS NULL OR restore_lease_token = $3)"
        ))
        .bind(lifecycle)
        .bind(sandbox_id)
        .bind(restore_lease_token)
        .execute(&self.pool)
        .await
        .db_context_with(|| format!("failed to revert sandbox lifecycle to {lifecycle}"))?;
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn begin_restore_lifecycle(
        &self,
        sandbox_id: SandboxId,
        require_evacuation_marker: bool,
    ) -> Result<RestoreClaim, SubstrateError> {
        // `require_evacuation_marker` makes the restore claim atomic on the
        // node-drain `evacuation_restore_target` marker. The cluster-
        // controller's deferred cohort restore passes `true`: between listing
        // a restore-pending sandbox and this claim, a concurrent user restore
        // (allowed once the snapshot is portable) can clear the marker.
        // Folding the marker into the claim statement means the drain restore
        // loses the CAS in that race instead of restoring a sandbox the user
        // took over. The user restore path passes `false` — it must restore
        // regardless of the marker, and it is what clears it.
        //
        // Both claims accept an already-`restoring` row, but on different
        // conditions. The evacuation claim accepts a `restoring` row with the
        // marker still set — crash recovery after the controller dies between
        // this claim and finalize. The user claim accepts a markerless
        // `restoring` row only when no live restore lease exists: an expired
        // lease means a prior user attempt timed out and retry may re-drive,
        // while an unexpired lease means a restore is still active and must be
        // reported as retryable rather than driven twice. A `restoring` row
        // that still carries the marker is a drain restore in flight, and the
        // user path must NOT grab it. (A `suspended` row with a marker is still
        // a clean user takeover: the user wins `suspended → restoring`, clears
        // the marker, and the drain's marker-required claim then loses.)
        let restore_lease_token = (!require_evacuation_marker).then(Uuid::now_v7);
        let restore_claim = if require_evacuation_marker {
            sqlx::query_as::<_, RestoreClaimRow>(sql!(
                "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
                        AND evacuation_restore_target IS NOT NULL
                      FOR UPDATE
                 ),
                 updated AS (
                     UPDATE sandboxes s
                        SET lifecycle = 'restoring'
                       FROM candidate c
                     WHERE s.id = c.id
                      RETURNING c.lifecycle
                 )
                 SELECT u.lifecycle, args.restore_lease_token
                   FROM updated u
                   CROSS JOIN (
                     SELECT $2::bigint AS ttl_secs, $3::uuid AS restore_lease_token
                   ) args
                  WHERE args.ttl_secs >= 0"
            ))
        } else {
            // Preserve `lifecycle_changed_at` across `restoring -> restoring`
            // re-drives: restore selection uses it as the immutable boundary
            // between the original source window and artifacts published by a
            // failed restore branch. The lifecycle trigger still sets it for a
            // fresh `suspended -> restoring` claim. The restore lease protects
            // an active re-drive from the transient-lifecycle sweeper.
            sqlx::query_as::<_, RestoreClaimRow>(sql!(
                "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND (
                          LOWER(TRIM(lifecycle)) = 'suspended'
                          OR (
                            LOWER(TRIM(lifecycle)) = 'restoring'
                            AND evacuation_restore_target IS NULL
                            AND (
                              restore_lease_expires_at IS NULL
                              OR restore_lease_expires_at <= NOW()
                            )
                          )
                        )
                        AND (
                          restore_lease_expires_at IS NULL
                          OR restore_lease_expires_at <= NOW()
                        )
                        AND NOT EXISTS (
                          SELECT 1
                            FROM sandbox_k8s_executions e
                           WHERE e.sandbox_id = sandboxes.id
                             AND e.active
                             AND e.state IN ('creating', 'created', 'recovering', 'running')
                             AND e.updated_at >
                                 NOW() - make_interval(secs => $4)
                        )
                      FOR UPDATE
                 ),
                 updated AS (
                     UPDATE sandboxes s
                        SET lifecycle = 'restoring',
                            evacuation_restore_target = NULL,
                            restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                            restore_lease_token = $3
                       FROM candidate c
                      WHERE s.id = c.id
                      RETURNING c.lifecycle
                 )
                 SELECT lifecycle, $3::uuid AS restore_lease_token FROM updated"
            ))
        }
        .bind(sandbox_id.as_uuid())
        .bind(recovery_lease_ttl_secs())
        .bind(restore_lease_token)
        .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to begin sandbox restore")?;

        let Some(restore_claim) = restore_claim else {
            let (lifecycle, marker, ownership_remaining_secs) =
                self.fetch_restore_ownership_snapshot(sandbox_id).await?;
            // When the marker was required and the row is still in the
            // suspend/restore window, the CAS lost to a concurrent user
            // restore that cleared the marker — surface that distinctly so the
            // drain re-drive treats it as "no longer mine; drop it" rather
            // than a generic failure.
            if require_evacuation_marker
                && matches!(
                    lifecycle,
                    SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                )
            {
                return Err(SubstrateError::EvacuationClaimLost {
                    message: format!(
                        "evacuation restore claim lost for sandbox {sandbox_id}: \
                         evacuation_restore_target cleared (user restore took over)"
                    ),
                });
            }
            if !require_evacuation_marker
                && marker.is_none()
                && ownership_remaining_secs > 0.0
                && matches!(
                    lifecycle,
                    SandboxLifecycle::Suspended | SandboxLifecycle::Restoring
                )
            {
                return Err(Self::restore_ownership_collision(
                    sandbox_id,
                    ownership_remaining_secs,
                ));
            }
            return Err(SubstrateError::Conflict {
                message: format!("sandbox cannot be restored while lifecycle is {lifecycle}"),
            });
        };

        match restore_claim.lifecycle.trim().to_ascii_lowercase().as_str() {
            "suspended" => Ok(RestoreClaim::FreshFromSuspended {
                restore_lease_token: restore_claim.restore_lease_token,
            }),
            "restoring" => Ok(RestoreClaim::RedroveRestoring {
                restore_lease_token: restore_claim.restore_lease_token,
            }),
            lifecycle => Err(SubstrateError::Failed {
                message: format!("restore claim returned unexpected lifecycle {lifecycle}"),
            }),
        }
    }

    async fn claim_markerless_restoring_restore_lease(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RestoreClaim>, SubstrateError> {
        let restore_lease_token = Uuid::now_v7();
        let restore_claim = sqlx::query_as::<_, RestoreClaimRow>(sql!(
            "WITH candidate AS (
                 SELECT id, lifecycle
                   FROM sandboxes
                  WHERE id = $1
                    AND LOWER(TRIM(lifecycle)) = 'restoring'
                    AND evacuation_restore_target IS NULL
                    AND (
                      restore_lease_expires_at IS NULL
                      OR restore_lease_expires_at <= NOW()
                    )
                    AND NOT EXISTS (
                      SELECT 1
                        FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = sandboxes.id
                         AND e.active
                         AND e.state IN ('creating', 'created', 'recovering', 'running')
                         AND e.updated_at > NOW() - make_interval(secs => $4)
                    )
                  FOR UPDATE
             ),
             updated AS (
                 UPDATE sandboxes s
                    SET restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                        restore_lease_token = $3
                   FROM candidate c
                  WHERE s.id = c.id
                  RETURNING c.lifecycle
             )
             SELECT lifecycle, $3::uuid AS restore_lease_token FROM updated"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(recovery_lease_ttl_secs())
        .bind(restore_lease_token)
        .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to claim restoring sandbox restore lease")?;

        if let Some(restore_claim) = restore_claim {
            return match restore_claim.lifecycle.trim().to_ascii_lowercase().as_str() {
                "restoring" => Ok(Some(RestoreClaim::RedroveRestoring {
                    restore_lease_token: restore_claim.restore_lease_token,
                })),
                lifecycle => Err(SubstrateError::Failed {
                    message: format!(
                        "restore lease claim returned unexpected lifecycle {lifecycle}"
                    ),
                }),
            };
        }

        let (lifecycle, marker, ownership_remaining_secs) =
            self.fetch_restore_ownership_snapshot(sandbox_id).await?;

        if lifecycle == SandboxLifecycle::Restoring
            && marker.is_none()
            && ownership_remaining_secs > 0.0
        {
            return Err(Self::restore_ownership_collision(
                sandbox_id,
                ownership_remaining_secs,
            ));
        }

        if lifecycle == SandboxLifecycle::Restoring && marker.is_some() {
            return Ok(None);
        }

        Err(SubstrateError::Conflict {
            message: format!(
                "sandbox {sandbox_id} changed lifecycle while claiming its restore lease"
            ),
        })
    }

    pub(in crate::controllers::substrate::kubernetes) async fn release_user_restore_lease(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<(), SubstrateError> {
        // Expire ownership so a clean pre-work failure can retry immediately,
        // but retain the paired token and attempt clock at NOW(). The transient
        // sweeper requires a full quiet window after that clock before changing
        // `restoring`, while the next retry atomically replaces the stale pair.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW()
              WHERE id = $1
                AND restore_lease_token = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(restore_lease_token)
        .execute(&self.pool)
        .await
        .db_context("failed to release sandbox restore lease")?;
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn prepare_wake_sandbox(
        &self,
        sandbox: &Sandbox,
        sandbox_id: SandboxId,
        preclaimed_restore_claim: Option<RestoreClaim>,
    ) -> Result<PreparedWakeSandbox, SubstrateError> {
        if super::super::is_suspended_lifecycle(&sandbox.lifecycle) {
            // Reached only by the user wake entry (the evacuation restore
            // already flipped the row to `restoring` before this point), so
            // the marker is not required for this claim.
            let restore_claim = match preclaimed_restore_claim {
                Some(restore_claim) => restore_claim,
                None => self.begin_restore_lifecycle(sandbox_id, false).await?,
            };
            return Ok(PreparedWakeSandbox {
                sandbox: self.require_sandbox(sandbox_id).await?,
                restore_started: true,
                restore_claim: Some(restore_claim),
            });
        }

        if sandbox.lifecycle.trim().eq_ignore_ascii_case("restoring") {
            // Markerless user restores must acquire the lease before a retry
            // can re-drive. Marker-set evacuation restores keep the preexisting
            // bounded takeover behavior; the marker owner still controls them.
            let restore_claim = match preclaimed_restore_claim {
                Some(restore_claim) => Some(restore_claim),
                None => {
                    self.claim_markerless_restoring_restore_lease(sandbox_id)
                        .await?
                }
            };
            return Ok(PreparedWakeSandbox {
                sandbox: sandbox.clone(),
                restore_started: false,
                restore_claim,
            });
        }

        Ok(PreparedWakeSandbox {
            sandbox: sandbox.clone(),
            restore_started: false,
            restore_claim: None,
        })
    }
}
