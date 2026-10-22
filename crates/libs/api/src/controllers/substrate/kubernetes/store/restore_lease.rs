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

    /// Revert a failed fresh restore claim from `restoring` to `suspended`.
    ///
    /// The revert is the one writer that knows the restore was abandoned, so
    /// it records that on the execution the claim created: in the same
    /// transaction, that execution is retired as `replaced` and the row is
    /// unplaced. A pod the claim materialized then belongs to no active
    /// execution, so no later restore adopts it (a new claim allocates a newer
    /// generation, whose lingering-pod sweep deletes this one), and a backup
    /// its runtime captures registers against a retired generation. Left
    /// active instead, the execution keeps a same-generation pod adoptable on
    /// a `suspended` row that no reconciler revisits, and that row looks like
    /// one whose pod holds the only copy of the sandbox's state.
    ///
    /// The retirement needs the suspended-revert proof: restore selection on
    /// the reverted row must still choose a ready, durably stored pre-restore
    /// source, the state the sandbox had when it was suspended. The retired
    /// runtime served no user request: the access, runtime-client, ingress
    /// and data-plane route paths never dial a `restoring` sandbox's pod, but
    /// join its restore or fail retryably, once every routing replica runs
    /// them; until then retirement stays disabled. Without the proof, or when the
    /// active execution can't be shown
    /// to be this claim's (one that predates the claim was reused, not
    /// created by it), only the lifecycle reverts and the execution is left as
    /// it was.
    ///
    /// `restore_lease_token` binds the revert to the claim that owns the row:
    /// a rollback delayed past the lease TTL must not flip a retry that holds
    /// a newer token back to `suspended` (the lease-clear trigger would also
    /// drop that retry's lease). `None` is the tokenless evacuation claim.
    /// Either way the row must still be `restoring`; a row another writer
    /// moved on, including one a node converged to `started`, is left alone.
    ///
    /// `activation_attempt` is the attempt and generation this claim was
    /// assigned, if it got that far. The execution is retired only if it
    /// still carries them, because a redrive of the same `restoring` row
    /// reuses the execution under a new attempt. A claim with no attempt is
    /// identified by its lease token alone, so a tokenless one retires
    /// nothing: a delayed evacuation rollback can't tell its execution from a
    /// later evacuation redrive's.
    ///
    /// With `retirement` disabled, only the lifecycle reverts, whatever the
    /// proof says: the execution, placement and placement clock stay as the
    /// restore left them (see [`AbandonedRestoreRetirement`]).
    pub(in crate::controllers::substrate::kubernetes) async fn revert_restore_claim_to_suspended(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Option<Uuid>,
        activation_attempt: Option<(SandboxActivationAttemptId, i64)>,
        retirement: crate::controllers::substrate::AbandonedRestoreRetirement,
    ) -> Result<RestoreClaimRevert, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start restore revert transaction")?;
        // Lock first, in its own statement: a later statement in this
        // transaction then reads the snapshots and executions as of after the
        // lock, so a writer that committed while this waited is seen.
        let locked: Option<bool> = sqlx::query_scalar(sql!(
            "SELECT TRUE
               FROM sandboxes
              WHERE id = $1
                AND lifecycle = 'restoring'
                AND ($2::uuid IS NULL OR restore_lease_token = $2)
                FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(restore_lease_token)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock sandbox for restore revert")?;
        if locked.is_none() {
            tx.commit()
                .await
                .db_context("failed to commit restore revert transaction")?;
            return Ok(RestoreClaimRevert::NotReverted);
        }

        let proof = restore_retry_suspended_revert_snapshot_exists("s");
        // Under the row lock no other claim can replace the execution, and
        // `lifecycle_changed_at` still reads this claim's `restoring`
        // transition, so an active execution created since, carrying this
        // claim's attempt (or, with no attempt, under this claim's lease
        // token), is the one it created.
        let query = format!(
            "SELECT claimed.generation,
                    claimed.pod_name,
                    EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                    ),
                    {proof}
               FROM sandboxes s
               LEFT JOIN LATERAL (
                   SELECT e.generation, e.pod_name
                     FROM sandbox_k8s_executions e
                    WHERE e.sandbox_id = s.id
                      AND e.active
                      AND e.created_at >= s.lifecycle_changed_at
                      AND CASE
                              WHEN $3::uuid IS NOT NULL
                                  THEN e.activation_attempt_id = $3
                                       AND e.generation = $4
                              ELSE $2::uuid IS NOT NULL
                          END
               ) claimed ON TRUE
              WHERE s.id = $1"
        );
        let (claimed_generation, claimed_pod, any_active, source_proven): (
            Option<i64>,
            Option<String>,
            bool,
            bool,
        ) = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(restore_lease_token)
            .bind(activation_attempt.map(|(attempt, _)| attempt))
            .bind(activation_attempt.map(|(_, generation)| generation))
            .fetch_one(&mut *tx)
            .await
            .db_context("failed to read restore revert evidence")?;

        let retire = source_proven && retirement.is_enabled();
        let outcome = match (claimed_generation.zip(claimed_pod), retire) {
            (Some((generation, pod_name)), true) => {
                if !self
                    .finish_active_execution_generation_in(
                        &mut tx, sandbox_id, generation, "replaced",
                    )
                    .await?
                {
                    // Not expected under the row lock; fail closed rather
                    // than revert a row whose execution this claim can't
                    // retire.
                    tx.rollback()
                        .await
                        .db_context("failed to roll back restore revert transaction")?;
                    return Ok(RestoreClaimRevert::NotReverted);
                }
                // `target_node_id` and the artifact marker name the wake
                // source and stay; the lifecycle triggers keep the marker on
                // `suspended`. `placement_changed_at` is cleared, as the
                // markerless restoring recovery in `pod_tracking.rs` does for
                // its rows: a
                // restore whose pod bound moved it past the proven
                // pre-restore source, and both the suspended selector and
                // `durably_restorable` take a markerless snapshot only when
                // it is newer than the last placement.
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET lifecycle = 'suspended',
                            placement = 'unplaced',
                            active_node_id = NULL,
                            placement_changed_at = NULL
                      WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&mut *tx)
                .await
                .db_context("failed to revert sandbox lifecycle to suspended")?;
                RestoreClaimRevert::ExecutionRetired { pod_name }
            }
            (None, true) if !any_active => {
                // Proven, and no execution at all: the same placement-clock
                // clear as above. An execution the claim reused rather than
                // created keeps the clock, since nothing proves its pod
                // holds no state of its own.
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET lifecycle = 'suspended',
                            placement_changed_at = NULL
                      WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&mut *tx)
                .await
                .db_context("failed to revert sandbox lifecycle to suspended")?;
                RestoreClaimRevert::LifecycleOnly
            }
            (claimed, _) => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&mut *tx)
                .await
                .db_context("failed to revert sandbox lifecycle to suspended")?;
                if claimed.is_some() || any_active {
                    RestoreClaimRevert::ExecutionLeftActive
                } else {
                    RestoreClaimRevert::LifecycleOnly
                }
            }
        };
        tx.commit()
            .await
            .db_context("failed to commit restore revert transaction")?;
        Ok(outcome)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn begin_restore_lifecycle(
        &self,
        sandbox_id: SandboxId,
        require_evacuation_marker: bool,
    ) -> Result<RestoreClaim, SubstrateError> {
        self.begin_restore_lifecycle_for(sandbox_id, require_evacuation_marker, None)
            .await
    }

    /// The evacuation claim for a caller that has already fixed the lifecycle
    /// it restores to. Only a `Stopped` target may take the stopped arm: a
    /// caller holding a stale `Started`/`Paused` target from before the row
    /// became a stopped evacuation would otherwise claim it and restore it
    /// toward a lifecycle the user no longer has, and a failure would leave it
    /// `restoring`, since that claim is never rolled back.
    pub(in crate::controllers::substrate::kubernetes) async fn begin_evacuation_restore_lifecycle(
        &self,
        sandbox_id: SandboxId,
        target: SandboxLifecycle,
    ) -> Result<RestoreClaim, SubstrateError> {
        self.begin_restore_lifecycle_for(sandbox_id, true, Some(target))
            .await
    }

    async fn begin_restore_lifecycle_for(
        &self,
        sandbox_id: SandboxId,
        require_evacuation_marker: bool,
        evacuation_target: Option<SandboxLifecycle>,
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
        //
        // The evacuation claim also takes a released stopped evacuation:
        // marker `'stopped'` and no `active_node_id`, for a `Stopped` target
        // only ($5; see `begin_evacuation_restore_lifecycle`). A markerless
        // `stopped` sandbox is user-owned, and one still on its node would get
        // two homes. It claims to `restoring` like the suspend path; see
        // `RestoreClaim::FreshFromStopped`.
        let restore_lease_token = (!require_evacuation_marker).then(Uuid::now_v7);
        let restore_claim = if require_evacuation_marker {
            sqlx::query_as::<_, RestoreClaimRow>(sql!(
                "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND (
                          LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
                          OR (
                            LOWER(TRIM(lifecycle)) = 'stopped'
                            AND evacuation_restore_target = 'stopped'
                            AND active_node_id IS NULL
                            AND $5::text = 'stopped'
                          )
                        )
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
        .bind(evacuation_target.map(SandboxLifecycle::as_str))
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
            // Three of a `stopped` row's misses are deferrals, not restore
            // failures, and report `EvacuationClaimLost`: its marker is gone, it
            // carries the `'stopped'` marker but the caller's target isn't
            // `Stopped`, or the teardown hasn't released its node. A row with any
            // other marker falls through to `Conflict` below.
            if require_evacuation_marker && lifecycle == SandboxLifecycle::Stopped {
                let reason = match marker.as_deref() {
                    None => Some("evacuation_restore_target cleared while stopped"),
                    Some("stopped") => {
                        Some("its stopped evacuation is not claimable by this attempt")
                    }
                    Some(_) => None,
                };
                if let Some(reason) = reason {
                    return Err(SubstrateError::EvacuationClaimLost {
                        message: format!(
                            "evacuation restore claim unavailable for sandbox {sandbox_id}: \
                             {reason}"
                        ),
                    });
                }
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
            "stopped" => Ok(RestoreClaim::FreshFromStopped),
            "restoring" => Ok(RestoreClaim::RedroveRestoring {
                restore_lease_token: restore_claim.restore_lease_token,
            }),
            lifecycle => Err(SubstrateError::Failed {
                message: format!("restore claim returned unexpected lifecycle {lifecycle}"),
            }),
        }
    }

    pub(in crate::controllers::substrate::kubernetes) async fn claim_markerless_restoring_restore_lease(
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

    /// Commit `restoring -> started` for the restore this caller owns, once
    /// its wake has succeeded. Returns whether the row moved.
    ///
    /// This is the cancelled-wake redrive's fenced completion CAS
    /// (`wake_completion.rs`): the row must still be `restoring` with a
    /// routable active execution behind it. Where the redrive waits for the
    /// lease to expire, so that no live attempt is overridden, this requires
    /// the lease to be the caller's own: the owner is that live attempt. A
    /// row a rollback, the sweeper, a suspend or a delete has moved on matches
    /// nothing (leaving `restoring` also clears the token, through the
    /// `sandboxes_clear_restore_lease` trigger), and once it is `started`
    /// neither the API rollback nor the sweeper's revert (both
    /// `restoring`-guarded) can move it back.
    ///
    /// Without this, a Kubernetes restore driven from an access path, which
    /// runs no lifecycle-controller completion, stays `restoring` until the
    /// node's published state converges it, and a user request may not dial
    /// it until then.
    pub(in crate::controllers::substrate::kubernetes) async fn complete_owned_user_restore(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<bool, SubstrateError> {
        let routable =
            pplx_space_common_svc::sql::sandbox_routability::routable_execution_or_non_k8s(
                "sandboxes",
                "$3",
            );
        let query = format!(
            "UPDATE sandboxes
                SET lifecycle = 'started', placement = 'on_node'
              WHERE id = $1
                AND lifecycle = 'restoring'
                AND restore_lease_token = $2
                AND {routable}"
        );
        let completed = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id.as_uuid())
            .bind(restore_lease_token)
            .bind(pplx_space_common_core::types::SubstrateKind::Kubernetes)
            .execute(&self.pool)
            .await
            .db_context("failed to complete owned sandbox restore")?
            .rows_affected();
        Ok(completed == 1)
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
