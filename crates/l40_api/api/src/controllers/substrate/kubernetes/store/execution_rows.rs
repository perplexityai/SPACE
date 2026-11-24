//! `sandbox_k8s_executions` rows: claiming, locking, finishing, and reading the
//! active execution of a sandbox, plus abandoning unmaterialized create claims.

use super::*;

impl K8sStore {
    pub(super) fn execution_pod_name(sandbox_id: SandboxId, generation: i64) -> String {
        format!(
            "space-sbx-{}-g{}",
            sandbox_id.as_uuid().simple(),
            generation
        )
    }

    /// Record generation 1 of a freshly inserted sandbox as already retired,
    /// so its first execution is allocated generation 2.
    ///
    /// For a cold create under an ID a warm-pool slot reserved: the slot's
    /// pod was built as this ID's generation 1 (`space-sbx-<id>-g1`) and may
    /// still exist while the pool collects it. Generation allocation is
    /// `MAX(generation) + 1` everywhere (create, wake, restore, recovery), so
    /// one retired row moves this execution and every later one past that
    /// pod's name, route identity, and readiness identity. The row is
    /// inactive, so nothing routes to it. A no-op when the sandbox already
    /// has execution rows.
    pub(in crate::controllers::substrate::kubernetes) async fn retire_pool_reserved_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        let pod_name = Self::execution_pod_name(sandbox_id, 1);
        pplx_space_sandbox_store::k8s_executions::claim::retire_pool_reserved_generation(
            &self.pool,
            sandbox_id,
            &pod_name,
            &self.namespace,
        )
        .await
        .db_context("failed to retire the pool-reserved execution generation")?;
        Ok(())
    }

    pub(super) async fn active_execution_row(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxExecutionRow>, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::active_execution(
            &mut *conn,
            sandbox_id,
            super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS,
        )
        .await
        .and_then(|record| record.map(SandboxExecutionRow::try_from).transpose())
        .db_context("failed to query active Kubernetes execution row")
    }

    pub(super) async fn insert_active_execution_claim_in(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        // The minted generation is correct only under the caller's sandbox
        // row lock with the insert below run next in this transaction; see
        // `pplx_space_sandbox_store::k8s_executions::claim::mint_generation`.
        let generation = pplx_space_sandbox_store::k8s_executions::claim::mint_generation(
            &mut *conn,
            sandbox_uuid,
        )
        .await
        .db_context("failed to allocate Kubernetes execution generation")?;
        let pod_name = Self::execution_pod_name(sandbox_uuid, generation);

        let row = pplx_space_sandbox_store::k8s_executions::claim::insert_creating_claim(
            &mut *conn,
            sandbox_uuid,
            generation,
            &pod_name,
            &self.namespace,
        )
        .await
        .and_then(|record| record.map(SandboxExecutionRow::try_from).transpose())
        .db_context("failed to insert Kubernetes execution row")?;

        let created = row.is_some();
        let row = match row {
            Some(row) => row,
            None => self
                .active_execution_row(conn, sandbox_uuid)
                .await?
                .ok_or_else(|| SubstrateError::Failed {
                    message: "failed to create active Kubernetes execution row".to_string(),
                })?,
        };

        Ok(SandboxExecutionClaim {
            row,
            created,
            route_publication: None,
        })
    }

    /// Apply the pod metadata update to the (caller-locked) active execution
    /// row. Callers must have taken the row lock first
    /// (`lock_tracked_pod_in`) — the guarded UPDATE alone cannot
    /// distinguish "row already current" from "row retired underneath".
    ///
    /// Stamps `pod_materialized_at` when the row records a pod UID, or held
    /// one before this write replaces or clears it, and never clears a stamp:
    /// the record that a pod ran must outlive the UID. An otherwise unchanged
    /// row is still written when it carries a UID and no stamp, so a row from
    /// before the record gains one; that write leaves `state` and
    /// `updated_at` as they were.
    pub(super) async fn update_active_execution_pod_metadata(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<(), SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::materialize::record_pod_metadata(
            conn, sandbox_id, generation, pod_uid, pod_ip,
        )
        .await
        .db_context("failed to persist pod metadata")?;
        Ok(())
    }

    /// Lock the active execution row of `generation` and return its current
    /// state, or `None` when no such row exists. `FOR UPDATE` follows the
    /// update chain to the latest committed row version and re-evaluates the
    /// predicate, so a retirement that commits while we wait on the lock
    /// yields `None` here — unlike a plain read, whose statement-start
    /// snapshot could still show the pre-retirement active version.
    pub(super) async fn lock_active_execution_state_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let state = pplx_space_sandbox_store::k8s_executions::transition::lock_active_state(
            &mut *conn, sandbox_id, generation,
        )
        .await
        .db_context("failed to lock active Kubernetes execution row")?;
        state
            .map(|state| {
                state
                    .parse::<ActiveExecutionState>()
                    .map_err(|err| SubstrateError::Failed {
                        message: format!("failed to parse active execution state: {err}"),
                    })
            })
            .transpose()
    }

    pub(super) async fn update_active_execution_node_name(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        node_name: &str,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::materialize::record_node_name(
            conn, sandbox_id, node_name,
        )
        .await
        .db_context("failed to persist sandbox pod node name")
    }

    pub(super) async fn finish_active_execution_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        state: &str,
    ) -> Result<(), SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::retire::finish_active(
            &mut **tx, sandbox_id, state,
        )
        .await
        .db_context("failed to finish active Kubernetes execution")?;
        self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
            .await?;
        Ok(())
    }

    /// An [`ExecutionIdentity`] as SQL binds — the fields the table has a
    /// column for.
    ///
    /// Both guards on this path — the tracking release and the UID backfill —
    /// take their predicate from here rather than each choosing columns, which
    /// is what stops them drifting apart from each other and from
    /// [`ExecutionIdentity::reconcile`]. `resource_version` has no column, so
    /// it is the one field the database cannot enforce; the live-pod half of
    /// the check covers that.
    pub(super) fn identity_binds(
        identity: &ExecutionIdentity,
    ) -> (
        Option<i64>,
        Option<SandboxActivationAttemptId>,
        Option<String>,
        Option<SandboxActivationAttemptId>,
    ) {
        (
            identity.generation,
            identity.activation_attempt_id,
            identity
                .recorded
                .pod_uid()
                .map(|uid| uid.as_str().to_string()),
            identity.recorded.route_activation_id(),
        )
    }

    /// [`Self::finish_active_execution_generation_in`] with the fenced
    /// execution's full identity in the predicate.
    ///
    /// A generation is not by itself a unique execution identity: activation
    /// retries and cross-node redrives reuse one, which is why the attempt-
    /// scoped variant exists at all. So a replacement that took the generation
    /// over after the fenced delete would satisfy a generation-only CAS and be
    /// marked inactive by a teardown that never saw it. Both guards use
    /// `IS NOT DISTINCT FROM` because NULL is a value the caller observed, not
    /// an absence of one — a completed execution's attempt is NULL, and an
    /// unmaterialized pod's UID is too, so treating either as "unconstrained"
    /// would fail open on exactly the states most likely to be replaced.
    pub(super) async fn finish_fenced_execution_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let (generation, activation_attempt_id, pod_uid, route_activation_id) =
            Self::identity_binds(identity);
        let finished = pplx_space_sandbox_store::k8s_executions::retire::finish_fenced(
            &mut **tx,
            sandbox_id,
            pplx_space_sandbox_store::k8s_executions::retire::ExecutionFence {
                generation,
                activation_attempt_id,
                pod_uid: pod_uid.as_deref(),
                route_activation_id,
            },
            state,
        )
        .await
        .db_context("failed to finish fenced Kubernetes execution")?;
        if finished {
            self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
                .await?;
        }
        Ok(finished)
    }

    pub(super) async fn finish_active_execution_generation_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: i64,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let finished = pplx_space_sandbox_store::k8s_executions::retire::finish_generation(
            &mut **tx, sandbox_id, generation, state,
        )
        .await
        .db_context("failed to finish active Kubernetes execution")?;
        if finished {
            self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
                .await?;
        }
        Ok(finished)
    }

    pub(super) async fn finish_active_execution_generation_attempt_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_sandbox_model::SandboxActivationAttemptId,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let finished = pplx_space_sandbox_store::k8s_executions::retire::finish_generation_attempt(
            &mut **tx,
            sandbox_id,
            generation,
            activation_attempt_id,
            state,
        )
        .await
        .db_context("failed to finish activation-fenced Kubernetes execution")?;
        if finished {
            self.invalidate_open_port_exposure_observations_in(tx, sandbox_id)
                .await?;
        }
        Ok(finished)
    }

    async fn invalidate_open_port_exposure_observations_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        pplx_space_sandbox_store::port_exposures::invalidate_open_observations(
            &mut **tx, sandbox_id,
        )
        .await
        .db_context("failed to invalidate sandbox port exposure observations")?;
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn require_sandbox(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Sandbox, SubstrateError> {
        fetch_api_sandbox(&self.pool, sandbox_id)
            .await
            .db_context("failed to load sandbox row")?
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            })
    }

    /// Read only after publishing the execution claim under the sandbox row
    /// lock. A timeout accepted before that claim may have updated the policy
    /// since the activation caller loaded its original sandbox snapshot.
    pub(in crate::controllers::substrate::kubernetes) async fn auto_lifecycle_policy_after_claim(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<pplx_space_auto_lifecycle::AutoLifecyclePolicy, SubstrateError> {
        pplx_space_sandbox_store::auto_lifecycle_policy(&self.pool, sandbox_id)
            .await
            .map_err(sqlx::Error::from)
            .db_context("failed to reload claimed sandbox auto lifecycle policy")?
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            })
    }

    pub(in crate::controllers::substrate::kubernetes) async fn prior_state_exists(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::row_reads::prior_state_exists(&self.pool, sandbox_id)
            .await
            .db_context("failed to query sandbox prior state")
    }

    /// Tracked pod names for generations strictly older than `generation`.
    ///
    /// The unfenced teardown deletes *every* recorded pod name, which is how
    /// stale generations get collected — nothing else reliably does, since a
    /// retired sandbox leaves the failed-cleanup scan. A fenced teardown can't
    /// use that list wholesale: on a race it includes a successor's pod, which
    /// the caller never observed. Bounding it below the fenced generation
    /// keeps the collection and excludes the successor by construction.
    pub(in crate::controllers::substrate::kubernetes) async fn tracked_execution_pod_names_before_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Vec<String>, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::pod_names_before_generation(
            &self.pool, sandbox_id, generation,
        )
        .await
        .db_context("failed to query prior-generation Kubernetes execution pods")
    }

    /// Pod names on *inactive* execution rows.
    ///
    /// No successor can own one: a successor's pod is recorded on the active
    /// row. So this is the set a caller that observed no active execution may
    /// still collect — the orphan left when a create cleanup marked its row
    /// inactive and then failed its Kubernetes delete, which no other pass
    /// retries because they all select active rows.
    pub(in crate::controllers::substrate::kubernetes) async fn inactive_execution_pod_names(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<String>, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::inactive_pod_names(&self.pool, sandbox_id)
            .await
            .db_context("failed to query inactive Kubernetes execution pods")
    }

    /// Return every pod name recorded for the sandbox so sandbox deletion can
    /// issue idempotent deletes for stale generations as well as the active one.
    pub(in crate::controllers::substrate::kubernetes) async fn tracked_execution_pod_names(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<String>, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::pod_names(&self.pool, sandbox_id)
            .await
            .db_context("failed to query Kubernetes execution pods")
    }

    pub(super) async fn lock_sandbox_for_execution_mutation_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        // Multi-row execution mutations lock the sandbox before execution
        // rows so concurrent creation and cleanup cannot cycle.
        pplx_space_sandbox_store::row_locks::lock_id(conn, sandbox_id)
            .await
            .map(|row| row.is_some())
            .db_context("failed to lock sandbox row for Kubernetes execution")
    }

    pub(super) async fn lock_sandbox_execution_lifecycle_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxLifecycle>, SubstrateError> {
        let lifecycle: Option<String> =
            pplx_space_sandbox_store::row_locks::lock_lifecycle(conn, sandbox_id)
                .await
                .db_context("failed to lock sandbox for execution materialization")?;
        lifecycle
            .map(|lifecycle| {
                lifecycle
                    .parse::<SandboxLifecycle>()
                    .map_err(|err| SubstrateError::Failed {
                        message: format!("failed to parse sandbox execution lifecycle: {err}"),
                    })
            })
            .transpose()
    }

    pub(super) async fn lock_sandbox_for_execution_lifecycle_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        expected_lifecycle: SandboxLifecycle,
    ) -> Result<(), SubstrateError> {
        let Some(lifecycle) = Self::lock_sandbox_execution_lifecycle_in(conn, sandbox_id).await?
        else {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        };
        if lifecycle == SandboxLifecycle::Suspending {
            return Err(SubstrateError::suspend_in_flight(sandbox_id));
        }
        if lifecycle != expected_lifecycle {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox cannot materialize a {expected_lifecycle} execution while lifecycle is {lifecycle}"
                ),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn ensure_active_execution_claim(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes execution transaction")?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await?;

        if !sandbox_exists {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }

        let claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, None)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes execution transaction")?;

        Ok(claim)
    }

    /// The generation a claim for `sandbox_id` would mint now, or `None` when
    /// an active execution exists and a claim would reuse it. Read without a
    /// lock: a concurrent claim can mint first, so a caller treats it as a
    /// hint and keeps its post-claim checks.
    pub(in crate::controllers::substrate::kubernetes) async fn unclaimed_execution_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<i64>, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::unclaimed_generation(&self.pool, sandbox_id)
            .await
            .db_context("failed to read the next Kubernetes execution generation")
    }

    pub(in crate::controllers::substrate::kubernetes) async fn ensure_active_execution_claim_for_lifecycle(
        &self,
        sandbox_id: SandboxId,
        expected_lifecycle: SandboxLifecycle,
        restore_lease_token: Option<Uuid>,
        activation_attempt: Option<(
            pplx_space_sandbox_model::SandboxActivationAttemptId,
            SandboxTransitionTargetState,
        )>,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes materialization transaction")?;

        Self::lock_sandbox_for_execution_lifecycle_in(&mut tx, sandbox_id, expected_lifecycle)
            .await?;
        if let Some(restore_lease_token) = restore_lease_token {
            let renewed = pplx_space_sandbox_store::wake::renew_restore_lease_for_execution_claim(
                &mut *tx,
                sandbox_id,
                restore_lease_token,
                super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS,
            )
            .await
            .db_context("failed to renew restore materialization ownership")?;
            if !renewed {
                let lease_remaining_secs =
                    pplx_space_sandbox_store::row_reads::restore_lease_remaining_secs(
                        &mut *tx, sandbox_id,
                    )
                    .await
                    .db_context("failed to query replacement restore lease")?;
                return Err(Self::restore_ownership_collision(
                    sandbox_id,
                    lease_remaining_secs,
                ));
            }
        }
        let claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, activation_attempt)
            .await?;
        if let Some(restore_lease_token) = restore_lease_token {
            self.fence_unsent_user_restore_in(&mut tx, sandbox_id, restore_lease_token)
                .await?;
        }

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes materialization transaction")?;

        record_route_identity_decision(&claim, false);

        Ok(claim)
    }

    pub(super) async fn ensure_active_execution_claim_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        activation_attempt: Option<(
            pplx_space_sandbox_model::SandboxActivationAttemptId,
            SandboxTransitionTargetState,
        )>,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut claim = match self.active_execution_row(&mut *tx, sandbox_uuid).await? {
            Some(row) => SandboxExecutionClaim {
                row,
                created: false,
                route_publication: None,
            },
            None => {
                self.insert_active_execution_claim_in(&mut *tx, sandbox_id)
                    .await?
            }
        };

        if let Some((activation_attempt_id, target_state)) = activation_attempt {
            let route_publication =
                classify_activation_route_publication(&claim.row, claim.created)?;
            let activation_attempt_id = Self::assign_activation_attempt_in(
                tx,
                sandbox_id,
                claim.row.generation,
                activation_attempt_id,
                target_state,
                route_publication,
            )
            .await?;
            claim.row.activation_attempt_id = Some(activation_attempt_id);
            if route_publication == ActivationRoutePublication::InitializeFirst {
                claim.row.route_activation_id = Some(activation_attempt_id);
            }
            claim.route_publication = Some(route_publication);
        }

        Ok(claim)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn replace_active_execution_claim_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        state: &str,
        activation_attempt: Option<(SandboxActivationAttemptId, SandboxTransitionTargetState)>,
    ) -> Result<Option<SandboxExecutionClaim>, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes execution transaction")?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await?;

        if !sandbox_exists {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }

        let recovery_in_flight = pplx_space_sandbox_store::k8s_executions::active_in_state(
            &mut *tx,
            sandbox_uuid,
            generation,
            ActiveExecutionState::Recovering.as_str(),
        )
        .await
        .db_context("failed to inspect replaced Kubernetes execution state")?
        .unwrap_or(false);

        let replaced = self
            .finish_active_execution_generation_in(&mut tx, sandbox_uuid, generation, state)
            .await?;
        if !replaced {
            tx.commit()
                .await
                .db_context("failed to commit Kubernetes execution transaction")?;
            return Ok(None);
        }

        let mut claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, activation_attempt)
            .await?;
        if recovery_in_flight {
            let marked =
                pplx_space_sandbox_store::k8s_executions::transition::compare_and_set_active_state(
                    &mut *tx,
                    sandbox_uuid,
                    claim.row.generation,
                    ActiveExecutionState::Recovering.as_str(),
                    ActiveExecutionState::Creating.as_str(),
                )
                .await
                .db_context("failed to preserve recovery on replacement execution")?;
            if marked != 1 {
                return Err(SubstrateError::Failed {
                    message: "failed to preserve recovery on replacement execution".to_string(),
                });
            }
            claim.row.state = ActiveExecutionState::Recovering;
        }
        tx.commit()
            .await
            .db_context("failed to commit Kubernetes execution transaction")?;

        record_route_identity_decision(&claim, true);

        Ok(Some(claim))
    }

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn ensure_active_execution_row(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionRow, SubstrateError> {
        let claim = self.ensure_active_execution_claim(sandbox_id).await?;
        let row = claim.row;
        Ok(row)
    }

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn finish_active_execution_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start active execution transaction")?;
        let finished = self
            .finish_active_execution_generation_in(&mut tx, sandbox_id, generation, state)
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit active execution transaction")?;
        Ok(finished)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn abandon_created_unmaterialized_create_claim(
        &self,
        sandbox_id: SandboxId,
        claim: &SandboxExecutionClaim,
    ) -> Result<bool, SubstrateError> {
        if !claim.created {
            return Ok(false);
        }
        self.abandon_unmaterialized_create_claim(sandbox_id, claim)
            .await
    }

    /// Retire an unmaterialized `creating` claim regardless of whether this
    /// request created it. Used by the cross-node re-drive failure path: the
    /// pinned pod has already been deleted, so a pre-existing `creating` claim
    /// (`claim.created == false` on a retry) no longer has anything in flight,
    /// and leaving it would send every subsequent ensure call down the
    /// create-in-flight early return until the stale-create age expires. The
    /// UPDATE CASes on `creating + pod_uid IS NULL + pod_ip IS NULL`, so a row
    /// a concurrent attempt already materialized is never touched.
    pub(in crate::controllers::substrate::kubernetes) async fn abandon_unmaterialized_create_claim(
        &self,
        sandbox_id: SandboxId,
        claim: &SandboxExecutionClaim,
    ) -> Result<bool, SubstrateError> {
        // The attempt is always fenced. The restore lease is intentionally
        // unfenced (`None`) here because these in-request handlers run while
        // their owner still holds it.
        self.abandon_unmaterialized_create_claim_by_generation(
            sandbox_id,
            claim.row.generation,
            None,
            claim.row.activation_attempt_id,
        )
        .await
    }

    /// [`Self::abandon_unmaterialized_create_claim`] keyed by generation
    /// directly — for the re-drive cancellation guard, which fires from a
    /// `Drop` impl and cannot borrow the claim.
    ///
    /// The activation-attempt identity fences retries that reuse a generation.
    /// `restore_lease_token` additionally ownership-fences restore takeover
    /// (with the same `None`-is-unguarded contract as
    /// [`Self::revert_restore_claim_to_suspended`]). A dropped leader
    /// can outlive its lease; once a follower adopts the still-`creating`
    /// generation, either changed fence makes the delayed retirement a no-op.
    ///
    /// The token is read after locking the sandbox row `FOR UPDATE`, following
    /// the sandbox -> execution lock order the rest of this store uses. The
    /// lease takeover in [`Self::begin_restore_lifecycle`] locks this same row
    /// `FOR UPDATE` before overwriting `restore_lease_token`, so taking the lock
    /// here serializes A's cleanup against B's takeover: either A reads the row
    /// first and retires its own still-`creating` generation, or A blocks until
    /// B commits, then reads B's token and no-ops. An unlocked read (the token
    /// as a subquery inside the retirement UPDATE) instead lets A retire B's
    /// adopted claim on a stale READ COMMITTED snapshot while B's own
    /// materialization races on a different row and affects zero rows.
    pub(in crate::controllers::substrate::kubernetes) async fn abandon_unmaterialized_create_claim_by_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        restore_lease_token: Option<Uuid>,
        activation_attempt_id: Option<pplx_space_sandbox_model::SandboxActivationAttemptId>,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes create claim transaction")?;
        if !Self::locked_lease_owner_matches(&mut tx, sandbox_id, restore_lease_token).await? {
            tx.commit()
                .await
                .db_context("failed to commit Kubernetes create claim transaction")?;
            return Ok(false);
        }
        let abandoned =
            pplx_space_sandbox_store::k8s_executions::retire::abandon_unmaterialized_create_claim(
                &mut *tx,
                sandbox_id,
                generation,
                activation_attempt_id,
            )
            .await
            .db_context("failed to abandon Kubernetes create claim")?;
        if abandoned {
            self.invalidate_open_port_exposure_observations_in(&mut tx, sandbox_id)
                .await?;
        }
        tx.commit()
            .await
            .db_context("failed to commit Kubernetes create claim transaction")?;
        Ok(abandoned)
    }

    /// Lock the sandbox row `FOR UPDATE` and report whether `restore_lease_token`
    /// still owns the restore lease. `None` is the unfenced in-request contract
    /// (owner alive, lease held) and always matches. Locking here — instead of
    /// reading the token in an unlocked subquery — serializes the re-drive
    /// cancellation cleanup against the `FOR UPDATE` lease takeover in
    /// [`Self::begin_restore_lifecycle`], following the sandbox -> execution
    /// lock order: a lapsed leader either wins the lock and cleans its own
    /// generation, or blocks until the follower commits and then reads the
    /// follower's token and no-ops.
    async fn locked_lease_owner_matches(
        tx: &mut PgConnection,
        sandbox_id: SandboxId,
        restore_lease_token: Option<Uuid>,
    ) -> Result<bool, SubstrateError> {
        let current_token =
            pplx_space_sandbox_store::restore_lease::lock_restore_lease_token(&mut *tx, sandbox_id)
                .await
                .db_context("failed to lock sandbox for create claim cleanup")?
                .flatten();
        Ok(match restore_lease_token {
            Some(expected) => current_token == Some(expected),
            None => true,
        })
    }

    /// Reset an active generation's pod tracking back to unmaterialized
    /// (`creating`, null UID/IP) after the cross-node re-drive confirmed the
    /// previously created source-pinned pod is deleted. That pod's UID is
    /// persisted before the scheduler's verdict is known, so an `Unschedulable`
    /// pin leaves the row `created` with `pod_ip` NULL even though the pod is
    /// now gone. Left as-is, the re-drive cancellation guard's `creating`-scoped
    /// retirement can't match, so a client disconnect mid-re-drive would strand
    /// the cross-node successor as an orphan VM once the transient-lifecycle
    /// sweeper reverts the sandbox to a dormant lifecycle. Resetting to
    /// `creating` restores the invariant the guard and the re-drive failure
    /// handler both rely on.
    ///
    /// Attempt- and lease-fenced (same lease contract as
    /// [`Self::abandon_unmaterialized_create_claim_by_generation`]) and scoped
    /// to a not-yet-routable row (`pod_ip IS NULL`), so a successor another
    /// attempt already materialized to a running pod is never clobbered.
    pub(in crate::controllers::substrate::kubernetes) async fn reset_active_execution_to_creating_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        restore_lease_token: Option<Uuid>,
        activation_attempt_id: Option<pplx_space_sandbox_model::SandboxActivationAttemptId>,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start create claim reset transaction")?;
        if !Self::locked_lease_owner_matches(&mut tx, sandbox_id, restore_lease_token).await? {
            tx.commit()
                .await
                .db_context("failed to commit create claim reset transaction")?;
            return Ok(false);
        }
        // Clearing the pod UID stamps `pod_materialized_at` first if the row
        // held one, so the record that a pod ran outlives the UID.
        let reset = pplx_space_sandbox_store::k8s_executions::claim::reset_to_creating(
            &mut *tx,
            sandbox_id,
            generation,
            activation_attempt_id,
        )
        .await
        .db_context("failed to reset create claim to creating")?;
        tx.commit()
            .await
            .db_context("failed to commit create claim reset transaction")?;
        Ok(reset)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn active_execution_is_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::active_is_generation(
            &self.pool, sandbox_id, generation,
        )
        .await
        .db_context("failed to check active Kubernetes execution generation")
    }

    pub(in crate::controllers::substrate::kubernetes) async fn active_execution_status(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<ActiveExecutionStatusRow>, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::active_execution_status(
            &self.pool,
            sandbox_id,
            super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS,
        )
        .await
        .and_then(|status| status.map(ActiveExecutionStatusRow::try_from).transpose())
        .db_context("failed to query pod metadata")
    }
}
