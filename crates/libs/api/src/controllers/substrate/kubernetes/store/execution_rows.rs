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
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active,
                 terminated_at
             )
             SELECT $1, 1, $2, $3, 'replaced', FALSE, NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1
             )
             ON CONFLICT DO NOTHING"
        ))
        .bind(sandbox_id)
        .bind(&pod_name)
        .bind(&self.namespace)
        .execute(&self.pool)
        .await
        .db_context("failed to retire the pool-reserved execution generation")?;
        Ok(())
    }

    pub(super) async fn active_execution_row(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxExecutionRow>, SubstrateError> {
        sqlx::query_as::<_, SandboxExecutionRow>(sql!(
            "SELECT generation, pod_name,
                    NULLIF(BTRIM(pod_uid), '') IS NOT NULL AS pod_materialized,
                    state,
                    updated_at <= NOW() - make_interval(secs => $2) AS create_age_expired,
                    activation_attempt_id, route_activation_id
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to query active Kubernetes execution row")
    }

    pub(super) async fn insert_active_execution_claim_in(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<SandboxExecutionClaim, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let generation = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COALESCE(MAX(generation), 0) + 1
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox_uuid)
        .fetch_one(&mut *conn)
        .await
        .db_context("failed to allocate Kubernetes execution generation")?;
        let pod_name = Self::execution_pod_name(sandbox_uuid, generation);

        let row = sqlx::query_as::<_, SandboxExecutionRow>(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active
             )
             VALUES ($1, $2, $3, $4, 'creating', TRUE)
             ON CONFLICT DO NOTHING
             RETURNING generation, pod_name, FALSE AS pod_materialized, state,
                       FALSE AS create_age_expired, activation_attempt_id,
                       route_activation_id"
        ))
        .bind(sandbox_uuid)
        .bind(generation)
        .bind(&pod_name)
        .bind(&self.namespace)
        .fetch_optional(&mut *conn)
        .await
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
    /// (`lock_active_execution_state_in`) — the guarded UPDATE alone cannot
    /// distinguish "row already current" from "row retired underneath".
    pub(super) async fn update_active_execution_pod_metadata(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = $1::TEXT,
                 pod_ip = $2::INET,
                 state = CASE
                     WHEN state = 'recovering' THEN state
                     WHEN $2::TEXT IS NOT NULL THEN 'running'
                     WHEN $1::TEXT IS NOT NULL THEN 'created'
                     ELSE state
                 END,
                 updated_at = CASE WHEN state = 'recovering' THEN updated_at ELSE NOW() END
             WHERE sandbox_id = $3 AND generation = $4 AND active
               AND (
                 pod_uid IS DISTINCT FROM $1::TEXT
                 OR pod_ip IS DISTINCT FROM $2::INET
                 OR (
                     state NOT IN ('running', 'recovering')
                     AND (
                         $2::TEXT IS NOT NULL
                         OR ($1::TEXT IS NOT NULL AND state <> 'created')
                     )
                 )
               )"
        ))
        .bind(pod_uid)
        .bind(pod_ip)
        .bind(sandbox_id)
        .bind(generation)
        .execute(conn)
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
        let state: Option<String> = sqlx::query_scalar(sql!(
            "SELECT state
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = $2 AND active
             FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_optional(&mut *conn)
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
    ) -> Result<ActiveExecutionNodeNameWrite, SubstrateError> {
        sqlx::query_as::<_, ActiveExecutionNodeNameWrite>(sql!(
            "WITH updated AS (
                UPDATE sandbox_k8s_executions
                SET k8s_node_name = $1::TEXT,
                    updated_at = CASE WHEN state = 'recovering' THEN updated_at ELSE NOW() END
                WHERE sandbox_id = $2 AND active
                  AND k8s_node_name IS DISTINCT FROM $1::TEXT
                RETURNING 1
             )
             SELECT EXISTS (
                SELECT 1
                FROM sandbox_k8s_executions
                WHERE sandbox_id = $2 AND active
             ) AS active_execution,
             EXISTS (SELECT 1 FROM updated) AS node_name_changed"
        ))
        .bind(node_name)
        .bind(sandbox_id)
        .fetch_one(conn)
        .await
        .db_context("failed to persist sandbox pod node name")
    }

    pub(super) async fn finish_active_execution_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        state: &str,
    ) -> Result<(), SubstrateError> {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2 AND active"
        ))
        .bind(state)
        .bind(sandbox_id)
        .execute(&mut **tx)
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
        let finished = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2
               AND active
               AND generation IS NOT DISTINCT FROM $3
               AND activation_attempt_id IS NOT DISTINCT FROM $4
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $5::TEXT
               AND route_activation_id IS NOT DISTINCT FROM $6"
        ))
        .bind(state)
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(pod_uid)
        .bind(route_activation_id)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish fenced Kubernetes execution")?
        .rows_affected()
            > 0;
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
        let finished = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2 AND active AND generation = $3"
        ))
        .bind(state)
        .bind(sandbox_id)
        .bind(generation)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish active Kubernetes execution")?
        .rows_affected()
            > 0;
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
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        state: &str,
    ) -> Result<bool, SubstrateError> {
        let finished = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2
               AND active
               AND generation = $3
               AND activation_attempt_id = $4"
        ))
        .bind(state)
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&mut **tx)
        .await
        .db_context("failed to finish activation-fenced Kubernetes execution")?
        .rows_affected()
            > 0;
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
        sqlx::query(sql!(
            "UPDATE sandbox_port_exposures
             SET apply_state = 'pending',
                 last_applied_node_id = NULL,
                 last_error = NULL,
                 last_bridge_status = NULL,
                 last_applied_bridge_generation = NULL,
                 last_observed_at = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND desired_state = 'open'"
        ))
        .bind(sandbox_id)
        .execute(&mut **tx)
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
    ) -> Result<pplx_space_common_core::types::AutoLifecyclePolicy, SubstrateError> {
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
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                  WHERE s.id = $1
                    AND (
                      s.current_local_artifact_id IS NOT NULL
                      OR EXISTS (
                          SELECT 1
                            FROM snapshots sn
                           WHERE sn.sandbox_id = s.id
                             AND sn.type = 'full'
                             AND sn.state IN ('creating', 'ready')
                      )
                    )
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_one(&self.pool)
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
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT pod_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation < $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .fetch_all(&self.pool)
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
        sqlx::query_scalar::<_, String>(sql!(
            "SELECT pod_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND NOT active"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .db_context("failed to query inactive Kubernetes execution pods")
    }

    /// Return every pod name recorded for the sandbox so sandbox deletion can
    /// issue idempotent deletes for stale generations as well as the active one.
    pub(in crate::controllers::substrate::kubernetes) async fn tracked_execution_pod_names(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<String>, SubstrateError> {
        sqlx::query_scalar::<_, String>(
            "SELECT pod_name FROM sandbox_k8s_executions WHERE sandbox_id = $1",
        )
        .bind(sandbox_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .db_context("failed to query Kubernetes execution pods")
    }

    pub(super) async fn lock_sandbox_for_execution_mutation_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        // Multi-row execution mutations lock the sandbox before execution
        // rows so concurrent creation and cleanup cannot cycle.
        sqlx::query_scalar::<_, Uuid>(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .fetch_optional(conn)
            .await
            .map(|row| row.is_some())
            .db_context("failed to lock sandbox row for Kubernetes execution")
    }

    pub(super) async fn lock_sandbox_execution_lifecycle_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxLifecycle>, SubstrateError> {
        let lifecycle: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(conn)
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

    pub(in crate::controllers::substrate::kubernetes) async fn ensure_active_execution_claim_for_lifecycle(
        &self,
        sandbox_id: SandboxId,
        expected_lifecycle: SandboxLifecycle,
        restore_lease_token: Option<Uuid>,
        activation_attempt: Option<(
            pplx_space_common_core::types::SandboxActivationAttemptId,
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
            let renewed = sqlx::query_scalar::<_, bool>(sql!(
                "UPDATE sandboxes
                    SET restore_lease_expires_at =
                        NOW() + make_interval(secs => $3)
                  WHERE id = $1
                    AND restore_lease_token = $2
                    AND restore_lease_expires_at > NOW()
                RETURNING TRUE"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(restore_lease_token)
            .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
            .fetch_optional(&mut *tx)
            .await
            .db_context("failed to renew restore materialization ownership")?;
            if renewed.is_none() {
                let lease_remaining_secs = sqlx::query_scalar::<_, f64>(sql!(
                    "SELECT GREATEST(
                                EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                                0.0
                            )::float8
                       FROM sandboxes
                      WHERE id = $1"
                ))
                .bind(sandbox_id.as_uuid())
                .fetch_one(&mut *tx)
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
            pplx_space_common_core::types::SandboxActivationAttemptId,
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

        let recovery_in_flight = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT state = $3
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = $2 AND active"
        ))
        .bind(sandbox_uuid)
        .bind(generation)
        .bind(ActiveExecutionState::Recovering.as_str())
        .fetch_optional(&mut *tx)
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
            let marked = sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                 SET state = $3, updated_at = NOW()
                 WHERE sandbox_id = $1 AND generation = $2 AND active AND state = $4"
            ))
            .bind(sandbox_uuid)
            .bind(claim.row.generation)
            .bind(ActiveExecutionState::Recovering.as_str())
            .bind(ActiveExecutionState::Creating.as_str())
            .execute(&mut *tx)
            .await
            .db_context("failed to preserve recovery on replacement execution")?
            .rows_affected();
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
    /// [`Self::revert_lifecycle_after_rollback_best_effort`]). A dropped leader
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
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
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
        let abandoned = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = 'replaced',
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND state = 'creating'
               AND pod_uid IS NULL
               AND pod_ip IS NULL"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&mut *tx)
        .await
        .db_context("failed to abandon Kubernetes create claim")?
        .rows_affected()
            > 0;
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
        let current_token = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
            "SELECT restore_lease_token FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut *tx)
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
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
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
        let reset = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = NULL,
                 pod_ip = NULL,
                 state = 'creating',
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND pod_ip IS NULL
               AND state IN ('creating', 'created')"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&mut *tx)
        .await
        .db_context("failed to reset create claim to creating")?
        .rows_affected()
            > 0;
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
        sqlx::query_scalar(sql!(
            "SELECT EXISTS(
                 SELECT 1
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1
                    AND generation = $2
                    AND active
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .fetch_one(&self.pool)
        .await
        .db_context("failed to check active Kubernetes execution generation")
    }

    pub(in crate::controllers::substrate::kubernetes) async fn active_execution_status(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<ActiveExecutionStatusRow>, SubstrateError> {
        sqlx::query_as(sql!(
            "SELECT e.generation, e.pod_name, NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                    host(e.pod_ip) AS pod_ip, s.active_node_id AS current_node_id,
                    e.k8s_node_name, e.state, s.lifecycle,
                    e.updated_at <= NOW() - make_interval(secs => $2) AS create_age_expired,
                    e.activation_attempt_id,
                    e.route_activation_id
             FROM sandbox_k8s_executions e
             JOIN sandboxes s ON s.id = e.sandbox_id
             WHERE e.sandbox_id = $1 AND e.active"
        ))
        .bind(sandbox_id)
        .bind(super::super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to query pod metadata")
    }
}
