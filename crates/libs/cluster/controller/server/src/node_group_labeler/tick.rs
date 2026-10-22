//! The tick body of [`super::Reconciler`]: the per-tick orchestration
//! ([`Reconciler::tick`]), the single-node label patch ([`label_one_node`]),
//! the cross-tick write rotation ([`FairnessCursor`]) and the per-tick log
//! rationing ([`claim_log_budget`]).
//!
//! Split from `mod.rs`, which keeps the module's public surface: the config, the
//! two seams, [`TickReport`], the error enums, the constants these read, and
//! [`super::run`].

use super::*;

/// Take one slot from this tick's per-node failure-log budget. Returns whether
/// the caller may log; once the budget is spent every caller is silenced and
/// the aggregate counters carry the rest.
pub(super) fn claim_log_budget(budget: &std::sync::atomic::AtomicUsize) -> bool {
    budget
        .try_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |remaining| remaining.checked_sub(1),
        )
        .is_ok()
}

/// Patch one node's default-group label, bounded by both
/// [`LABEL_WRITE_TIMEOUT`] and whatever is left of the tick, not issued at all
/// once the shutdown token is cancelled, and re-read from the store immediately
/// beforehand so the patch's precondition is current.
///
/// Both bounds are re-read per node rather than checked once for the phase. For
/// the deadline: a write starting just before it with the full per-write timeout
/// would overshoot, so a slow-then-wedged pair would cost
/// `patch + LABEL_WRITE_TIMEOUT`. For the token: cancellation lands mid-phase,
/// and this check is what turns the rest of the fan-out into no-ops so the phase
/// drains after at most the patches already in flight.
async fn label_one_node<L: WorkerNodeLabels>(
    labels: &L,
    node: &WorkerNode,
    group_name: &str,
    action_deadline: tokio::time::Instant,
    shutdown_token: &CancellationToken,
    failure_log_budget: &std::sync::atomic::AtomicUsize,
) -> LabelOutcome {
    // Neither early-out is logged per node: past the deadline (or the
    // cancellation) `buffer_unordered` still admits and polls every remaining
    // future, so a large backlog against a slow apiserver would emit one line
    // per unlabeled node every tick. The aggregates are `label_deferred` and
    // `label_deferred_shutdown`, and a node never contacted carries no
    // diagnostic the counts don't.
    let remaining = action_deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return LabelOutcome::NotAttempted(DeferReason::BudgetSpent);
    }
    // Budget checked first: a node the phase had already run out of time for was
    // lost to the budget whether or not a shutdown followed, so booking it as a
    // shutdown deferral would overstate what the wind-down cost.
    if shutdown_token.is_cancelled() {
        return LabelOutcome::NotAttempted(DeferReason::Shutdown);
    }

    // Re-read the node as of now, and patch *that* version. The snapshot this
    // node came from is up to a whole `config.tick` old, and its resourceVersion
    // is a precondition: without the refresh, any unrelated write to the Node in
    // that window fails the patch with a 409 — and there is a fleet-wide writer
    // of Node labels (node-ctl's template-presence labeler, resident on every
    // worker). Those 409s land in `label_write{outcome:error}` alongside a real
    // RBAC 403, where they cannot be told apart. Refreshing keeps the
    // precondition meaning what it is for: someone else assigned a *group*.
    //
    // A store read, not an apiserver call — cheap enough to do per node, and it
    // is deliberately outside the timeout below, which bounds the write.
    let Some(fresh) = labels.current_worker_node(node).await else {
        tracing::debug!(
            node = %node.name,
            "node_group_labeler: node left the worker set between the snapshot and \
             its patch; skipping"
        );
        return LabelOutcome::NotAttempted(DeferReason::Superseded);
    };
    // The never-overwrite invariant, restated against the fresh read. Before the
    // refresh the stale precondition enforced this as a side effect (any group
    // assignment bumps the resourceVersion); now that the patch carries a current
    // version, the check has to be explicit. Same for the dedicated taint, which
    // is applied control-plane-side and can land in this very window.
    if fresh.node_group.is_some() || fresh.dedicated {
        tracing::debug!(
            node = %node.name,
            group = ?fresh.node_group,
            dedicated = fresh.dedicated,
            "node_group_labeler: node acquired a node-group label or the dedicated \
             taint between the snapshot and its patch; leaving it alone"
        );
        return LabelOutcome::NotAttempted(DeferReason::Superseded);
    }

    // Whichever bound is tighter is the one that actually fires, so it is the
    // one to report — logging the constant when the tick budget was the
    // binding constraint describes a per-write apiserver timeout that didn't
    // happen.
    let budget = LABEL_WRITE_TIMEOUT.min(remaining);
    let truncated_by_tick = budget < LABEL_WRITE_TIMEOUT;
    match tokio::time::timeout(budget, labels.label_node(&fresh, group_name.to_string())).await {
        Err(_) if truncated_by_tick => {
            // Deliberately NOT drawing on `failure_log_budget`: this is `info`,
            // it is not routed to Datadog, and it becomes the common case on a
            // tick shorter than LABEL_WRITE_TIMEOUT — so letting it claim slots
            // would starve the warn lines carrying a real failure's cause, the
            // same starvation DEDICATED_MISLABEL_LOGGED_NODES was split out to
            // prevent. Bounded instead by the phase: at most one line per node
            // the tick actually reached.
            // Cut short by the tick budget, not by apiserver slowness — so not
            // an error rate. Indeterminate rather than deferred because the
            // request may already have gone out (with a sub-millisecond budget
            // it may not have; the classification errs toward "may have landed").
            {
                tracing::info!(
                    node = %node.name,
                    budget_ms = budget.as_millis(),
                    "node_group_labeler: label write cut short by the tick budget; \
                     outcome unknown, re-reading next tick"
                );
            }
            LabelOutcome::Indeterminate
        }
        Err(_) => {
            if claim_log_budget(failure_log_budget) {
                tracing::warn!(
                    node = %node.name,
                    timeout_secs = LABEL_WRITE_TIMEOUT.as_secs(),
                    "node_group_labeler: label patch timed out; outcome unknown, \
                     re-reading next tick"
                );
            }
            LabelOutcome::Indeterminate
        }
        Ok(Err(err)) => {
            // Includes the resourceVersion-conflict case, now narrowed to a
            // concurrent actor changing the Node between the re-read just above
            // and this patch. Retry next tick — the fresh list re-observes
            // whatever label landed.
            if claim_log_budget(failure_log_budget) {
                tracing::warn!(
                    node = %node.name,
                    error = %err,
                    "node_group_labeler: label patch failed; will retry next tick"
                );
            }
            LabelOutcome::Failed
        }
        Ok(Ok(())) => {
            tracing::info!(
                node = %node.name,
                group = %group_name,
                "node_group_labeler: labeled worker into default node group"
            );
            LabelOutcome::Labeled(node.name.clone())
        }
    }
}

/// Rotation offset that makes the label phase fair across ticks.
///
/// The tick budget caps how many patches are *attempted*, so a tick can leave a
/// tail unvisited. Without rotation each tick would restart from whatever the
/// snapshot hands back first, and a persistently-failing prefix would be retried
/// forever while the tail was never attempted — an unlabeled worker serves no
/// sandboxes once required affinity ships, so the rollout would plateau while
/// `label_deferred` looked like ordinary back-pressure. Same fix as
/// `node_service_readiness`'s `FairnessCursor`.
///
/// Progress, not an exact hand-off: the offset advances by the patches the tick
/// issued, so a tick that got through a prefix starts past it rather than
/// replaying it. It is *not* true that the window moves on every tick with work
/// to do — the advance is `labeled + errors + indeterminate`, and two reachable
/// ticks issue none of those with a non-empty pending list. Neither costs
/// coverage. Every candidate `Superseded` (each pre-write re-read contradicts
/// the snapshot) advances by zero, and those nodes are gone from the next
/// tick's snapshot anyway, so there is no prefix left to skip. A tick cancelled
/// by the shutdown token likewise advances by zero, and it is the last tick this
/// process runs — the offset dies with it and the next leader starts from its
/// own zero. Full coverage in `ceil(n / attempted)` ticks needs a stable
/// enumeration, which the reflector store gives only while membership is
/// unchanged; across join/leave churn the rotation is a fairness heuristic, not
/// a guarantee.
#[derive(Debug, Default)]
struct FairnessCursor {
    offset: std::sync::atomic::AtomicUsize,
}

impl FairnessCursor {
    /// Rotate the pending list so this tick begins where the last one
    /// stopped.
    fn rotate<T>(&self, pending: &mut [T]) {
        if pending.is_empty() {
            return;
        }
        let offset = self.offset.load(std::sync::atomic::Ordering::Relaxed) % pending.len();
        pending.rotate_left(offset);
    }

    /// Advance past the patches this tick actually attempted. Wrapping is
    /// fine — [`Self::rotate`] takes the offset modulo the current list
    /// length, so it never has to track a changing fleet size.
    fn advance(&self, attempted: usize) {
        self.offset
            .fetch_add(attempted, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct Reconciler<S: NodeGroupSink, L: WorkerNodeLabels> {
    config: ReconcilerConfig,
    sink: S,
    labels: L,
    label_cursor: FairnessCursor,
    /// Node uids the gate refused to label as of the last tick, used to decide
    /// when to re-report the suppressed state at error level.
    ///
    /// Suppression is a standing condition, so logging it every tick would turn
    /// one misordered rollout into a permanent error-level alert — but staying
    /// quiet is wrong too, since every worker joining after the gate closes is
    /// one that will never be labeled. So the signal is "did any node *newly*
    /// enter the set", keyed on membership rather than count: a 1:1 ASG
    /// replacement keeps the count flat while still adding an unlabeled node.
    /// It does not buy quiet (cardinality-growth is a subset of
    /// membership-change, so this fires at least as often); the gauge is what to
    /// alert on and this log is the breadcrumb. Cleared while the gate is open.
    ///
    /// **Process-local, so the quiet is best-effort.** [`run`] constructs the
    /// `Reconciler` per process, so a restart or a leader hand-off starts from
    /// an empty set and replays the whole standing condition at `error` on the
    /// first tick after it. Leader election is enabled, so a flapping lease
    /// re-emits every suppression line on each acquisition. Durable
    /// de-duplication would need the baseline in Postgres, which is not worth a
    /// table for a breadcrumb whose alertable form is a gauge.
    suppressed_last_tick: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Worker uids seen carrying a label naming a nonexistent group as of the
    /// last tick, for the same reason as [`Self::suppressed_last_tick`]: this is
    /// a standing condition nothing reconciles away, so warning on every tick
    /// turns one mislabeled node into 2,880 Datadog lines a day. Membership, not
    /// count, so a 1:1 replacement still reports. `foreign_group_nodes` is the
    /// gauge to alert on; this log names the nodes.
    ///
    /// **Process-local, so the quiet is best-effort**, for the same reason and
    /// with the same consequence as [`Self::suppressed_last_tick`]: a restart or
    /// a leader hand-off replays every foreign-group line at `warn`.
    foreign_group_last_tick: std::sync::Mutex<std::collections::HashSet<String>>,
    /// The service shutdown token, the same one [`run`]'s `select!` waits on.
    ///
    /// A field rather than a [`Self::tick`] parameter: `tick` has one production
    /// caller and dozens of test call sites, and threading it through every one
    /// of them would say nothing at any of them.
    ///
    /// The tick *observes* it; it is never dropped on it. `run`'s `select!`
    /// keeps the loop from starting another tick, and this keeps the in-flight
    /// one from starting further patches — while still reaching the post-write
    /// census re-check, which is the only thing that makes a gate race
    /// auditable, and the tick's normal return, which is what the metrics and
    /// the summary log are published from.
    shutdown_token: CancellationToken,
}

impl<S: NodeGroupSink, L: WorkerNodeLabels> Reconciler<S, L> {
    pub fn new(
        config: ReconcilerConfig,
        sink: S,
        labels: L,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            config,
            sink,
            labels,
            label_cursor: FairnessCursor::default(),
            suppressed_last_tick: std::sync::Mutex::new(std::collections::HashSet::new()),
            foreign_group_last_tick: std::sync::Mutex::new(std::collections::HashSet::new()),
            shutdown_token,
        }
    }

    pub async fn tick(&self) -> TickReport {
        let mut report = TickReport::default();

        let ensured = match self
            .sink
            .ensure_default_group(&self.config.default_group_name)
            .await
        {
            Ok(ensured) => ensured,
            // Both of these need an operator decision and will not clear on
            // their own, so they must not be reported as the routine "schema
            // isn't deployed here yet" case.
            Err(
                err @ (EnsureDefaultGroupError::NameConflict { .. }
                | EnsureDefaultGroupError::InvalidName { .. }
                | EnsureDefaultGroupError::SeedBlockedByExistingGroups { .. }),
            ) => {
                tracing::error!(
                    error = %err,
                    "node_group_labeler: the default node group cannot be resolved to \
                     a usable name; retrying will not clear this — rename via \
                     the default node-group name, promote the existing \
                     group to default, or fix the stored row"
                );
                report.seed_blocked_errors += 1;
                return report;
            }
            Err(err) => {
                // Three causes share this arm and only the error string above
                // separates them, so the line names all three rather than
                // asserting the first: booking one `tick_error{kind:seed}`
                // per cause would be a metric-surface change, and the operator
                // action is the same "look, then decide" either way.
                tracing::warn!(
                    error = %err,
                    "node_group_labeler: ensure default group failed; will retry. \
                     Any of three things: the node-groups migration is not applied in \
                     this environment (the first-enablement case, and the only one \
                     that clears on its own); the seeding path's 5s lock_timeout — or \
                     a server statement_timeout — expired waiting on `node_groups`, \
                     which happens only while no default row exists yet and \
                     correlates with another writer of that table; or an ordinary \
                     connection/pool failure. A standing count is not proof the \
                     migration is missing — confirm it is absent before treating it \
                     as the cause"
                );
                report.ensure_errors += 1;
                return report;
            }
        };
        // The one `node_groups` write that does not come from the admin API,
        // so the only one that has to emit its own audit entry. Gated on
        // `Seeded` rather than emitted unconditionally: `Existing` is what every
        // steady-state tick returns, and auditing a read would put an event on
        // `space.admin.audit` every `config.tick` for the life of the cluster.
        if let EnsuredDefaultGroup::Seeded(seeded) = &ensured {
            record_node_group_system(SystemNodeGroupAuditEntry {
                actor: SYSTEM_ACTOR_NODE_GROUP_LABELER,
                action: SEED_DEFAULT_AUDIT_ACTION,
                node_group_id: seeded.id,
                node_group_name: Some(seeded.name.as_str()),
                // Nothing existed to transition from — this is a create, and
                // `node-group-create` leaves `from_state` unset too.
                from_state: None,
                // Rendered by the admin surface's own formatter, so an
                // operator-created group and this one cannot drift apart in
                // the trail. The three arguments are constants rather than
                // read back from the row because the seeded shape is
                // `NodeGroupSink::ensure_default_group`'s contract — no org,
                // `default` policy, `is_default` — not a per-call value.
                to_state: Some(&group_state_summary(
                    None,
                    NodeGroupTemplatePolicy::Default,
                    true,
                )),
                success: true,
                error: None,
            });
        }
        let group = ensured.into_group();
        if group.name != self.config.default_group_name {
            tracing::debug!(
                stored = %group.name,
                configured = %self.config.default_group_name,
                "node_group_labeler: existing default group name differs from \
                 configured seed name; the stored name wins"
            );
        }

        let nodes = match self.labels.list_worker_nodes().await {
            Ok(nodes) => nodes,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "node_group_labeler: listing worker nodes failed; will retry"
                );
                report.list_errors += 1;
                return report;
            }
        };
        report.workers = nodes.len();

        // The patch phase's per-node failure lines only. The classification
        // loop's mislabel warn draws on its own budget below — see
        // [`DEDICATED_MISLABEL_LOGGED_NODES`] for why they cannot share one.
        let failure_log_budget = std::sync::atomic::AtomicUsize::new(LABEL_FAILURE_LOGGED_NODES);
        let dedicated_mislabel_log_budget =
            std::sync::atomic::AtomicUsize::new(DEDICATED_MISLABEL_LOGGED_NODES);
        // Classification is pure bookkeeping over the listed snapshot, so it
        // stays a plain loop; only the resulting patches fan out below.
        let mut to_label: Vec<WorkerNode> = Vec::new();
        // Names of the nodes this tick patched, for the audit diagnostics.
        let mut newly_labeled: Vec<String> = Vec::new();
        // Nodes labeled with some other group. The set drives the cross-tick
        // warn budget and is keyed on uid, because a name can be reused by a
        // replacement instance; the map carries the names the warn reports.
        let mut foreign_group_uids: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut foreign_group_names: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for node in nodes {
            // Dedicated-tainted nodes are never ours: claiming one into the
            // default generic group would hand an org's reserved hardware to
            // the shared pool. Note this check only catches nodes whose taint
            // has *already* landed — the taint is applied control-plane-side
            // and can arrive after registration (see `DEDICATED_TAINT_KEY`),
            // so it is a narrowing, not the safety property. The
            // `labeling_allowed` gate is what covers the untainted window.
            if node.dedicated {
                if node.node_group.as_deref() == Some(group.name.as_str()) {
                    // Counted before the budget check, not inside it: the warn
                    // names at most DEDICATED_MISLABEL_LOGGED_NODES nodes a
                    // tick, so on a wider occurrence the gauge this feeds is
                    // the only signal for the rest.
                    report.mislabeled_dedicated += 1;
                    if claim_log_budget(&dedicated_mislabel_log_budget) {
                        // Budgeted on its own line-count, not the patch-failure
                        // budget: neither the label nor the taint is reconciled
                        // out of this state, so N tainted-and-default-labeled
                        // nodes would otherwise warn N times every tick
                        // forever, starving the patch diagnostics.
                        tracing::warn!(
                            node = %node.name,
                            group = %group.name,
                            "node_group_labeler: dedicated-tainted node carries the \
                             DEFAULT node-group label; leaving it alone — needs \
                             operator attention"
                        );
                    }
                }
                report.skipped_dedicated += 1;
                continue;
            }
            // An unlabeled node here is only *assumed* to be a default-pool
            // worker, and that assumption holds only while no other node
            // group exists — a non-default generic group's freshly-joined
            // nodes are selector-matched, untainted and label-less too, and
            // so is a dedicated node whose taint hasn't been applied yet.
            // The gate below is what makes the assumption safe rather than
            // merely documented.
            match node.node_group.as_deref() {
                None => to_label.push(node),
                Some(value) if value == group.name.as_str() => {
                    report.already_labeled += 1;
                }
                Some(_other) => {
                    // A different group (e.g. a dedicated org group) — its label and
                    // membership are the operator flow's to own, never overwritten.
                    //
                    // Latent: renaming the default group after nodes are labeled lands
                    // every worker here permanently, with only this counter as a signal.
                    // There is no rename surface today.
                    report.foreign_group += 1;
                    foreign_group_names.insert(node.uid.clone(), node.name);
                    foreign_group_uids.insert(node.uid);
                }
            }
        }

        // Labeling gate, read as late as possible — immediately before the
        // patches. "Selector-matched, untainted, unlabeled" only means
        // "default-pool worker" while the default group is the ONLY group; once
        // another exists, this cannot tell a default-pool node from a
        // freshly-joined member of that group, and mislabeling is unrecoverable
        // (the label is written only when absent, so it is never corrected).
        //
        // A gate, not a transaction: a Postgres count and a series of apiserver
        // patches share no atomicity. Two windows remain, which is why enabling
        // this is sequenced work rather than a flag flip:
        //   1. a group row inserted after this count and before the patches —
        //      narrow, and detected by the re-check below;
        //   2. a group's nodes registering *before* its row is inserted — no
        //      group-table gate can catch this, atomic or not.
        // Closing both needs the per-node pool-intent signal.
        let non_default_groups = match self.sink.non_default_group_count().await {
            Ok(count) => count,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "node_group_labeler: counting non-default node groups failed; \
                     will retry"
                );
                report.census_errors += 1;
                return report;
            }
        };
        report.other_groups = non_default_groups;
        if non_default_groups == 0 {
            // Gate open: clear the suppression baseline so that if it ever
            // closes again, that closure is reported rather than compared
            // against a stale set from a previous episode.
            self.suppressed_last_tick
                .lock()
                .expect("suppressed-node set mutex poisoned")
                .clear();
        } else {
            let current: std::collections::HashSet<String> =
                to_label.iter().map(|node| node.uid.clone()).collect();
            let newly_suppressed = {
                let mut previous = self
                    .suppressed_last_tick
                    .lock()
                    .expect("suppressed-node set mutex poisoned");
                let newly = current.difference(&previous).count();
                *previous = current;
                newly
            };
            report.newly_suppressed = newly_suppressed;
            if newly_suppressed == 0 {
                tracing::debug!(
                    non_default_groups,
                    unlabeled = to_label.len(),
                    "node_group_labeler: labeling still suppressed (non-default node \
                     groups exist); no newly-suppressed nodes since the last tick"
                );
            } else {
                tracing::error!(
                    non_default_groups,
                    unlabeled = to_label.len(),
                    "node_group_labeler: non-default node groups exist, so an unlabeled \
                 worker can no longer be assumed to belong to the default group; \
                 suppressing all label writes. Most often these are another group's \
                 nodes inside their normal provisioning window: the dedicated taint \
                 is applied control-plane-side and can land after registration, and \
                 until it does such a node is selector-matched, untainted and \
                 unlabeled — indistinguishable here from a shared worker. Those \
                 clear themselves once the taint lands. A count that does NOT clear \
                 means shared-pool labeling was left incomplete when the first other \
                 group was created; finishing it needs the per-node pool-intent \
                 signal (an unprotected, kubelet-settable MNG label naming the \
                 intended group), which ships with dedicated node-group provisioning."
                );
            }
            report.label_suppressed = to_label.len();
            to_label.clear();
        }

        // Patches fan out rather than looping: independent single-object
        // writes, and a serial writer makes first-enablement cost the sum of
        // every apiserver call with one wedged patch blocking the rest. Each is
        // clamped to what's left of the tick so a wedged apiserver defers.
        // Fair rotation before the writes: the tick budget may not reach the
        // whole list, so start where the last tick stopped.
        self.label_cursor.rotate(&mut to_label);

        let action_deadline = tokio::time::Instant::now() + self.config.tick.get();
        // Borrowed, not cloned per node: the whole stream is awaited here, so
        // `labels` and `group_name` outlive every patch future.
        let labels = &self.labels;
        let group_name = group.name.as_str();
        let shutdown_token = &self.shutdown_token;
        let outcomes: Vec<LabelOutcome> = futures_util::stream::iter(to_label)
            .map(|node| {
                let budget = &failure_log_budget;
                async move {
                    label_one_node(
                        labels,
                        &node,
                        group_name,
                        action_deadline,
                        shutdown_token,
                        budget,
                    )
                    .await
                }
            })
            .buffer_unordered(clamp_write_concurrency(self.config.write_concurrency))
            .collect()
            .await;
        for outcome in outcomes {
            match outcome {
                LabelOutcome::Labeled(name) => {
                    report.labeled += 1;
                    // The audit diagnostics below need the nodes *this tick*
                    // patched, which is exactly this set — an auditor does not
                    // need the already-labeled majority mixed in.
                    newly_labeled.push(name);
                }
                LabelOutcome::Failed => report.label_errors += 1,
                LabelOutcome::Indeterminate => report.label_indeterminate += 1,
                LabelOutcome::NotAttempted(DeferReason::BudgetSpent) => {
                    report.label_deferred += 1;
                }
                LabelOutcome::NotAttempted(DeferReason::Shutdown) => {
                    // `label_deferred` stays the total of every reason, so the
                    // gauge built on it keeps its meaning; the second counter
                    // only says which of them this node was.
                    report.label_deferred += 1;
                    report.label_deferred_shutdown += 1;
                }
                LabelOutcome::NotAttempted(DeferReason::Superseded) => {
                    report.label_deferred += 1;
                    report.label_superseded += 1;
                }
            }
        }
        // Advance by the patches actually issued, not by the whole list, so
        // the next tick resumes past the work this one did instead of
        // re-attempting it. This is a progress guarantee, not an exact
        // hand-off: the pending list is rebuilt from a fresh snapshot each
        // tick (successes drop out, new nodes appear) and the offset is taken
        // modulo its length, so the resume point is approximate.
        self.label_cursor
            .advance(report.labeled + report.label_errors + report.label_indeterminate);
        // Set once, before any branch below can return, so the audit logs and
        // the report cannot disagree about which nodes are in doubt.
        report.newly_labeled = newly_labeled;

        // Re-read the census after the patches. If a group appeared while we
        // were writing, window 1 above may have fired and some of these labels
        // could belong to its nodes. The labels are already written and cannot
        // be attributed from here, so all this tick can do is make it loud and
        // withhold its measurement — the audit is the whole value, and it is
        // what an operator needs before the required-affinity flip. The next
        // tick suppresses labeling, since the group it just observed closes the
        // gate.
        // Reported before the post-write census re-check, not after: every arm
        // of that re-check returns early, and this is a standing condition whose
        // only other signal (`foreign_group_nodes`) is a gauge withheld on an
        // unmeasured tick. Left below it, a fleet that fails the re-check every
        // tick — a stuck patch keeping `patches_issued > 0` plus a failing census
        // read — would never name a permanently unlabelable node at all.
        //
        // Above it, that fleet names the node on the *first* such tick and then
        // goes quiet on both channels: the baseline has absorbed the uid, so no
        // further warn, and `completed` stays false, so the gauge stays
        // withheld. One line, plus the standing
        // `tick_error{kind:census_after_write}` and the absence of
        // `tick_completed` — not continuous coverage. Reads only
        // `report.other_groups`, `report.foreign_group` and
        // `foreign_group_uids`, none of which the re-check touches.
        if report.other_groups == 0 && report.foreign_group > 0 {
            let newly_foreign = {
                let mut previous = self
                    .foreign_group_last_tick
                    .lock()
                    .expect("foreign-group set mutex poisoned");
                let fresh = foreign_group_uids
                    .difference(&previous)
                    .cloned()
                    .collect::<Vec<_>>();
                *previous = foreign_group_uids;
                fresh
            };
            report.newly_foreign_group = newly_foreign.len();
            if !newly_foreign.is_empty() {
                // Sorted for a stable line across ticks: `newly_foreign` comes
                // out of a HashSet difference, so its order is arbitrary and a
                // truncated list would otherwise name different nodes each time.
                let mut named: Vec<&str> = newly_foreign
                    .iter()
                    .filter_map(|uid| foreign_group_names.get(uid).map(String::as_str))
                    .collect();
                named.sort_unstable();
                tracing::warn!(
                    foreign_group = report.foreign_group,
                    newly_foreign = report.newly_foreign_group,
                    newly_foreign_nodes = ?named
                        .iter()
                        .take(FOREIGN_GROUP_LOGGED_NODES)
                        .collect::<Vec<_>>(),
                    newly_foreign_truncated =
                        named.len().saturating_sub(FOREIGN_GROUP_LOGGED_NODES),
                    default_group = %group.name,
                    // Reports the observation rather than asserting the
                    // present tense: the gate above is the *pre*-write census,
                    // and this line is deliberately emitted before the
                    // post-write re-check, so on a tick where that re-check
                    // finds a group appeared, a flat "does not exist" would
                    // already be false when it was written. The
                    // `gate_race_detected` error that follows supplies the
                    // context.
                    "node_group_labeler: worker nodes carry a node-group label naming a \
                     group that did not exist as of this tick's census (the default was \
                     the only group then); unless it has since been created they will \
                     never be labeled and will serve no sandboxes once required \
                     node-group affinity ships"
                );
            }
        } else {
            // Either another group exists — so the label may name a real group
            // this tick can't see — or nothing is mislabeled. Drop the baseline
            // so a later recurrence is reported instead of being compared
            // against a stale set from a previous episode.
            self.foreign_group_last_tick
                .lock()
                .expect("foreign-group set mutex poisoned")
                .clear();
        }

        // Deliberately counts failures and indeterminates as issued: neither is
        // proof no label landed (see `LabelOutcome::Failed`), so the audit is
        // owed either way. The cost of that fail-closed choice is that a tick
        // which wrote *nothing successfully* can still trip
        // `gate_race_detected` — one node whose patch fails every tick (a 409
        // from a competing writer, a partial RBAC denial) holds
        // `label_errors >= 1` forever, so the next group create landing inside
        // a tick window reports a race with `labeled = 0` and an empty
        // `confirmed_nodes`. `label_indeterminate` does the same and does it
        // routinely: on a fleet larger than one tick's write budget, the writes
        // the deadline truncates land in it every tick (see the
        // `truncated_by_tick` arm above). So the signature to read as this case
        // rather than as a real race is no confirmed nodes with a non-zero
        // errored *or* indeterminate count; the metrics catalog says the same.
        // Do not "fix" it by requiring
        // `report.labeled > 0`: that would drop the audit on exactly the ticks
        // where the seam cannot tell a rejected patch from a committed one.
        //
        // The other direction is a real bug rather than a cost: `label_deferred`
        // must stay out, because a deferred node was never contacted. `t25`
        // pins both directions, one row per `LabelOutcome`.
        let patches_issued = report.labeled + report.label_errors + report.label_indeterminate;
        if non_default_groups == 0 {
            match self.sink.non_default_group_count().await {
                Ok(0) => {}
                Ok(appeared) if patches_issued == 0 => {
                    // The gate was open but this tick issued nothing, so no
                    // label can have landed and there is nothing to audit. The
                    // tick still ends unmeasured: the census its counts were
                    // classified against has changed.
                    tracing::warn!(
                        non_default_groups = appeared,
                        "node_group_labeler: a non-default node group appeared during \
                         this tick; no labels were written, so nothing needs auditing \
                         — re-evaluating next tick"
                    );
                    report.census_unusable_no_writes = true;
                    return report;
                }
                Ok(appeared) => {
                    tracing::error!(
                        non_default_groups = appeared,
                        labeled = report.labeled,
                        indeterminate = report.label_indeterminate,
                        errored = report.label_errors,
                        // The audit this asks for needs the node names, and
                        // the confirmed ones are right here. Bounded so a
                        // full-fleet tick can't emit an unbounded log line.
                        //
                        // The indeterminate/errored names are only *partly*
                        // recoverable: their per-node warns are rationed by
                        // `LABEL_FAILURE_LOGGED_NODES`, so on a wide failure
                        // most are named nowhere. The counts below bound the
                        // audit; the names do not cover it.
                        confirmed_nodes = ?report
                            .newly_labeled
                            .iter()
                            .take(GATE_RACE_LOGGED_NODES)
                            .collect::<Vec<_>>(),
                        confirmed_nodes_truncated = report
                            .newly_labeled
                            .len()
                            .saturating_sub(GATE_RACE_LOGGED_NODES),
                        "node_group_labeler: a non-default node group appeared while \
                         this tick was writing labels; the labels just applied may \
                         include nodes belonging to it — audit the \
                         node-restriction.kubernetes.io/space-node-group labels on \
                         recently-joined nodes before enabling required node-group \
                         affinity."
                    );
                    report.gate_race_detected = true;
                    return report;
                }
                Err(err) if patches_issued > 0 => {
                    // Labels were written and the gate can no longer be
                    // confirmed. That owes the same audit as an observed race:
                    // if a group did appear in this window, the labels stand
                    // and every later tick reads them as `already_labeled`.
                    // Booking this only as a census error would report it
                    // under a contract that says nothing was written.
                    report.census_after_write_errors += 1;
                    tracing::error!(
                        error = %err,
                        labeled = report.labeled,
                        indeterminate = report.label_indeterminate,
                        errored = report.label_errors,
                        confirmed_nodes = ?report
                            .newly_labeled
                            .iter()
                            .take(GATE_RACE_LOGGED_NODES)
                            .collect::<Vec<_>>(),
                        confirmed_nodes_truncated = report
                            .newly_labeled
                            .len()
                            .saturating_sub(GATE_RACE_LOGGED_NODES),
                        "node_group_labeler: could not re-check the node-group census \
                         after writing labels, so the gate cannot be confirmed to have \
                         held (no group was observed to appear — the read itself \
                         failed) — audit the \
                         node-restriction.kubernetes.io/space-node-group labels on \
                         recently-joined nodes before enabling required node-group \
                         affinity."
                    );
                    return report;
                }
                Err(err) => {
                    // Nothing was written, so there is nothing to audit — the
                    // tick ends unmeasured only because the census behind its
                    // counts can't be re-validated.
                    tracing::warn!(
                        error = %err,
                        "node_group_labeler: re-checking the node-group census after \
                         labeling failed; nothing was written, so re-evaluating next \
                         tick"
                    );
                    report.census_errors += 1;
                    report.census_unusable_no_writes = true;
                    return report;
                }
            }
        }

        report.completed = true;
        report
    }
}
