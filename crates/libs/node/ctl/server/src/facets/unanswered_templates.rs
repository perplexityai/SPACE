//! Deleting local templates the cluster released to this worker.
//!
//! The cluster answers a template desired-state request only for templates the
//! node has a reason to hold: the template is generic and present, a sandbox of
//! it is placed here, an enabled Kubernetes warm-pool config targets it, or it
//! is retired and still in this node's published inventory. A present template
//! the node reports holding without such a reason comes back in
//! `released_templates` instead. Without acting on that, a worker keeps an org
//! template whose sandboxes have all left, asks about it every sync, and holds
//! its build until the template-manager LRU finds the cache over budget.
//!
//! A worker records when each local template was first released, and on a full
//! sync deletes one that has stayed released for the wait. A template the
//! cluster does not know, or one missing from both lists, is never deleted:
//! only membership in `released_templates` counts, so a cluster-api that does
//! not send the list deletes nothing.

use super::*;

/// How long a template must stay released before a worker deletes its copy.
/// The wait keeps the copy for a sandbox of the same template placed here
/// again soon after.
pub(super) const UNANSWERED_TEMPLATE_EVICT_AFTER: Duration = Duration::from_secs(60 * 60);

/// Overrides [`UNANSWERED_TEMPLATE_EVICT_AFTER`] in seconds; `0` turns the
/// deletes off.
const UNANSWERED_TEMPLATE_EVICT_AFTER_SECS_ENV: &str =
    "SPACE_NODECTL_UNANSWERED_TEMPLATE_EVICT_AFTER_SECS";

/// The configured wait, or `None` when deleting released templates is off.
pub(super) fn unanswered_template_evict_after() -> Option<Duration> {
    static AFTER: std::sync::LazyLock<Option<Duration>> = std::sync::LazyLock::new(|| {
        match std::env::var(UNANSWERED_TEMPLATE_EVICT_AFTER_SECS_ENV) {
            Err(_) => Some(UNANSWERED_TEMPLATE_EVICT_AFTER),
            Ok(raw) => match raw.trim().parse::<u64>() {
                Ok(0) => None,
                Ok(secs) => Some(Duration::from_secs(secs)),
                // The setting turns deletes off, so a value that is not a
                // number of seconds does too rather than falling back to the
                // default.
                Err(_) => {
                    tracing::warn!(
                        env_var = UNANSWERED_TEMPLATE_EVICT_AFTER_SECS_ENV,
                        value = %raw,
                        "invalid released-template wait; deleting released templates is off",
                    );
                    None
                }
            },
        }
    });
    *AFTER
}

/// Cap on deletes per sync. Each can spend a template-manager subvolume-delete
/// budget; the rest wait for the next sync.
pub(super) const UNANSWERED_TEMPLATE_EVICTS_PER_SYNC: usize = 4;

/// Deadline for each recheck and delete call.
pub(super) const UNANSWERED_TEMPLATE_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Wall-clock budget for the deletes inside one sync, well inside the facet's
/// stall watchdog so the desired-state applies that share the sync can never
/// fail because of them. No recheck or delete call runs past it; what is left
/// waits for the next sync.
pub(super) const UNANSWERED_TEMPLATE_SYNC_BUDGET: Duration = Duration::from_secs(20);

/// What [`UnansweredTemplates::plan`] compares against.
#[derive(Clone, Copy, Debug)]
pub(super) struct PlanLimits {
    /// How long a template must stay released.
    pub(super) after: Duration,
    /// The longest gap between full answers that still counts as the cluster
    /// answering continuously.
    pub(super) max_gap: Duration,
    /// The most templates one sync deletes.
    pub(super) limit: usize,
}

/// When each local template was first released, cleared as soon as an answer
/// stops releasing it. Process lifetime, in memory: a node-ctl restart only
/// restarts the wait.
#[derive(Debug, Default)]
pub(super) struct UnansweredTemplates {
    since: BTreeMap<String, std::time::Instant>,
    /// When the last full-scope answer arrived.
    last_full_answer: Option<std::time::Instant>,
}

impl UnansweredTemplates {
    /// Record one desired-state answer and pick the templates to delete now.
    ///
    /// The wait counts only time the cluster kept answering: after a gap
    /// longer than `limits.max_gap` without a full answer, such as a cluster
    /// outage, every wait starts again. Only a full sync deletes, so the cap
    /// holds per sync and a run scoped to other templates acts on nothing it
    /// did not ask about.
    pub(super) fn plan(
        &mut self,
        requested: &[String],
        released: &BTreeSet<String>,
        full_scope: bool,
        now: std::time::Instant,
        limits: PlanLimits,
    ) -> Vec<String> {
        if full_scope {
            if self
                .last_full_answer
                .is_some_and(|last| now.saturating_duration_since(last) > limits.max_gap)
            {
                self.since.clear();
            }
            self.last_full_answer = Some(now);
            let requested = requested.iter().collect::<BTreeSet<_>>();
            self.since
                .retain(|template, _| requested.contains(template));
        }
        for template in requested {
            if released.contains(template) {
                self.since.entry(template.clone()).or_insert(now);
            } else {
                self.since.remove(template);
            }
        }
        if full_scope {
            self.due(now, limits.after, limits.limit)
        } else {
            Vec::new()
        }
    }

    /// Templates released for at least `after`, longest first.
    pub(super) fn due(
        &self,
        now: std::time::Instant,
        after: Duration,
        limit: usize,
    ) -> Vec<String> {
        let mut due = self
            .since
            .iter()
            .filter(|(_, since)| now.saturating_duration_since(**since) >= after)
            .map(|(template, since)| (*since, template.clone()))
            .collect::<Vec<_>>();
        due.sort();
        due.into_iter()
            .take(limit)
            .map(|(_, template)| template)
            .collect()
    }

    /// Drop every record: a full sync found no local template.
    pub(super) fn clear(&mut self) {
        self.since.clear();
    }

    fn forget(&mut self, template: &str) {
        self.since.remove(template);
    }

    /// Start the template's wait again.
    fn restart(&mut self, template: &str, now: std::time::Instant) {
        self.since.insert(template.to_string(), now);
    }

    /// Put back the record a delete attempt replaced.
    fn restore(&mut self, template: &str, since: Option<std::time::Instant>) {
        match since {
            Some(since) => {
                self.since.insert(template.to_string(), since);
            }
            None => self.forget(template),
        }
    }
}

/// What deleting a released template came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UnansweredDelete {
    Deleted,
    /// It was no longer local.
    Gone,
    /// template-manager kept it: a build was used within the wait, or a local
    /// sandbox, a pin or a warm pool still protects one.
    InUse,
    /// Another eviction held template-manager's gate, or the call ran out of
    /// time: the delete may simply not have happened yet.
    Busy,
    /// template-manager predates `DeleteIdleTemplate`.
    Unsupported,
    Failed,
}

impl UnansweredDelete {
    pub(super) fn from_result<T>(result: &Result<T, tonic::Status>) -> Self {
        match result {
            Ok(_) => Self::Deleted,
            Err(error) => match error.code() {
                Code::NotFound => Self::Gone,
                Code::FailedPrecondition => Self::InUse,
                Code::Aborted | Code::DeadlineExceeded => Self::Busy,
                Code::Unimplemented => Self::Unsupported,
                _ => Self::Failed,
            },
        }
    }
}

/// The calls the delete loop makes, so its decisions can be driven in tests.
pub(super) trait UnansweredTemplateCalls {
    /// Whether the cluster still releases `template`, asked about it alone.
    async fn still_released(&mut self, template: &str) -> Result<bool, tonic::Status>;

    /// template-manager `DeleteIdleTemplate`, with `idle` as the idle bound.
    async fn delete_idle(&mut self, template: &str, idle: Duration) -> Result<(), tonic::Status>;
}

/// Delete the `due` templates that are still local and still released.
/// Returns how many were deleted.
///
/// Each delete is rechecked with the cluster first: the answer that made a
/// template due can be up to a sync old, and a sandbox placed here since then
/// makes the cluster answer for it. The attempt is recorded before the delete
/// call, so one the facet watchdog cancels still waits the full period. A
/// delete that may simply not have happened yet ([`UnansweredDelete::Busy`])
/// gets its original record back and is tried again on the next sync.
pub(super) async fn delete_unanswered_templates(
    tracker: &std::sync::Mutex<UnansweredTemplates>,
    due: Vec<String>,
    is_local: impl Fn(&str) -> bool,
    calls: &mut impl UnansweredTemplateCalls,
    after: Duration,
) -> u64 {
    let lock = move || {
        tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    };
    let deadline = tokio::time::Instant::now() + UNANSWERED_TEMPLATE_SYNC_BUDGET;
    let mut deleted = 0;
    for template in due {
        if !is_local(&template) {
            lock().forget(&template);
            continue;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        match tokio::time::timeout_at(
            deadline.min(now + UNANSWERED_TEMPLATE_RPC_TIMEOUT),
            calls.still_released(&template),
        )
        .await
        {
            Ok(Ok(true)) => {}
            // The cluster answers for it again or no longer releases it.
            Ok(Ok(false)) => {
                lock().forget(&template);
                continue;
            }
            Ok(Err(error)) => {
                tracing::debug!(
                    template,
                    %error,
                    "kept released local template: cluster recheck failed"
                );
                continue;
            }
            Err(_) => {
                tracing::debug!(
                    template,
                    "kept released local template: cluster recheck timed out"
                );
                continue;
            }
        }

        let previous = {
            let mut records = lock();
            let previous = records.since.get(&template).copied();
            records.restart(&template, std::time::Instant::now());
            previous
        };
        let result = tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + UNANSWERED_TEMPLATE_RPC_TIMEOUT),
            calls.delete_idle(&template, after),
        )
        .await
        .unwrap_or_else(|_| {
            Err(tonic::Status::deadline_exceeded(
                "DeleteIdleTemplate timed out",
            ))
        });
        let outcome = UnansweredDelete::from_result(&result);
        match (&result, outcome) {
            (Ok(()), _) => tracing::info!(template, "deleted local template the cluster released"),
            (
                Err(error),
                UnansweredDelete::InUse | UnansweredDelete::Busy | UnansweredDelete::Unsupported,
            ) => {
                tracing::debug!(template, %error, "kept released local template")
            }
            (Err(error), UnansweredDelete::Failed) => tracing::warn!(
                template,
                %error,
                "failed to delete local template the cluster released"
            ),
            (Err(_), UnansweredDelete::Deleted | UnansweredDelete::Gone) => {}
        }
        let mut records = lock();
        match outcome {
            UnansweredDelete::Deleted => {
                records.forget(&template);
                deleted += 1;
            }
            UnansweredDelete::Gone => records.forget(&template),
            UnansweredDelete::Busy => records.restore(&template, previous),
            UnansweredDelete::InUse | UnansweredDelete::Unsupported | UnansweredDelete::Failed => {
                records.restart(&template, std::time::Instant::now());
            }
        }
    }
    deleted
}

/// The production calls: the cluster connection and template-manager client
/// the facet already holds.
pub(super) struct FacetUnansweredTemplateCalls<'a> {
    pub(super) cluster: &'a mut crate::TracedClusterApiClient,
    pub(super) template_manager: &'a mut pplx_space_template_mgr_client::TemplateManagerGrpcClient,
    pub(super) node_id: &'a str,
    pub(super) node_role: Option<NodeRole>,
}

impl UnansweredTemplateCalls for FacetUnansweredTemplateCalls<'_> {
    async fn still_released(&mut self, template: &str) -> Result<bool, tonic::Status> {
        let response = self
            .cluster
            .get_node_template_desired_state(GetNodeTemplateDesiredStateRequest {
                node_id: self.node_id.to_string(),
                templates: vec![template.to_string()],
                role: proto_node_role(self.node_role),
            })
            .await?
            .into_inner();
        Ok(response
            .released_templates
            .iter()
            .any(|released| released == template))
    }

    async fn delete_idle(&mut self, template: &str, idle: Duration) -> Result<(), tonic::Status> {
        self.template_manager
            .delete_idle_template(local_manager_request(
                DeleteIdleTemplateRequest {
                    template: template.to_string(),
                    idle_secs: idle.as_secs(),
                },
                true,
            ))
            .await
            .map(drop)
    }
}

pub(super) static UNANSWERED_TEMPLATES: std::sync::LazyLock<std::sync::Mutex<UnansweredTemplates>> =
    std::sync::LazyLock::new(Default::default);

#[cfg(test)]
mod tests;
