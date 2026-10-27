//! The template desired-state facet and the template base-snapshot policy refresh.

use super::*;

/// How long the cluster must have left a local template out of its
/// desired-state answers before a worker deletes its copy.
///
/// The cluster answers for a template only while this node has a reason to
/// hold it: the template is generic, a sandbox of it is placed here, an
/// enabled warm pool targets it, or it is being retired. An org-specific
/// template whose sandboxes have all left, like a template the cluster does
/// not know, is omitted from every answer while its build stays cached, and
/// the template-manager LRU reclaims it only once the cache is over budget. Without this, a worker keeps such templates and
/// asks about them every sync indefinitely. The wait keeps the copy for a
/// sandbox of the same template placed here again soon after.
pub(super) const UNANSWERED_TEMPLATE_EVICT_AFTER: Duration = Duration::from_secs(60 * 60);

/// Test-only override of [`UNANSWERED_TEMPLATE_EVICT_AFTER`], in seconds, so a
/// Kind run can watch a delete and the next placement without waiting an hour.
const UNANSWERED_TEMPLATE_EVICT_AFTER_SECS_ENV: &str =
    "SPACE_NODECTL_UNANSWERED_TEMPLATE_EVICT_AFTER_SECS";

fn unanswered_template_evict_after() -> Duration {
    static AFTER: std::sync::LazyLock<Duration> = std::sync::LazyLock::new(|| {
        pplx_space_common_core::env::test_hook_var(UNANSWERED_TEMPLATE_EVICT_AFTER_SECS_ENV)
            .and_then(|secs| secs.trim().parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .map_or(UNANSWERED_TEMPLATE_EVICT_AFTER, Duration::from_secs)
    });
    *AFTER
}

/// Cap on unanswered-template deletes per sync. Each delete can spend a
/// template-manager subvolume-delete budget, and the facet shares one
/// watchdog; the rest wait for the next sync.
pub(super) const UNANSWERED_TEMPLATE_EVICTS_PER_SYNC: usize = 4;

/// When each local template was first left out of a desired-state answer,
/// cleared as soon as an answer includes it. Process lifetime, in memory: a
/// node-ctl restart only restarts the wait.
#[derive(Debug, Default)]
pub(super) struct UnansweredTemplates {
    since: BTreeMap<String, std::time::Instant>,
}

impl UnansweredTemplates {
    /// Record one answer to a request for `requested`. `full_scope` means the
    /// request named every local template, so entries for templates no longer
    /// local are dropped.
    pub(super) fn observe(
        &mut self,
        requested: &[String],
        answered: &BTreeSet<String>,
        full_scope: bool,
        now: std::time::Instant,
    ) {
        if full_scope {
            let requested = requested.iter().collect::<BTreeSet<_>>();
            self.since
                .retain(|template, _| requested.contains(template));
        }
        for template in requested {
            if answered.contains(template) {
                self.since.remove(template);
            } else {
                self.since.entry(template.clone()).or_insert(now);
            }
        }
    }

    /// Templates left out of every answer for at least `after`, longest first.
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

    /// Record a delete attempt. A template that is gone is forgotten; one
    /// still here waits the full period again before the next attempt.
    pub(super) fn settle(
        &mut self,
        template: &str,
        outcome: UnansweredDelete,
        now: std::time::Instant,
    ) {
        match outcome {
            UnansweredDelete::Deleted | UnansweredDelete::Gone => {
                self.since.remove(template);
            }
            UnansweredDelete::InUse | UnansweredDelete::Failed => {
                self.since.insert(template.to_string(), now);
            }
        }
    }
}

/// What deleting an unanswered template came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UnansweredDelete {
    Deleted,
    /// It was no longer local.
    Gone,
    /// template-manager refused: a local sandbox still uses one of its builds.
    InUse,
    Failed,
}

impl UnansweredDelete {
    pub(super) fn from_result<T>(result: &Result<T, tonic::Status>) -> Self {
        match result {
            Ok(_) => Self::Deleted,
            Err(error) if error.code() == Code::NotFound => Self::Gone,
            Err(error) if error.code() == Code::FailedPrecondition => Self::InUse,
            Err(_) => Self::Failed,
        }
    }
}

static UNANSWERED_TEMPLATES: std::sync::LazyLock<std::sync::Mutex<UnansweredTemplates>> =
    std::sync::LazyLock::new(Default::default);

fn unanswered_templates() -> std::sync::MutexGuard<'static, UnansweredTemplates> {
    UNANSWERED_TEMPLATES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) async fn refresh_template_base_snapshot_policy(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
) -> Result<ReconcileStats, String> {
    let templates = resolve_template_scope(scope).await?;
    let mut stats = ReconcileStats {
        templates_seen: templates.len() as u64,
        ..ReconcileStats::default()
    };
    if templates.is_empty() {
        return Ok(stats);
    }

    let mut client = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = client
        .get_node_template_base_snapshot_policy_state(
            GetNodeTemplateBaseSnapshotPolicyStateRequest {
                node_id: node_id.to_string(),
                templates,
                role: proto_node_role(node_role),
            },
        )
        .await
        .map_err(|error| format!("cluster base-snapshot policy refresh failed: {error}"))?
        .into_inner();

    let mut tmpl = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let mut failures = 0_u64;
    for template in response.templates {
        if let Err(error) = tmpl
            .update_template_snapshot_policy(local_manager_request(
                UpdateTemplateSnapshotPolicyRequest {
                    template: template.template.clone(),
                    ensure_base_snapshot: template.ensure_base_snapshot,
                    snapshot_vcpu: Some(template.snapshot_vcpu),
                    snapshot_memory_mib: Some(template.snapshot_memory_mib),
                },
                false,
            ))
            .await
        {
            tracing::warn!(
                template = %template.template,
                %error,
                "failed to apply template base-snapshot policy"
            );
            failures = failures.saturating_add(1);
            continue;
        }
        stats.templates_applied = stats.templates_applied.saturating_add(1);
    }
    if failures > 0 {
        return Err(format!(
            "failed to apply {failures} template base-snapshot policy item(s)"
        ));
    }
    Ok(stats)
}

pub(super) async fn refresh_template_desired_state(
    node_id: &str,
    node_role: Option<NodeRole>,
    scope: FacetScope,
) -> Result<ReconcileStats, String> {
    let full_scope = matches!(scope, FacetScope::Full);
    let templates = resolve_template_scope(scope).await?;
    let mut stats = ReconcileStats {
        templates_seen: templates.len() as u64,
        ..ReconcileStats::default()
    };
    if templates.is_empty() {
        return Ok(stats);
    }

    let mut cluster = connect_cluster_api()
        .await
        .map_err(|error| error.to_string())?;
    let response = cluster
        .get_node_template_desired_state(GetNodeTemplateDesiredStateRequest {
            node_id: node_id.to_string(),
            templates: templates.clone(),
            role: proto_node_role(node_role),
        })
        .await
        .map_err(|error| format!("cluster template desired-state refresh failed: {error}"))?
        .into_inner();
    let evict_unanswered = if node_role == Some(NodeRole::Worker) {
        let answered = response
            .templates
            .iter()
            .map(|template| template.template.clone())
            .collect::<BTreeSet<_>>();
        let now = std::time::Instant::now();
        let mut unanswered = unanswered_templates();
        unanswered.observe(&templates, &answered, full_scope, now);
        unanswered.due(
            now,
            unanswered_template_evict_after(),
            UNANSWERED_TEMPLATE_EVICTS_PER_SYNC,
        )
    } else {
        Vec::new()
    };

    let mut tmpl = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|error| format!("connect template-manager: {error}"))?;
    let local_templates = tmpl
        .list_templates(Request::new(ListTemplatesRequest {}))
        .await
        .map_err(|error| format!("list local templates: {error}"))?
        .into_inner()
        .templates
        .into_iter()
        .map(|template| (template.name.clone(), template.default_auto_lifecycle))
        .collect::<BTreeMap<_, _>>();
    let local_pools = tmpl
        .pool_status(Request::new(PoolStatusRequest { template: None }))
        .await
        .map(|response| {
            response
                .into_inner()
                .pools
                .into_iter()
                .map(|pool| (pool.template.clone(), pool))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let mut failures = 0_u64;
    for template in response.templates {
        let mut applied = false;
        if template.present == Some(false) {
            if local_pools.contains_key(&template.template) {
                match tmpl
                    .delete_pool(local_manager_request(
                        DeletePoolRequest {
                            template: template.template.clone(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        applied = true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to delete undesired template pool"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
            if local_templates.contains_key(&template.template) {
                match tmpl
                    .delete_template(local_manager_request(
                        DeleteTemplateRequest {
                            template: template.template.clone(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        applied = true;
                    }
                    Err(error) if error.code() == Code::NotFound => {}
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to delete undesired template"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
            if applied {
                stats.templates_applied = stats.templates_applied.saturating_add(1);
            }
            continue;
        }

        if !local_templates.contains_key(&template.template) {
            if local_pools.contains_key(&template.template) {
                match tmpl
                    .delete_pool(local_manager_request(
                        DeletePoolRequest {
                            template: template.template.clone(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        stats.templates_applied = stats.templates_applied.saturating_add(1);
                    }
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to delete pool for absent local template"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
            continue;
        }

        if let Some(policy) = template.default_auto_lifecycle.clone() {
            let current = local_templates
                .get(&template.template)
                .and_then(|policy| policy.as_ref());
            if current != Some(&policy) {
                match tmpl
                    .update_template_settings(local_manager_request(
                        UpdateTemplateSettingsRequest {
                            template: template.template.clone(),
                            default_auto_lifecycle: Some(policy),
                            network_policy: None,
                            network_policy_generation: 0,
                            network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                            auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
                        },
                        true,
                    ))
                    .await
                {
                    Ok(_) => {
                        applied = true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            template = %template.template,
                            %error,
                            "failed to apply template default auto-lifecycle policy"
                        );
                        failures = failures.saturating_add(1);
                    }
                }
            }
        }

        match template.pool {
            Some(pool) => match tmpl
                .create_pool(local_manager_request(
                    CreatePoolRequest {
                        template: template.template.clone(),
                        target_count: Some(pool.target_count),
                        boot_mode: pool.boot_mode,
                        vcpu: pool.vcpu,
                        memory_mib: pool.memory_mib,
                    },
                    true,
                ))
                .await
            {
                Ok(_) => {
                    applied = true;
                }
                Err(error) => {
                    tracing::warn!(
                        template = %template.template,
                        %error,
                        "failed to apply template pool policy"
                    );
                    failures = failures.saturating_add(1);
                }
            },
            None => {
                if local_pools.contains_key(&template.template) {
                    match tmpl
                        .delete_pool(local_manager_request(
                            DeletePoolRequest {
                                template: template.template.clone(),
                            },
                            true,
                        ))
                        .await
                    {
                        Ok(_) => {
                            applied = true;
                        }
                        Err(error) => {
                            tracing::warn!(
                                template = %template.template,
                                %error,
                                "failed to delete undesired template pool"
                            );
                            failures = failures.saturating_add(1);
                        }
                    }
                }
            }
        }

        if applied {
            stats.templates_applied = stats.templates_applied.saturating_add(1);
        }
    }

    for template in evict_unanswered {
        let outcome = if local_templates.contains_key(&template) {
            let result = tmpl
                .delete_template(local_manager_request(
                    DeleteTemplateRequest {
                        template: template.clone(),
                    },
                    true,
                ))
                .await;
            let outcome = UnansweredDelete::from_result(&result);
            match &result {
                Ok(_) => {
                    tracing::info!(
                        template,
                        "deleted local template the cluster no longer answers for"
                    );
                    stats.templates_applied = stats.templates_applied.saturating_add(1);
                }
                Err(error) => match outcome {
                    UnansweredDelete::InUse => tracing::debug!(
                        template,
                        %error,
                        "kept unanswered local template that local sandboxes use"
                    ),
                    UnansweredDelete::Failed => tracing::warn!(
                        template,
                        %error,
                        "failed to delete local template the cluster no longer answers for"
                    ),
                    UnansweredDelete::Deleted | UnansweredDelete::Gone => {}
                },
            }
            outcome
        } else {
            UnansweredDelete::Gone
        };
        unanswered_templates().settle(&template, outcome, std::time::Instant::now());
    }

    if failures > 0 {
        return Err(format!(
            "failed to apply {failures} template desired-state item(s)"
        ));
    }
    Ok(stats)
}
