//! The template desired-state facet and the template base-snapshot policy refresh.

use super::*;

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
            templates,
            role: proto_node_role(node_role),
        })
        .await
        .map_err(|error| format!("cluster template desired-state refresh failed: {error}"))?
        .into_inner();

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

    if failures > 0 {
        return Err(format!(
            "failed to apply {failures} template desired-state item(s)"
        ));
    }
    Ok(stats)
}
