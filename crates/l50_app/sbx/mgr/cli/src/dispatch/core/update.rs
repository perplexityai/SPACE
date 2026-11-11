use super::*;
use std::collections::HashMap;

pub(super) async fn handle(
    sandbox_id: SandboxSelector,
    vcpu: Option<u32>,
    memory: Option<u32>,
    pids: Option<u32>,
    inherit_template_auto_lifecycle: bool,
    auto_lifecycle: AutoLifecycleArgs,
    client: &mut SandboxClient,
) {
    validate_update_request(
        vcpu,
        memory,
        pids,
        inherit_template_auto_lifecycle,
        &auto_lifecycle,
    )
    .unwrap_or_else(|error| exit_error(error));

    if vcpu.is_some() || memory.is_some() || pids.is_some() {
        client
            .update(UpdateRequest {
                sandbox_id: sandbox_id.to_string(),
                vcpu,
                memory_mib: memory,
                pids_max: pids,
                expected_current_owner: None,
                owner: None,
                process_env: HashMap::default(),
                replace_process_env: false,
            })
            .await
            .unwrap_or_else(|error| exit_error(error));
    }

    let auto_lifecycle_summary = apply_auto_lifecycle_update(
        client,
        &sandbox_id,
        inherit_template_auto_lifecycle,
        &auto_lifecycle,
    )
    .await;

    let changes = update_change_summary(
        vcpu,
        memory,
        pids,
        inherit_template_auto_lifecycle,
        auto_lifecycle_summary,
    );
    println!(
        "Updated {}: {}",
        sandbox_label(sandbox_id.as_str()),
        changes.join(", ")
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateValidationError {
    InheritTemplateWithExplicitPolicy,
    NoUpdateRequested,
}

impl std::fmt::Display for UpdateValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InheritTemplateWithExplicitPolicy => formatter.write_str(
                "--inherit-template-auto-lifecycle cannot be combined with explicit policy flags",
            ),
            Self::NoUpdateRequested => formatter.write_str("no update requested"),
        }
    }
}

fn validate_update_request(
    vcpu: Option<u32>,
    memory: Option<u32>,
    pids: Option<u32>,
    inherit_template_auto_lifecycle: bool,
    auto_lifecycle: &AutoLifecycleArgs,
) -> Result<(), UpdateValidationError> {
    if inherit_template_auto_lifecycle && auto_lifecycle.is_explicit() {
        return Err(UpdateValidationError::InheritTemplateWithExplicitPolicy);
    }

    if vcpu.is_none()
        && memory.is_none()
        && pids.is_none()
        && !inherit_template_auto_lifecycle
        && !auto_lifecycle.is_explicit()
    {
        return Err(UpdateValidationError::NoUpdateRequested);
    }

    Ok(())
}

async fn apply_auto_lifecycle_update(
    client: &mut SandboxClient,
    sandbox_id: &SandboxSelector,
    inherit_template_auto_lifecycle: bool,
    auto_lifecycle: &AutoLifecycleArgs,
) -> Option<String> {
    if !inherit_template_auto_lifecycle && !auto_lifecycle.is_explicit() {
        return None;
    }

    let policy = if inherit_template_auto_lifecycle {
        let template_name = resolve_sandbox_template(client, sandbox_id).await;
        fetch_template_default_policy(&template_name).await
    } else {
        auto_lifecycle.to_policy().unwrap_or_else(|error| {
            exit_error(format!("invalid auto lifecycle policy: {error}"));
        })
    };
    let policy = policy.unwrap_or_else(AutoLifecyclePolicy::disabled);
    let summary = format_policy(&policy);
    client
        .set_auto_lifecycle_policy(SetAutoLifecyclePolicyRequest {
            sandbox_id: sandbox_id.to_string(),
            policy: Some(pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy::from(
                &policy,
            )),
            authority: PolicyAuthority::Local.as_proto_i32(),
        })
        .await
        .unwrap_or_else(|error| exit_error(error));
    Some(summary)
}

fn update_change_summary(
    vcpu: Option<u32>,
    memory: Option<u32>,
    pids: Option<u32>,
    inherit_template_auto_lifecycle: bool,
    auto_lifecycle_summary: Option<String>,
) -> Vec<String> {
    let mut changes = Vec::new();
    if let Some(v) = vcpu {
        changes.push(format!("vcpu={v}"));
    }
    if let Some(m) = memory {
        changes.push(format!("memory={m}MiB"));
    }
    if let Some(p) = pids {
        changes.push(format!("pids={p}"));
    }
    if inherit_template_auto_lifecycle {
        changes.push(format!(
            "auto_lifecycle={}",
            auto_lifecycle_summary.unwrap_or_else(|| "template-default".to_string())
        ));
    } else if let Some(summary) = auto_lifecycle_summary {
        changes.push(format!("auto_lifecycle={summary}"));
    }
    changes
}

#[cfg(test)]
mod tests;
