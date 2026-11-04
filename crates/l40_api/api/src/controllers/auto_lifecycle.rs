use pplx_space_common_core::types::{
    AutoLifecycleActivitySource, AutoLifecycleConditionKind, AutoLifecycleConditionSetId,
    AutoLifecyclePolicy, AutoLifecycleRuntimeState, AutoLifecycleStatus, AutoLifecycleWakeSource,
};
use pplx_space_sbx_mgr_client::{auto_lifecycle as sbx_auto_lifecycle, proto as sbx_proto};
use tonic::Status;

pub const ACCESS_IDLE_CONDITION_SET: AutoLifecycleConditionSetId =
    AutoLifecycleConditionSetId::AccessIdle;
pub const COMMAND_IDLE_CONDITION_SET: AutoLifecycleConditionSetId =
    AutoLifecycleConditionSetId::CommandIdle;
pub const GUEST_ACTIVITY_CONDITION_SET: AutoLifecycleConditionSetId =
    AutoLifecycleConditionSetId::GuestActivity;

pub fn validate_v1_policy(policy: &AutoLifecyclePolicy) -> Result<(), String> {
    let mut normalized = policy.clone();
    normalized.normalize_in_place();
    normalized.validate_basic()?;

    for source in &normalized.wake.on {
        if !matches!(
            source,
            AutoLifecycleWakeSource::Access | AutoLifecycleWakeSource::Command
        ) {
            return Err("only wake.on=['access', 'command'] is supported in v1".to_string());
        }
    }

    for (name, condition) in &normalized.condition_sets {
        match *name {
            ACCESS_IDLE_CONDITION_SET => {
                if condition.kind != AutoLifecycleConditionKind::Inactivity
                    || condition.activity_source != Some(AutoLifecycleActivitySource::Access)
                {
                    return Err(
                        "access_idle must be an inactivity condition with activity_source='access'"
                            .to_string(),
                    );
                }
            }
            COMMAND_IDLE_CONDITION_SET => {
                if condition.kind != AutoLifecycleConditionKind::Inactivity
                    || condition.activity_source != Some(AutoLifecycleActivitySource::Command)
                {
                    return Err(
                        "command_idle must be an inactivity condition with activity_source='command'"
                            .to_string(),
                    );
                }
            }
            GUEST_ACTIVITY_CONDITION_SET => {
                if condition.kind != AutoLifecycleConditionKind::GuestActivity
                    || condition.activity_source.is_some()
                {
                    return Err(
                        "guest_activity must be a guest_activity condition without activity_source"
                            .to_string(),
                    );
                }
            }
        }
    }

    Ok(())
}

pub fn policy_from_proto(
    policy: &sbx_proto::AutoLifecyclePolicy,
) -> Result<AutoLifecyclePolicy, Status> {
    let policy = AutoLifecyclePolicy::try_from(policy).map_err(Status::from)?;
    validate_v1_policy(&policy).map_err(Status::invalid_argument)?;
    Ok(policy)
}

pub fn policy_to_proto(policy: &AutoLifecyclePolicy) -> sbx_proto::AutoLifecyclePolicy {
    sbx_proto::AutoLifecyclePolicy::from(policy)
}

pub fn status_from_proto(
    status: &sbx_proto::AutoLifecycleStatus,
) -> Result<AutoLifecycleStatus, Status> {
    AutoLifecycleStatus::try_from(status).map_err(Status::from)
}

pub fn runtime_state_from_proto(
    status: &sbx_proto::AutoLifecycleStatus,
) -> Result<AutoLifecycleRuntimeState, Status> {
    sbx_auto_lifecycle::runtime_state_from_proto(status).map_err(Status::from)
}

pub fn runtime_state_from_status(status: &AutoLifecycleStatus) -> AutoLifecycleRuntimeState {
    sbx_auto_lifecycle::runtime_state_from_status(status)
}
