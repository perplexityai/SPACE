use pplx_space_common_core::types::{
    AutoLifecycleActivitySource, AutoLifecycleConditionRuntimeState, AutoLifecycleConditionSet,
    AutoLifecycleConditionSetId, AutoLifecycleConditionStatus, AutoLifecycleIdleAction,
    AutoLifecyclePolicy, AutoLifecycleRuntimeState, AutoLifecycleStage, AutoLifecycleStatus,
    AutoLifecycleTransition, AutoLifecycleWakePolicy, AutoLifecycleWakeSource, SimpleDuration,
};
use prost_types::{Duration as ProtoDuration, Timestamp as ProtoTimestamp};

use crate::proto;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct AutoLifecycleProtoError {
    message: String,
}

impl AutoLifecycleProtoError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<AutoLifecycleProtoError> for tonic::Status {
    fn from(error: AutoLifecycleProtoError) -> Self {
        tonic::Status::invalid_argument(error.to_string())
    }
}

pub fn whole_seconds_from_proto(
    duration: &ProtoDuration,
    field: &'static str,
) -> Result<u64, AutoLifecycleProtoError> {
    if duration.seconds <= 0 || duration.nanos != 0 {
        return Err(AutoLifecycleProtoError::new(format!(
            "{field} must be a positive whole-second duration"
        )));
    }
    Ok(duration.seconds.cast_unsigned())
}

pub fn whole_seconds_to_proto(seconds: u64) -> ProtoDuration {
    ProtoDuration {
        seconds: seconds as i64,
        nanos: 0,
    }
}

pub fn simple_duration_from_proto(
    duration: &ProtoDuration,
    field: &'static str,
) -> Result<SimpleDuration, AutoLifecycleProtoError> {
    if duration.seconds < 0 || duration.nanos < 0 {
        return Err(AutoLifecycleProtoError::new(format!(
            "{field} must be a positive duration"
        )));
    }
    let duration = std::time::Duration::new(
        duration.seconds.cast_unsigned(),
        duration.nanos.cast_unsigned(),
    );
    SimpleDuration::try_from(duration)
        .map_err(|_| AutoLifecycleProtoError::new(format!("{field} must be a positive duration")))
}

pub fn simple_duration_to_proto(duration: SimpleDuration) -> ProtoDuration {
    let duration = duration.as_duration();
    ProtoDuration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn timestamp_from_secs(seconds: u64) -> ProtoTimestamp {
    ProtoTimestamp {
        seconds: seconds as i64,
        nanos: 0,
    }
}

fn timestamp_to_secs(
    timestamp: &ProtoTimestamp,
    field: &'static str,
) -> Result<u64, AutoLifecycleProtoError> {
    if timestamp.seconds < 0 || timestamp.nanos != 0 {
        return Err(AutoLifecycleProtoError::new(format!(
            "{field} must be a non-negative whole-second timestamp"
        )));
    }
    Ok(timestamp.seconds.cast_unsigned())
}

pub fn auto_lifecycle_transition_from_proto(
    value: i32,
) -> Result<AutoLifecycleTransition, AutoLifecycleProtoError> {
    match proto::AutoLifecycleTransition::try_from(value) {
        Ok(proto::AutoLifecycleTransition::Noop) => Ok(AutoLifecycleTransition::Noop),
        Ok(proto::AutoLifecycleTransition::Pause) => Ok(AutoLifecycleTransition::Pause),
        Ok(proto::AutoLifecycleTransition::Suspend) => Ok(AutoLifecycleTransition::Suspend),
        Ok(proto::AutoLifecycleTransition::Stop) => Ok(AutoLifecycleTransition::Stop),
        Ok(proto::AutoLifecycleTransition::Delete) => Ok(AutoLifecycleTransition::Delete),
        Ok(proto::AutoLifecycleTransition::Unspecified) => Err(AutoLifecycleProtoError::new(
            "auto lifecycle last_auto_transition is unspecified",
        )),
        Err(_) => Err(AutoLifecycleProtoError::new(
            "auto lifecycle last_auto_transition is unknown",
        )),
    }
}

pub fn auto_lifecycle_transition_to_proto(transition: AutoLifecycleTransition) -> i32 {
    match transition {
        AutoLifecycleTransition::Noop => proto::AutoLifecycleTransition::Noop as i32,
        AutoLifecycleTransition::Pause => proto::AutoLifecycleTransition::Pause as i32,
        AutoLifecycleTransition::Suspend => proto::AutoLifecycleTransition::Suspend as i32,
        AutoLifecycleTransition::Stop => proto::AutoLifecycleTransition::Stop as i32,
        AutoLifecycleTransition::Delete => proto::AutoLifecycleTransition::Delete as i32,
    }
}

fn condition_set_id_from_proto(
    value: i32,
    field: &'static str,
) -> Result<AutoLifecycleConditionSetId, AutoLifecycleProtoError> {
    match proto::AutoLifecycleConditionSetId::try_from(value) {
        Ok(proto::AutoLifecycleConditionSetId::AccessIdle) => {
            Ok(AutoLifecycleConditionSetId::AccessIdle)
        }
        Ok(proto::AutoLifecycleConditionSetId::CommandIdle) => {
            Ok(AutoLifecycleConditionSetId::CommandIdle)
        }
        Ok(proto::AutoLifecycleConditionSetId::GuestActivity) => {
            Ok(AutoLifecycleConditionSetId::GuestActivity)
        }
        Ok(proto::AutoLifecycleConditionSetId::Unspecified) => Err(AutoLifecycleProtoError::new(
            format!("{field} must not be unspecified"),
        )),
        Err(_) => Err(AutoLifecycleProtoError::new(format!(
            "unknown {field} value {value}"
        ))),
    }
}

fn condition_set_id_to_proto(condition_set: AutoLifecycleConditionSetId) -> i32 {
    match condition_set {
        AutoLifecycleConditionSetId::AccessIdle => {
            proto::AutoLifecycleConditionSetId::AccessIdle as i32
        }
        AutoLifecycleConditionSetId::CommandIdle => {
            proto::AutoLifecycleConditionSetId::CommandIdle as i32
        }
        AutoLifecycleConditionSetId::GuestActivity => {
            proto::AutoLifecycleConditionSetId::GuestActivity as i32
        }
    }
}

impl TryFrom<&proto::AutoLifecyclePolicy> for AutoLifecyclePolicy {
    type Error = AutoLifecycleProtoError;

    fn try_from(policy: &proto::AutoLifecyclePolicy) -> Result<Self, Self::Error> {
        let wake = AutoLifecycleWakePolicy {
            on: policy
                .wake
                .as_ref()
                .map(|wake| {
                    wake.on
                        .iter()
                        .map(
                            |source| match proto::AutoLifecycleWakeSource::try_from(*source) {
                                Ok(proto::AutoLifecycleWakeSource::Access) => {
                                    Ok(AutoLifecycleWakeSource::Access)
                                }
                                Ok(proto::AutoLifecycleWakeSource::Command) => {
                                    Ok(AutoLifecycleWakeSource::Command)
                                }
                                Ok(proto::AutoLifecycleWakeSource::Unspecified) => {
                                    Err(AutoLifecycleProtoError::new(
                                        "auto lifecycle wake source must not be unspecified",
                                    ))
                                }
                                Err(_) => Err(AutoLifecycleProtoError::new(format!(
                                    "unknown auto lifecycle wake source value {source}"
                                ))),
                            },
                        )
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default(),
        };

        let mut condition_sets = std::collections::BTreeMap::new();
        for condition in &policy.condition_sets {
            let kind = match proto::AutoLifecycleConditionKind::try_from(condition.kind)
                .unwrap_or(proto::AutoLifecycleConditionKind::Unspecified)
            {
                proto::AutoLifecycleConditionKind::Inactivity => {
                    pplx_space_common_core::types::AutoLifecycleConditionKind::Inactivity
                }
                proto::AutoLifecycleConditionKind::GuestActivity => {
                    pplx_space_common_core::types::AutoLifecycleConditionKind::GuestActivity
                }
                proto::AutoLifecycleConditionKind::Unspecified => {
                    return Err(AutoLifecycleProtoError::new(format!(
                        "auto lifecycle condition '{}' must specify kind",
                        condition_set_id_from_proto(
                            condition.id,
                            "auto lifecycle condition set id"
                        )
                        .map(|id| id.to_string())
                        .unwrap_or_else(|_| "<invalid>".to_string())
                    )));
                }
            };
            let activity_source = match condition.activity_source {
                Some(source) => match proto::AutoLifecycleActivitySource::try_from(source) {
                    Ok(proto::AutoLifecycleActivitySource::Access) => {
                        Some(AutoLifecycleActivitySource::Access)
                    }
                    Ok(proto::AutoLifecycleActivitySource::Command) => {
                        Some(AutoLifecycleActivitySource::Command)
                    }
                    Ok(proto::AutoLifecycleActivitySource::Unspecified) => None,
                    Err(_) => {
                        return Err(AutoLifecycleProtoError::new(format!(
                            "unknown auto lifecycle activity source value {source}"
                        )));
                    }
                },
                None => None,
            };
            let id = condition_set_id_from_proto(condition.id, "auto lifecycle condition set id")?;
            let max_guest_idle_extension = condition
                .max_guest_idle_extension
                .as_ref()
                .map(|duration| {
                    simple_duration_from_proto(duration, "auto lifecycle max_guest_idle_extension")
                })
                .transpose()?;
            if condition_sets
                .insert(
                    id,
                    AutoLifecycleConditionSet {
                        kind,
                        activity_source,
                        max_guest_idle_extension,
                    },
                )
                .is_some()
            {
                return Err(AutoLifecycleProtoError::new(format!(
                    "duplicate auto lifecycle condition set '{id}'"
                )));
            }
        }

        let mut stages = Vec::new();
        for stage in &policy.stages {
            let duration = stage.at.as_ref().ok_or_else(|| {
                AutoLifecycleProtoError::new("auto lifecycle stage duration is required")
            })?;
            let at = whole_seconds_from_proto(duration, "auto lifecycle stage at")?;
            let action = match proto::AutoLifecycleIdleAction::try_from(stage.action)
                .unwrap_or(proto::AutoLifecycleIdleAction::Unspecified)
            {
                proto::AutoLifecycleIdleAction::None => AutoLifecycleIdleAction::None,
                proto::AutoLifecycleIdleAction::Pause => AutoLifecycleIdleAction::Pause,
                proto::AutoLifecycleIdleAction::Suspend => AutoLifecycleIdleAction::Suspend,
                proto::AutoLifecycleIdleAction::Stop => AutoLifecycleIdleAction::Stop,
                proto::AutoLifecycleIdleAction::Delete => AutoLifecycleIdleAction::Delete,
                proto::AutoLifecycleIdleAction::Unspecified => {
                    return Err(AutoLifecycleProtoError::new(
                        "auto lifecycle stage action is required",
                    ));
                }
            };
            let condition_set = condition_set_id_from_proto(
                stage.condition_set,
                "auto lifecycle stage condition set",
            )?;
            stages.push(AutoLifecycleStage {
                condition_set,
                at,
                action,
            });
        }

        let mut policy = AutoLifecyclePolicy {
            wake,
            condition_sets,
            stages,
        };
        policy.normalize_in_place();
        policy
            .validate_basic()
            .map_err(AutoLifecycleProtoError::new)?;
        Ok(policy)
    }
}

impl TryFrom<proto::AutoLifecyclePolicy> for AutoLifecyclePolicy {
    type Error = AutoLifecycleProtoError;

    fn try_from(policy: proto::AutoLifecyclePolicy) -> Result<Self, Self::Error> {
        Self::try_from(&policy)
    }
}

impl From<&AutoLifecyclePolicy> for proto::AutoLifecyclePolicy {
    fn from(policy: &AutoLifecyclePolicy) -> Self {
        proto::AutoLifecyclePolicy {
            wake: Some(proto::AutoLifecycleWakePolicy {
                on: policy
                    .wake
                    .on
                    .iter()
                    .map(|source| match source {
                        AutoLifecycleWakeSource::Access => {
                            proto::AutoLifecycleWakeSource::Access as i32
                        }
                        AutoLifecycleWakeSource::Command => {
                            proto::AutoLifecycleWakeSource::Command as i32
                        }
                    })
                    .collect(),
            }),
            condition_sets: policy
                .condition_sets
                .iter()
                .map(|(id, condition)| proto::AutoLifecycleConditionSet {
                    id: condition_set_id_to_proto(*id),
                    kind: match condition.kind {
                        pplx_space_common_core::types::AutoLifecycleConditionKind::Inactivity => {
                            proto::AutoLifecycleConditionKind::Inactivity as i32
                        }
                        pplx_space_common_core::types::AutoLifecycleConditionKind::GuestActivity => {
                            proto::AutoLifecycleConditionKind::GuestActivity as i32
                        }
                    },
                    activity_source: condition.activity_source.map(|source| match source {
                        AutoLifecycleActivitySource::Access => {
                            proto::AutoLifecycleActivitySource::Access as i32
                        }
                        AutoLifecycleActivitySource::Command => {
                            proto::AutoLifecycleActivitySource::Command as i32
                        }
                    }),
                    max_guest_idle_extension: condition
                        .max_guest_idle_extension
                        .map(simple_duration_to_proto),
                })
                .collect(),
            stages: policy
                .stages
                .iter()
                .map(|stage| proto::AutoLifecycleStage {
                    condition_set: condition_set_id_to_proto(stage.condition_set),
                    at: Some(whole_seconds_to_proto(stage.at)),
                    action: match stage.action {
                        AutoLifecycleIdleAction::None => proto::AutoLifecycleIdleAction::None as i32,
                        AutoLifecycleIdleAction::Pause => {
                            proto::AutoLifecycleIdleAction::Pause as i32
                        }
                        AutoLifecycleIdleAction::Suspend => {
                            proto::AutoLifecycleIdleAction::Suspend as i32
                        }
                        AutoLifecycleIdleAction::Stop => proto::AutoLifecycleIdleAction::Stop as i32,
                        AutoLifecycleIdleAction::Delete => {
                            proto::AutoLifecycleIdleAction::Delete as i32
                        }
                    },
                })
                .collect(),
        }
    }
}

impl From<AutoLifecyclePolicy> for proto::AutoLifecyclePolicy {
    fn from(policy: AutoLifecyclePolicy) -> Self {
        Self::from(&policy)
    }
}

impl TryFrom<&proto::AutoLifecycleStatus> for AutoLifecycleStatus {
    type Error = AutoLifecycleProtoError;

    fn try_from(status: &proto::AutoLifecycleStatus) -> Result<Self, Self::Error> {
        let mut conditions = std::collections::BTreeMap::new();
        for condition in &status.conditions {
            conditions.insert(
                condition_set_id_from_proto(
                    condition.condition_set,
                    "auto lifecycle condition status condition set",
                )?,
                AutoLifecycleConditionStatus {
                    armed: condition.armed,
                    epoch_at: condition
                        .epoch_at
                        .as_ref()
                        .map(|timestamp| timestamp_to_secs(timestamp, "auto lifecycle epoch_at"))
                        .transpose()?,
                    last_touched_at: condition
                        .last_touched_at
                        .as_ref()
                        .map(|timestamp| {
                            timestamp_to_secs(timestamp, "auto lifecycle last_touched_at")
                        })
                        .transpose()?,
                },
            );
        }

        Ok(AutoLifecycleStatus {
            conditions,
            next_auto_event_at: status
                .next_auto_event_at
                .as_ref()
                .map(|timestamp| timestamp_to_secs(timestamp, "auto lifecycle next_auto_event_at"))
                .transpose()?,
            open_activity_leases: status.open_activity_leases,
            last_auto_transition: status
                .last_auto_transition
                .map(auto_lifecycle_transition_from_proto)
                .transpose()?,
            last_auto_transition_at: status
                .last_auto_transition_at
                .as_ref()
                .map(|timestamp| {
                    timestamp_to_secs(timestamp, "auto lifecycle last_auto_transition_at")
                })
                .transpose()?,
            last_auto_error: status.last_auto_error.clone(),
            last_access_at: status
                .last_access_at
                .as_ref()
                .map(|timestamp| timestamp_to_secs(timestamp, "auto lifecycle last_access_at"))
                .transpose()?,
            last_command_at: status
                .last_command_at
                .as_ref()
                .map(|timestamp| timestamp_to_secs(timestamp, "auto lifecycle last_command_at"))
                .transpose()?,
        })
    }
}

impl TryFrom<proto::AutoLifecycleStatus> for AutoLifecycleStatus {
    type Error = AutoLifecycleProtoError;

    fn try_from(status: proto::AutoLifecycleStatus) -> Result<Self, Self::Error> {
        Self::try_from(&status)
    }
}

impl From<&AutoLifecycleStatus> for proto::AutoLifecycleStatus {
    fn from(status: &AutoLifecycleStatus) -> Self {
        proto::AutoLifecycleStatus {
            conditions: status
                .conditions
                .iter()
                .map(|(id, condition)| proto::AutoLifecycleConditionStatus {
                    condition_set: condition_set_id_to_proto(*id),
                    armed: condition.armed,
                    epoch_at: condition.epoch_at.map(timestamp_from_secs),
                    last_touched_at: condition.last_touched_at.map(timestamp_from_secs),
                })
                .collect(),
            next_auto_event_at: status.next_auto_event_at.map(timestamp_from_secs),
            open_activity_leases: status.open_activity_leases,
            last_auto_transition: status
                .last_auto_transition
                .map(auto_lifecycle_transition_to_proto),
            last_auto_transition_at: status.last_auto_transition_at.map(timestamp_from_secs),
            last_auto_error: status.last_auto_error.clone(),
            last_access_at: status.last_access_at.map(timestamp_from_secs),
            last_command_at: status.last_command_at.map(timestamp_from_secs),
        }
    }
}

impl From<AutoLifecycleStatus> for proto::AutoLifecycleStatus {
    fn from(status: AutoLifecycleStatus) -> Self {
        Self::from(&status)
    }
}

pub fn runtime_state_from_status(status: &AutoLifecycleStatus) -> AutoLifecycleRuntimeState {
    AutoLifecycleRuntimeState {
        // A replacement execution does not inherit the previous runtime's floor.
        access_extended_until: None,
        conditions: status
            .conditions
            .iter()
            .map(|(name, condition)| {
                (
                    *name,
                    AutoLifecycleConditionRuntimeState {
                        armed: condition.armed,
                        epoch_at: condition.epoch_at,
                        last_touched_at: condition.last_touched_at,
                        last_fired_stage_index: None,
                    },
                )
            })
            .collect(),
        next_auto_event_at: status.next_auto_event_at,
        open_activity_leases: status.open_activity_leases,
        last_auto_transition: status.last_auto_transition,
        last_auto_transition_at: status.last_auto_transition_at,
        last_auto_error: status.last_auto_error.clone(),
        last_access_at: status.last_access_at,
        last_command_at: status.last_command_at,
        // Not part of the public status; a handoff restarts the retry budget.
        auto_error_streak: 0,
    }
}

pub fn runtime_state_from_proto(
    status: &proto::AutoLifecycleStatus,
) -> Result<AutoLifecycleRuntimeState, AutoLifecycleProtoError> {
    let status = AutoLifecycleStatus::try_from(status)?;
    Ok(runtime_state_from_status(&status))
}

#[cfg(test)]
mod tests;
