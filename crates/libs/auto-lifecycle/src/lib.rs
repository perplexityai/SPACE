//! Sandbox auto-lifecycle policy types.
//!
//! [`AutoLifecyclePolicy`] says which activity wakes a sandbox and which
//! idle actions (pause, suspend, stop, delete) fire after how long, per
//! condition set. The crate also holds the stage-chain shorthand
//! (`pause@5m,delete@1d`), the per-sandbox runtime state that tracks activity
//! and fired stages, and the public status derived from it.
//!
//! It is its own crate so that code can name these types without depending on
//! `pplx-space-common-core`. `pplx-space-common-core` re-exports it from
//! `types::auto_lifecycle`.

use std::{collections::BTreeMap, fmt, str::FromStr};

use std::time::Duration;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use pplx_space_durations::{SimpleDuration, format_simple_duration, parse_simple_duration_secs};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AutoLifecycleIdleAction {
    #[default]
    None,
    Pause,
    Suspend,
    Stop,
    Delete,
}

impl AutoLifecycleIdleAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pause => "pause",
            Self::Suspend => "suspend",
            Self::Stop => "stop",
            Self::Delete => "delete",
        }
    }

    pub fn dormancy_severity(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Pause => 1,
            Self::Suspend => 2,
            Self::Stop => 3,
            Self::Delete => 4,
        }
    }
}

impl fmt::Display for AutoLifecycleIdleAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleIdleAction {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "none" => Ok(Self::None),
            "pause" => Ok(Self::Pause),
            "suspend" => Ok(Self::Suspend),
            "stop" => Ok(Self::Stop),
            "delete" => Ok(Self::Delete),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle idle action '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AutoLifecycleWakeSource {
    #[default]
    Access,
    Command,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AutoLifecycleIdleBreakSource {
    #[default]
    Access,
    Command,
    GuestActivity,
}

impl AutoLifecycleIdleBreakSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Command => "command",
            Self::GuestActivity => "guest-activity",
        }
    }
}

impl fmt::Display for AutoLifecycleIdleBreakSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleIdleBreakSource {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "access" => Ok(Self::Access),
            "command" => Ok(Self::Command),
            "guest-activity" | "guest_activity" => Ok(Self::GuestActivity),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle idle break source '{other}'"
            ))),
        }
    }
}

impl AutoLifecycleWakeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Command => "command",
        }
    }
}

impl fmt::Display for AutoLifecycleWakeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleWakeSource {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "access" => Ok(Self::Access),
            "command" => Ok(Self::Command),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle wake source '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AutoLifecycleConditionKind {
    #[default]
    Inactivity,
    GuestActivity,
}

impl AutoLifecycleConditionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inactivity => "inactivity",
            Self::GuestActivity => "guest_activity",
        }
    }
}

impl fmt::Display for AutoLifecycleConditionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleConditionKind {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "inactivity" => Ok(Self::Inactivity),
            "guest_activity" => Ok(Self::GuestActivity),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle condition kind '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AutoLifecycleActivitySource {
    #[default]
    Access,
    Command,
}

impl AutoLifecycleActivitySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Command => "command",
        }
    }
}

impl fmt::Display for AutoLifecycleActivitySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleActivitySource {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "access" => Ok(Self::Access),
            "command" => Ok(Self::Command),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle activity source '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct AutoLifecycleParseError(String);

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AutoLifecycleConditionSetId {
    AccessIdle,
    CommandIdle,
    GuestActivity,
}

impl AutoLifecycleConditionSetId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccessIdle => "access_idle",
            Self::CommandIdle => "command_idle",
            Self::GuestActivity => "guest_activity",
        }
    }
}

impl fmt::Display for AutoLifecycleConditionSetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleConditionSetId {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "access_idle" => Ok(Self::AccessIdle),
            "command_idle" => Ok(Self::CommandIdle),
            "guest_activity" => Ok(Self::GuestActivity),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle condition set '{other}'"
            ))),
        }
    }
}

impl TryFrom<String> for AutoLifecycleConditionSetId {
    type Error = AutoLifecycleParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<AutoLifecycleConditionSetId> for String {
    fn from(value: AutoLifecycleConditionSetId) -> Self {
        value.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum AutoLifecycleTransition {
    Noop,
    Pause,
    Suspend,
    Stop,
    Delete,
}

impl AutoLifecycleTransition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Pause => "pause",
            Self::Suspend => "suspend",
            Self::Stop => "stop",
            Self::Delete => "delete",
        }
    }
}

impl fmt::Display for AutoLifecycleTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AutoLifecycleTransition {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "noop" => Ok(Self::Noop),
            "pause" => Ok(Self::Pause),
            "suspend" => Ok(Self::Suspend),
            "stop" => Ok(Self::Stop),
            "delete" => Ok(Self::Delete),
            other => Err(AutoLifecycleParseError(format!(
                "unsupported auto lifecycle transition '{other}'"
            ))),
        }
    }
}

fn format_duration(value: u64) -> String {
    format_simple_duration(Duration::from_secs(value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AutoLifecycleStageSpec {
    pub at: u64,
    pub action: AutoLifecycleIdleAction,
}

impl fmt::Display for AutoLifecycleStageSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.action, format_duration(self.at))
    }
}

impl FromStr for AutoLifecycleStageSpec {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        let (action, at) = value.split_once('@').ok_or_else(|| {
            AutoLifecycleParseError(format!(
                "invalid auto lifecycle stage '{value}', expected action@duration"
            ))
        })?;

        Ok(Self {
            action: action.parse()?,
            at: parse_simple_duration_secs(at.trim())
                .map_err(|error| AutoLifecycleParseError(error.to_string()))?,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct AutoLifecycleStageChain {
    stages: Vec<AutoLifecycleStageSpec>,
}

impl AutoLifecycleStageChain {
    pub fn new(stages: Vec<AutoLifecycleStageSpec>) -> Self {
        let mut chain = Self { stages };
        chain.normalize_in_place();
        chain
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn stages(&self) -> &[AutoLifecycleStageSpec] {
        &self.stages
    }

    pub fn into_condition_stages(
        self,
        condition_set: AutoLifecycleConditionSetId,
    ) -> Vec<AutoLifecycleStage> {
        self.stages
            .into_iter()
            .map(|stage| AutoLifecycleStage {
                condition_set,
                at: stage.at,
                action: stage.action,
            })
            .collect()
    }

    pub fn from_condition_stages<'a>(
        stages: impl IntoIterator<Item = &'a AutoLifecycleStage>,
    ) -> Self {
        Self::new(
            stages
                .into_iter()
                .map(|stage| AutoLifecycleStageSpec {
                    at: stage.at,
                    action: stage.action,
                })
                .collect(),
        )
    }

    fn normalize_in_place(&mut self) {
        let mut by_at = BTreeMap::<u64, AutoLifecycleIdleAction>::new();
        for stage in self.stages.drain(..) {
            by_at
                .entry(stage.at)
                .and_modify(|current| {
                    if stage.action.dormancy_severity() > current.dormancy_severity() {
                        *current = stage.action;
                    }
                })
                .or_insert(stage.action);
        }

        let mut normalized = Vec::new();
        let mut last_severity = None;
        for (at, action) in by_at {
            let severity = action.dormancy_severity();
            if last_severity.is_some_and(|last| severity <= last) {
                continue;
            }
            normalized.push(AutoLifecycleStageSpec { at, action });
            last_severity = Some(severity);
            if action == AutoLifecycleIdleAction::Delete {
                break;
            }
        }
        self.stages = normalized;
    }
}

impl fmt::Display for AutoLifecycleStageChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered = self
            .stages
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        f.write_str(&rendered)
    }
}

impl FromStr for AutoLifecycleStageChain {
    type Err = AutoLifecycleParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.replace("->", ",");
        let mut stages = Vec::new();
        for segment in normalized.split(',') {
            let segment = segment.trim();
            if segment.is_empty() {
                continue;
            }
            stages.push(segment.parse()?);
        }
        if stages.is_empty() {
            return Err(AutoLifecycleParseError(
                "auto lifecycle stage chain cannot be empty".to_string(),
            ));
        }
        Ok(Self::new(stages))
    }
}

impl TryFrom<String> for AutoLifecycleStageChain {
    type Error = AutoLifecycleParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<AutoLifecycleStageChain> for String {
    fn from(value: AutoLifecycleStageChain) -> Self {
        value.to_string()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleWakePolicy {
    #[cfg_attr(feature = "serde", serde(default))]
    pub on: Vec<AutoLifecycleWakeSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleConditionSet {
    pub kind: AutoLifecycleConditionKind,
    #[cfg_attr(feature = "serde", serde(default))]
    pub activity_source: Option<AutoLifecycleActivitySource>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub max_guest_idle_extension: Option<SimpleDuration>,
}

impl AutoLifecycleConditionSet {
    pub fn access_idle() -> Self {
        Self {
            kind: AutoLifecycleConditionKind::Inactivity,
            activity_source: Some(AutoLifecycleActivitySource::Access),
            max_guest_idle_extension: None,
        }
    }

    pub fn command_idle() -> Self {
        Self {
            kind: AutoLifecycleConditionKind::Inactivity,
            activity_source: Some(AutoLifecycleActivitySource::Command),
            max_guest_idle_extension: None,
        }
    }

    pub fn guest_activity(max_guest_idle_extension: Option<SimpleDuration>) -> Self {
        Self {
            kind: AutoLifecycleConditionKind::GuestActivity,
            activity_source: None,
            max_guest_idle_extension,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleStage {
    pub condition_set: AutoLifecycleConditionSetId,
    #[cfg_attr(feature = "serde", serde(with = "duration_string"))]
    pub at: u64,
    pub action: AutoLifecycleIdleAction,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecyclePolicy {
    #[cfg_attr(feature = "serde", serde(default))]
    pub wake: AutoLifecycleWakePolicy,
    #[cfg_attr(feature = "serde", serde(default))]
    pub condition_sets: BTreeMap<AutoLifecycleConditionSetId, AutoLifecycleConditionSet>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub stages: Vec<AutoLifecycleStage>,
}

impl AutoLifecyclePolicy {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_disabled(&self) -> bool {
        self.wake.on.is_empty() && self.condition_sets.is_empty() && self.stages.is_empty()
    }

    pub fn contains_action(&self, action: AutoLifecycleIdleAction) -> bool {
        self.stages.iter().any(|stage| stage.action == action)
    }

    pub fn normalized(mut self) -> Self {
        self.normalize_in_place();
        self
    }

    pub fn normalize_in_place(&mut self) {
        self.wake.on.sort_unstable_by_key(|source| match source {
            AutoLifecycleWakeSource::Access => 0u8,
            AutoLifecycleWakeSource::Command => 1u8,
        });
        self.wake.on.dedup();
        self.stages.sort_by(|left, right| {
            left.condition_set
                .cmp(&right.condition_set)
                .then(left.at.cmp(&right.at))
                .then(left.action.as_str().cmp(right.action.as_str()))
        });
    }

    pub fn validate_basic(&self) -> Result<(), String> {
        const MAX_STAGES: usize = 100;

        if self.stages.len() > MAX_STAGES {
            return Err(format!(
                "auto lifecycle supports at most {MAX_STAGES} stages"
            ));
        }

        for (name, condition_set) in &self.condition_sets {
            match *name {
                AutoLifecycleConditionSetId::AccessIdle => {
                    if condition_set.kind != AutoLifecycleConditionKind::Inactivity
                        || condition_set.activity_source
                            != Some(AutoLifecycleActivitySource::Access)
                    {
                        return Err(
                            "access_idle must be an inactivity condition with activity_source='access'"
                                .to_string(),
                        );
                    }
                }
                AutoLifecycleConditionSetId::CommandIdle => {
                    if condition_set.kind != AutoLifecycleConditionKind::Inactivity
                        || condition_set.activity_source
                            != Some(AutoLifecycleActivitySource::Command)
                    {
                        return Err(
                            "command_idle must be an inactivity condition with activity_source='command'"
                                .to_string(),
                        );
                    }
                }
                AutoLifecycleConditionSetId::GuestActivity => {
                    if condition_set.kind != AutoLifecycleConditionKind::GuestActivity
                        || condition_set.activity_source.is_some()
                    {
                        return Err(
                            "guest_activity must be a guest_activity condition without activity_source"
                                .to_string(),
                        );
                    }
                }
            }
            match condition_set.kind {
                AutoLifecycleConditionKind::Inactivity => {
                    if condition_set.activity_source.is_none() {
                        return Err(format!(
                            "condition set '{name}' must specify activity_source"
                        ));
                    }
                }
                AutoLifecycleConditionKind::GuestActivity => {
                    if condition_set.activity_source.is_some() {
                        return Err(format!(
                            "condition set '{name}' may not specify activity_source for guest_activity conditions"
                        ));
                    }
                }
            }
            if condition_set.kind != AutoLifecycleConditionKind::GuestActivity
                && condition_set.max_guest_idle_extension.is_some()
            {
                return Err(format!(
                    "condition set '{name}' may only set max_guest_idle_extension for guest_activity conditions"
                ));
            }
        }

        let mut last_stage_at: BTreeMap<AutoLifecycleConditionSetId, u64> = BTreeMap::new();
        let mut last_stage_action: BTreeMap<AutoLifecycleConditionSetId, AutoLifecycleIdleAction> =
            BTreeMap::new();
        for stage in &self.stages {
            if stage.at > i64::MAX as u64 {
                return Err("stage delay exceeds the supported deadline range".to_string());
            }
            if stage.at == 0 {
                return Err(format!(
                    "stage for condition set '{}' must have a positive delay",
                    stage.condition_set
                ));
            }
            if !self.condition_sets.contains_key(&stage.condition_set) {
                return Err(format!(
                    "stage references unknown condition set '{}'",
                    stage.condition_set
                ));
            }
            if let Some(previous) = last_stage_at.insert(stage.condition_set, stage.at)
                && previous >= stage.at
            {
                return Err(format!(
                    "stage delays for condition set '{}' must be strictly increasing",
                    stage.condition_set
                ));
            }
            if let Some(previous) = last_stage_action.insert(stage.condition_set, stage.action)
                && previous.dormancy_severity() > stage.action.dormancy_severity()
            {
                return Err(format!(
                    "stage actions for condition set '{}' must be monotonic in dormancy severity",
                    stage.condition_set
                ));
            }
        }

        Ok(())
    }

    pub fn wake_on_access(&self) -> bool {
        self.wake.on.contains(&AutoLifecycleWakeSource::Access)
    }

    pub fn wake_on_command(&self) -> bool {
        self.wake.on.contains(&AutoLifecycleWakeSource::Command)
    }

    pub fn has_stages_for_condition_set(
        &self,
        condition_set: &AutoLifecycleConditionSetId,
    ) -> bool {
        self.stages
            .iter()
            .any(|stage| stage.condition_set == *condition_set)
    }

    pub fn has_guest_activity_stages(&self) -> bool {
        self.condition_sets.iter().any(|(name, condition_set)| {
            condition_set.kind == AutoLifecycleConditionKind::GuestActivity
                && self.has_stages_for_condition_set(name)
        })
    }

    pub fn guest_activity_hold_cap(&self) -> Option<SimpleDuration> {
        self.condition_sets
            .iter()
            .filter_map(|(name, condition_set)| {
                (condition_set.kind == AutoLifecycleConditionKind::GuestActivity
                    && self.has_stages_for_condition_set(name))
                .then_some(condition_set.max_guest_idle_extension)
                .flatten()
            })
            .min()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleConditionRuntimeState {
    #[cfg_attr(feature = "serde", serde(default))]
    pub armed: bool,
    #[cfg_attr(feature = "serde", serde(default))]
    pub epoch_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_touched_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_fired_stage_index: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleRuntimeState {
    /// A connect-request minimum deadline, independent of actual activity.
    #[cfg_attr(feature = "serde", serde(default))]
    pub access_extended_until: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conditions: BTreeMap<AutoLifecycleConditionSetId, AutoLifecycleConditionRuntimeState>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_access_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_command_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub next_auto_event_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub open_activity_leases: u32,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_auto_transition: Option<AutoLifecycleTransition>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_auto_transition_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_auto_error: Option<String>,
    /// Consecutive failed auto transitions since `last_auto_error` was last
    /// cleared; drives the bounded suspend retry budget.
    #[cfg_attr(feature = "serde", serde(default))]
    pub auto_error_streak: u32,
}

impl AutoLifecycleRuntimeState {
    pub fn public_status(&self) -> AutoLifecycleStatus {
        AutoLifecycleStatus {
            conditions: self
                .conditions
                .iter()
                .map(|(name, state)| {
                    (
                        *name,
                        AutoLifecycleConditionStatus {
                            armed: state.armed,
                            epoch_at: state.epoch_at,
                            last_touched_at: state.last_touched_at,
                        },
                    )
                })
                .collect(),
            last_access_at: self.last_access_at,
            last_command_at: self.last_command_at,
            next_auto_event_at: self.next_auto_event_at,
            open_activity_leases: self.open_activity_leases,
            last_auto_transition: self.last_auto_transition,
            last_auto_transition_at: self.last_auto_transition_at,
            last_auto_error: self.last_auto_error.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleConditionStatus {
    #[cfg_attr(feature = "serde", serde(default))]
    pub armed: bool,
    #[cfg_attr(feature = "serde", serde(default))]
    pub epoch_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_touched_at: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AutoLifecycleStatus {
    #[cfg_attr(feature = "serde", serde(default))]
    pub conditions: BTreeMap<AutoLifecycleConditionSetId, AutoLifecycleConditionStatus>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_access_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_command_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub next_auto_event_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub open_activity_leases: u32,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_auto_transition: Option<AutoLifecycleTransition>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_auto_transition_at: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_auto_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "mode", rename_all = "snake_case"))]
pub enum SandboxAutoLifecyclePatch {
    Set { policy: AutoLifecyclePolicy },
    InheritTemplateDefault,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateAutoLifecycleSetting {
    Explicit(AutoLifecyclePolicy),
    InheritClusterDefault,
}

#[cfg(feature = "serde")]
mod duration_string {
    use super::{format_duration, parse_simple_duration_secs};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_duration(*value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        parse_simple_duration_secs(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests;
