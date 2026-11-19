use serde::{Deserialize, Serialize};

use crate::{ArtifactDigest, artifact::valid_label};

/// Rollback is deployment authorization, independent of permission to kill work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionIntent {
    Upgrade,
    Rollback,
}

/// One workspace release history. Deployment policy selects immutable bytes;
/// the marker identifies the last release worth restarting existing guests for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Promotion {
    pub channel: String,
    pub target: ArtifactDigest,
    pub target_version: semver::Version,
    pub guest_update_required_since: semver::Version,
    pub intent: SelectionIntent,
    pub minimum_host: semver::Version,
    pub maximum_guest: semver::Version,
}

#[derive(Debug, thiserror::Error)]
pub enum PromotionError {
    #[error("invalid release selection or guest-update marker")]
    Invalid,
    #[error("stale release selection or unauthorized channel change")]
    Stale,
    #[error("one workspace release names conflicting artifact identity")]
    Conflict,
}

impl Promotion {
    /// `previous` is the highest accepted forward release, preserved on rollback.
    pub fn accept_after(&self, previous: Option<&Self>) -> Result<(), PromotionError> {
        if !valid_label(&self.channel)
            || !self.target_version.pre.is_empty()
            || !self.target_version.build.is_empty()
            || !self.guest_update_required_since.pre.is_empty()
            || !self.guest_update_required_since.build.is_empty()
            || self.guest_update_required_since > self.target_version
            || self.target_version > self.maximum_guest
        {
            return Err(PromotionError::Invalid);
        }
        if let Some(previous) = previous {
            if self.channel != previous.channel {
                return Err(PromotionError::Stale);
            }
            if self.target_version == previous.target_version
                && (self.target != previous.target
                    || self.guest_update_required_since != previous.guest_update_required_since)
            {
                return Err(PromotionError::Conflict);
            }
            if self.intent == SelectionIntent::Upgrade
                && (self.target_version < previous.target_version
                    || self.guest_update_required_since < previous.guest_update_required_since)
            {
                return Err(PromotionError::Stale);
            }
        }
        Ok(())
    }

    pub fn should_install(
        &self,
        guest: &semver::Version,
        digest: ArtifactDigest,
    ) -> Result<bool, PromotionError> {
        self.accept_after(None)?;
        if digest == self.target {
            return Ok(false);
        }
        if guest == &self.target_version {
            return Err(PromotionError::Conflict);
        }
        Ok(match self.intent {
            SelectionIntent::Upgrade => {
                self.target_version > *guest && self.guest_update_required_since > *guest
            }
            SelectionIntent::Rollback => self.target_version < *guest,
        })
    }
}
