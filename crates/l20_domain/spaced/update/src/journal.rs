//! Schema 1 is the permanent selector ABI for this seed contract. Additional
//! diagnostic fields may be ignored; changing decision semantics or required
//! fields requires a new seed contract, not merely a schema-number bump.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ArtifactDigest;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Slot {
    A,
    B,
}

impl Slot {
    pub const fn name(self) -> &'static str {
        match self {
            Self::A => "a",
            Self::B => "b",
        }
    }
    pub const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
    pub const fn selector(self) -> &'static str {
        match self {
            Self::A => "slots/a",
            Self::B => "slots/b",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Selection {
    pub slot: Slot,
    pub digest: ArtifactDigest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrialProgress {
    Starting,
    AwaitingHost,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attempt {
    pub operation: Uuid,
    pub execution: Uuid,
    pub boot: Uuid,
    pub incarnation: Uuid,
    pub configuration_revision: u64,
    pub target: Selection,
    /// CLOCK_BOOTTIME milliseconds, meaningful only with `boot`.
    pub confirmation_deadline_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Restart {
    pub operation: Uuid,
    pub execution: Uuid,
    pub boot: Uuid,
    pub incarnation: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum JournalState {
    Stable,
    Staged {
        attempt: Attempt,
    },
    Armed {
        attempt: Attempt,
    },
    Trial {
        attempt: Attempt,
        progress: TrialProgress,
    },
    Committed {
        attempt: Attempt,
    },
    RolledBack {
        attempt: Attempt,
        progress: TrialProgress,
    },
    /// The committed selector was usable, but journal authority was lost.
    /// Committed bytes verified, but replay history is lost. Maintenance is
    /// disabled until the execution is retired and recreated from a template;
    /// never reset this to Stable in a running guest.
    RecoveryRequired,
    Restarting {
        restart: Restart,
    },
    Restarted {
        restart: Restart,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Journal {
    pub schema: u32,
    pub committed: Selection,
    pub state: JournalState,
}

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("unsupported or inconsistent update journal")]
    Invalid,
    #[error("update operation, incarnation or configuration fence does not match")]
    Fence,
    #[error("update trial confirmation deadline expired")]
    Expired,
    #[error("update transition is not valid in the current phase")]
    Phase,
}

impl Journal {
    pub fn stable(committed: Selection) -> Self {
        Self {
            schema: 1,
            committed,
            state: JournalState::Stable,
        }
    }

    pub fn validate(&self) -> Result<(), JournalError> {
        if self.schema != 1 {
            return Err(JournalError::Invalid);
        }
        let (attempt, committed) = match &self.state {
            JournalState::Restarting { restart } | JournalState::Restarted { restart } => {
                return if [
                    restart.operation,
                    restart.execution,
                    restart.boot,
                    restart.incarnation,
                ]
                .iter()
                .any(Uuid::is_nil)
                {
                    Err(JournalError::Invalid)
                } else {
                    Ok(())
                };
            }
            JournalState::Stable | JournalState::RecoveryRequired => return Ok(()),
            JournalState::Committed { attempt } => (attempt, true),
            JournalState::Staged { attempt }
            | JournalState::Armed { attempt }
            | JournalState::Trial { attempt, .. }
            | JournalState::RolledBack { attempt, .. } => (attempt, false),
        };
        if [
            attempt.operation,
            attempt.execution,
            attempt.boot,
            attempt.incarnation,
        ]
        .iter()
        .any(Uuid::is_nil)
            || attempt.confirmation_deadline_ms == 0
            || (committed && attempt.target != self.committed)
            || (!committed && attempt.target.slot == self.committed.slot)
        {
            return Err(JournalError::Invalid);
        }
        Ok(())
    }

    /// Called once by ExecStartPre. Persist the returned journal before changing
    /// current: a crash after consumption loses an attempt, never duplicates one.
    pub fn select(&mut self, boot: Uuid, now_ms: u64) -> Result<Selection, JournalError> {
        self.validate()?;
        match self.state.clone() {
            JournalState::Restarting { restart } => {
                self.state = JournalState::Restarted { restart };
                Ok(self.committed.clone())
            }
            JournalState::Armed { mut attempt }
                if attempt.boot == boot && now_ms < attempt.confirmation_deadline_ms =>
            {
                attempt.confirmation_deadline_ms = attempt.confirmation_deadline_ms.min(
                    now_ms.saturating_add(crate::TRIAL_CONFIRMATION_WINDOW.as_millis() as u64),
                );
                let selected = attempt.target.clone();
                self.state = JournalState::Trial {
                    attempt,
                    progress: TrialProgress::Starting,
                };
                Ok(selected)
            }
            JournalState::Armed { attempt } => {
                self.state = JournalState::RolledBack {
                    attempt,
                    progress: TrialProgress::Starting,
                };
                Ok(self.committed.clone())
            }
            JournalState::Trial { attempt, progress } => {
                self.state = JournalState::RolledBack { attempt, progress };
                Ok(self.committed.clone())
            }
            _ => Ok(self.committed.clone()),
        }
    }

    pub fn arm(&mut self, fence: &Attempt) -> Result<(), JournalError> {
        self.validate()?;
        match &self.state {
            JournalState::Staged { attempt } if attempt == fence => {
                self.state = JournalState::Armed {
                    attempt: attempt.clone(),
                };
                Ok(())
            }
            JournalState::Armed { attempt } if attempt == fence => Ok(()),
            _ => Err(JournalError::Phase),
        }
    }

    pub fn self_test_passed(&mut self, fence: &Attempt) -> Result<(), JournalError> {
        match &mut self.state {
            JournalState::Trial { attempt, progress } if attempt == fence => {
                *progress = TrialProgress::AwaitingHost;
                Ok(())
            }
            _ => Err(JournalError::Fence),
        }
    }

    pub fn commit(&mut self, fence: &Attempt, boot: Uuid, now_ms: u64) -> Result<(), JournalError> {
        self.validate()?;
        if let JournalState::Committed { attempt } = &self.state {
            return if attempt == fence {
                Ok(())
            } else {
                Err(JournalError::Fence)
            };
        }
        let JournalState::Trial {
            attempt,
            progress: TrialProgress::AwaitingHost,
        } = &self.state
        else {
            return Err(JournalError::Phase);
        };
        if attempt != fence || attempt.boot != boot {
            return Err(JournalError::Fence);
        }
        if now_ms >= attempt.confirmation_deadline_ms {
            return Err(JournalError::Expired);
        }
        self.committed = attempt.target.clone();
        self.state = JournalState::Committed {
            attempt: attempt.clone(),
        };
        Ok(())
    }
}
