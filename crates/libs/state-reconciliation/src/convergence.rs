//! Convergence episodes: the shared vocabulary and decision rule for a retry
//! loop that keeps failing to converge one subject.
//!
//! An episode is keyed by (loop, subject) and opens on the first attempt that
//! does not converge, or when a caller seeds one with [`Episode::from_parts`].
//! It records when its age starts, how many counted attempts it has made, and
//! why the latest one did not converge. [`observe`] folds one attempt's
//! [`Outcome`] into the episode and returns the loop's next step as a
//! [`Verdict`]. It is pure: callers own storage, clocks, metrics and the
//! escalation's side effect.
//!
//! The rules:
//!
//! - Only success evidence ([`Outcome::Converged`]) or the subject
//!   disappearing ([`Outcome::SubjectGone`]) clears an episode. There is no
//!   outcome for a re-enqueue, so requeueing a subject cannot reset its
//!   history.
//! - A changed reason updates `reason` and `reason_since` but never resets
//!   `attempts` or `first_seen_at`: a subject that alternates between two
//!   failures is still one stuck subject.
//! - An outage ([`Outcome::Outage`]) neither extends nor ends an episode and
//!   never opens one, so an outage can neither escalate a subject nor hide one
//!   that was already stuck. Age is wall-clock time since `first_seen_at` and
//!   includes outages, so the first counted attempt after an outage escalates
//!   an episode that has not escalated yet if, counting that attempt, both
//!   bounds hold.
//! - Escalation needs both `age >= escalate_after` and
//!   `attempts >= min_attempts`, and happens once per episode, on a counted
//!   attempt. After it the loop keeps observing at `park_interval`; nothing
//!   stops observing.
//! - Callers keep one writer per (loop, subject) at a time, so two concurrent
//!   attempts cannot both escalate. Either serialize reading the episode,
//!   calling [`observe`] and storing the result (a row lock or a single-writer
//!   loop), or store with a compare-and-set on the episode that was read. With
//!   a compare-and-set, both attempts can see [`Verdict::Escalate`] but only
//!   one store succeeds: apply the escalation in that store's transaction, or
//!   only after it succeeds.

use std::{fmt, str::FromStr, time::Duration, time::SystemTime};

use thiserror::Error;

/// Longest interval a [`Policy`] accepts, so every deadline [`observe`]
/// computes from a real clock reading is representable.
pub const MAX_POLICY_INTERVAL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Implements `ALL`, `as_str`, `Display` and `FromStr` for a closed wire enum.
macro_rules! wire_enum {
    ($name:ident, $parse_error:ident, $what:literal, { $($variant:ident => $wire:literal,)+ }) => {
        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        #[derive(Clone, Debug, Eq, PartialEq, Error)]
        #[error("unknown {} {:?}", $what, .0)]
        pub struct $parse_error(String);

        impl FromStr for $name {
            type Err = $parse_error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($wire => Ok(Self::$variant),)+
                    other => Err($parse_error(other.to_owned())),
                }
            }
        }
    };
}

/// The retry loop that owns an episode. Episodes key by (loop, subject), so
/// two loops retrying the same subject keep separate histories.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ConvergenceLoop {
    /// The API's Kubernetes wake path. A start, restore or started recovery
    /// that cannot proceed defers or fails the wake, and the next attempt
    /// comes from whoever wakes the sandbox again.
    KubernetesWake,
    /// The node manager's network slot reset.
    NodeSlotReset,
    /// The sandbox manager's re-drive of an unfinished teardown suspend.
    TeardownSuspendRedrive,
}

wire_enum!(ConvergenceLoop, ConvergenceLoopParseError, "convergence loop", {
    KubernetesWake => "kubernetes_wake",
    NodeSlotReset => "node_slot_reset",
    TeardownSuspendRedrive => "teardown_suspend_redrive",
});

/// Why the latest counted attempt did not converge its subject. A reason is
/// not tied to one loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NonConvergenceReason {
    /// A pod from an earlier execution generation is still present, so the
    /// wake waits for it to go.
    PriorGenerationPodLingering,
    /// A started recovery found no verified restore source for the sandbox,
    /// local or remote. A source that exists but cannot be used yet is a
    /// different condition.
    NoRestoreSource,
    /// A restore onto a node other than the sandbox's source failed.
    CrossNodeRestoreFailed,
    /// A slot reset was deferred because the process that leased the slot is
    /// still alive.
    LeasedProcessAlive,
    /// A teardown suspend had to be driven again because an earlier drive did
    /// not finish it.
    TeardownSuspendUnfinished,
}

wire_enum!(NonConvergenceReason, NonConvergenceReasonParseError, "non-convergence reason", {
    PriorGenerationPodLingering => "prior_generation_pod_lingering",
    NoRestoreSource => "no_restore_source",
    CrossNodeRestoreFailed => "cross_node_restore_failed",
    LeasedProcessAlive => "leased_process_alive",
    TeardownSuspendUnfinished => "teardown_suspend_unfinished",
});

/// What one attempt's outcome does to an episode's run of counted attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RetryEffect {
    /// The loop observed the subject and it has not converged: the attempt
    /// counts, and opens an episode when none is open.
    Extends,
    /// The loop could not observe the subject. Attempts, first-seen time,
    /// reason, error and any escalation stay as they were, and no episode
    /// opens. [`observe`] still moves `next_attempt_at`, so store the episode
    /// it returns, if any.
    Leaves,
    /// The subject converged or is gone: the episode ends.
    Ends,
}

/// The result of one attempt, as the loop classified it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Outcome {
    /// Success evidence: the subject reached the state the loop drives it to.
    Converged,
    /// The subject no longer exists, so there is nothing left to converge.
    SubjectGone,
    /// The loop observed the subject and it has not converged.
    NotConverged {
        reason: NonConvergenceReason,
        /// The attempt's error text, when the attempt failed rather than
        /// deferred.
        error: Option<String>,
    },
    /// The loop could not observe the subject: a dependency it needs to
    /// decide did not answer. Says nothing about the subject.
    Outage,
}

impl Outcome {
    pub const fn effect(&self) -> RetryEffect {
        match self {
            Self::Converged | Self::SubjectGone => RetryEffect::Ends,
            Self::NotConverged { .. } => RetryEffect::Extends,
            Self::Outage => RetryEffect::Leaves,
        }
    }
}

/// What an automatic escalation could destroy, which bounds the escalations
/// allowed for the subject.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SubjectSafety {
    /// Losing the subject loses nothing a user needs.
    Disposable,
    /// The subject may hold the only copy of user state.
    MayHoldOnlyCopy,
    /// Acting on the subject affects isolation, credentials or policy.
    SecurityRelevant,
}

wire_enum!(SubjectSafety, SubjectSafetyParseError, "subject safety", {
    Disposable => "disposable",
    MayHoldOnlyCopy => "may_hold_only_copy",
    SecurityRelevant => "security_relevant",
});

/// What a loop does once, when an episode crosses its policy's bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Escalation {
    /// Stop automatic work on the subject and keep it for inspection. Later
    /// passes, if any, only check whether it converged or is gone.
    QuarantineSubject,
    /// Move the subject to a terminal failed state. Later passes, if any, only
    /// check whether it converged or is gone.
    FailTerminal,
    /// Report the subject to an operator. The escalation changes nothing about
    /// the subject, and later passes make the loop's usual attempts, at the
    /// park interval.
    OperatorOnly,
}

wire_enum!(Escalation, EscalationParseError, "escalation", {
    QuarantineSubject => "quarantine_subject",
    FailTerminal => "fail_terminal",
    OperatorOnly => "operator_only",
});

impl Escalation {
    /// Whether a loop may apply this escalation to a subject of `safety`
    /// without a person deciding. Only a disposable subject may be acted on
    /// automatically.
    pub const fn allowed_for(self, safety: SubjectSafety) -> bool {
        match (self, safety) {
            (Self::OperatorOnly, _) => true,
            (Self::QuarantineSubject | Self::FailTerminal, SubjectSafety::Disposable) => true,
            (
                Self::QuarantineSubject | Self::FailTerminal,
                SubjectSafety::MayHoldOnlyCopy | SubjectSafety::SecurityRelevant,
            ) => false,
        }
    }
}

/// An escalation checked against the subject's safety class. The only way to
/// build one is [`SubjectEscalation::new`], so a [`Policy`] cannot carry an
/// escalation its subjects do not allow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SubjectEscalation {
    safety: SubjectSafety,
    escalation: Escalation,
}

impl SubjectEscalation {
    pub const fn new(
        safety: SubjectSafety,
        escalation: Escalation,
    ) -> Result<Self, IllegalEscalation> {
        if escalation.allowed_for(safety) {
            Ok(Self { safety, escalation })
        } else {
            Err(IllegalEscalation { safety, escalation })
        }
    }

    pub const fn safety(self) -> SubjectSafety {
        self.safety
    }

    pub const fn escalation(self) -> Escalation {
        self.escalation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("escalation {escalation} is not allowed for a {safety} subject")]
pub struct IllegalEscalation {
    pub safety: SubjectSafety,
    pub escalation: Escalation,
}

/// Capped exponential delay between counted attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Backoff {
    base: Duration,
    max: Duration,
}

impl Backoff {
    /// `base` after the first counted attempt, doubling per attempt up to
    /// `max`. A fixed cadence is `base == max`.
    pub fn exponential(base: Duration, max: Duration) -> Result<Self, PolicyError> {
        check_interval("backoff base", base)?;
        check_interval("backoff max", max)?;
        if max < base {
            return Err(PolicyError::BackoffMaxBelowBase { base, max });
        }
        Ok(Self { base, max })
    }

    /// The delay after `attempts` counted attempts. Zero attempts, an outage
    /// with no episode open, waits `base`.
    pub fn delay_after(self, attempts: u32) -> Duration {
        let mut delay = self.base;
        for _ in 1..attempts {
            if delay >= self.max {
                break;
            }
            delay = delay.saturating_mul(2);
        }
        delay.min(self.max)
    }
}

/// When a loop escalates an episode and how it paces attempts around that.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Policy {
    escalate_after: Duration,
    min_attempts: u32,
    park_interval: Duration,
    backoff: Backoff,
    escalation: SubjectEscalation,
}

impl Policy {
    /// Both bounds must hold before the episode escalates, so a burst of fast
    /// failures cannot escalate an episode younger than `escalate_after`, and
    /// a long outage cannot escalate one with fewer than `min_attempts`
    /// attempts. An `escalate_after` of zero leaves only the attempt bound. A
    /// `min_attempts` of zero behaves as one, since every counted attempt
    /// makes `attempts` at least one.
    pub fn new(
        escalate_after: Duration,
        min_attempts: u32,
        park_interval: Duration,
        backoff: Backoff,
        escalation: SubjectEscalation,
    ) -> Result<Self, PolicyError> {
        if escalate_after > MAX_POLICY_INTERVAL {
            return Err(PolicyError::IntervalTooLong {
                field: "escalate_after",
                value: escalate_after,
            });
        }
        check_interval("park_interval", park_interval)?;
        Ok(Self {
            escalate_after,
            min_attempts,
            park_interval,
            backoff,
            escalation,
        })
    }

    pub const fn escalate_after(&self) -> Duration {
        self.escalate_after
    }

    pub const fn min_attempts(&self) -> u32 {
        self.min_attempts
    }

    pub const fn park_interval(&self) -> Duration {
        self.park_interval
    }

    pub const fn backoff(&self) -> Backoff {
        self.backoff
    }

    pub const fn escalation(&self) -> SubjectEscalation {
        self.escalation
    }
}

/// A zero interval would make a loop retry or park in a hot loop.
fn check_interval(field: &'static str, value: Duration) -> Result<(), PolicyError> {
    if value.is_zero() {
        return Err(PolicyError::ZeroInterval { field });
    }
    if value > MAX_POLICY_INTERVAL {
        return Err(PolicyError::IntervalTooLong { field, value });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum PolicyError {
    #[error("{field} must be greater than zero")]
    ZeroInterval { field: &'static str },
    #[error("{field} of {value:?} exceeds the {MAX_POLICY_INTERVAL:?} limit")]
    IntervalTooLong {
        field: &'static str,
        value: Duration,
    },
    #[error("backoff max {max:?} is below its base {base:?}")]
    BackoffMaxBelowBase { base: Duration, max: Duration },
}

/// The escalation an episode received and when.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Escalated {
    at: SystemTime,
    escalation: Escalation,
}

impl Escalated {
    pub const fn new(at: SystemTime, escalation: Escalation) -> Self {
        Self { at, escalation }
    }

    pub const fn at(self) -> SystemTime {
        self.at
    }

    pub const fn escalation(self) -> Escalation {
        self.escalation
    }
}

/// One loop's history of failing to converge one subject.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Episode {
    first_seen_at: SystemTime,
    reason: NonConvergenceReason,
    reason_since: SystemTime,
    attempts: u32,
    next_attempt_at: SystemTime,
    escalated: Option<Escalated>,
    last_error: Option<String>,
}

impl Episode {
    /// Rebuilds an episode from the fields a caller stored, to pass back to
    /// [`observe`] as `prev`. [`observe`] accepts any values. Zero `attempts`
    /// reads as an episode with no counted attempt yet, which lets a caller
    /// seed `first_seen_at` from an existing record. A `first_seen_at` later
    /// than `now` reads as zero age. A stored escalation only parks the
    /// episode, so `escalated` is not checked against a safety class again.
    pub fn from_parts(
        first_seen_at: SystemTime,
        reason: NonConvergenceReason,
        reason_since: SystemTime,
        attempts: u32,
        next_attempt_at: SystemTime,
        escalated: Option<Escalated>,
        last_error: Option<String>,
    ) -> Self {
        Self {
            first_seen_at,
            reason,
            reason_since,
            attempts,
            next_attempt_at,
            escalated,
            last_error,
        }
    }

    /// When the episode's age starts: the first counted attempt that did not
    /// converge, or the time a caller seeded through [`Episode::from_parts`].
    pub const fn first_seen_at(&self) -> SystemTime {
        self.first_seen_at
    }

    /// Why the latest counted attempt did not converge.
    pub const fn reason(&self) -> NonConvergenceReason {
        self.reason
    }

    /// When `reason` last changed.
    pub const fn reason_since(&self) -> SystemTime {
        self.reason_since
    }

    /// Counted attempts since the episode opened, across reason changes and
    /// after escalation.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    pub const fn next_attempt_at(&self) -> SystemTime {
        self.next_attempt_at
    }

    pub fn escalated_at(&self) -> Option<SystemTime> {
        self.escalated.map(Escalated::at)
    }

    pub fn escalation(&self) -> Option<Escalation> {
        self.escalated.map(Escalated::escalation)
    }

    pub const fn escalated(&self) -> Option<Escalated> {
        self.escalated
    }

    /// The error text of the latest counted attempt, if it failed rather than
    /// deferred.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

/// What the loop does next with the subject.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Verdict {
    /// No episode is open: the subject converged or is gone.
    Cleared,
    /// Try again at `at`.
    Retry { at: SystemTime },
    /// This attempt crossed the policy's bound: apply `escalation` once, then
    /// observe again at `observe_at`. The returned episode already records the
    /// escalation, and later calls never return `Escalate` for it again. Store
    /// that episode together with the escalation's effect, in one transaction
    /// where the effect is a database write. If the effect fails, keep the
    /// previous episode stored instead, writing it back if the new one was
    /// already stored, so a later counted attempt escalates again.
    Escalate {
        escalation: Escalation,
        observe_at: SystemTime,
    },
    /// The episode has already escalated: observe the subject again at
    /// `observe_at` and pass the result to [`observe`]. The escalation is not
    /// repeated, and a `NotConverged` result still counts in `attempts`.
    Parked { observe_at: SystemTime },
}

/// Fold one attempt's outcome at `now` into the (loop, subject) episode `prev`
/// and decide the loop's next step. Returns the episode to store, `None` when
/// none is open.
pub fn observe(
    prev: Option<Episode>,
    outcome: Outcome,
    now: SystemTime,
    policy: &Policy,
) -> (Option<Episode>, Verdict) {
    let (reason, error) = match outcome {
        Outcome::Converged | Outcome::SubjectGone => return (None, Verdict::Cleared),
        // An outage opens nothing: the loop retries at its first backoff step.
        Outcome::Outage => match prev {
            None => {
                let at = now + policy.backoff.delay_after(0);
                return (None, Verdict::Retry { at });
            }
            Some(episode) => return schedule(episode, false, now, policy),
        },
        Outcome::NotConverged { reason, error } => (reason, error),
    };
    let mut episode = prev.unwrap_or(Episode {
        first_seen_at: now,
        reason,
        reason_since: now,
        attempts: 0,
        next_attempt_at: now,
        escalated: None,
        last_error: None,
    });
    if episode.reason != reason {
        episode.reason = reason;
        episode.reason_since = now;
    }
    episode.attempts = episode.attempts.saturating_add(1);
    episode.last_error = error;
    // A clock that stepped back before `first_seen_at` reads as zero age, which
    // delays an escalation rather than hastening one.
    let age = now
        .duration_since(episode.first_seen_at)
        .unwrap_or(Duration::ZERO);
    let escalates_now = episode.escalated.is_none()
        && age >= policy.escalate_after
        && episode.attempts >= policy.min_attempts;
    if escalates_now {
        episode.escalated = Some(Escalated {
            at: now,
            escalation: policy.escalation.escalation,
        });
    }
    schedule(episode, escalates_now, now, policy)
}

/// Sets the episode's next attempt: the backoff for its attempts, or the park
/// interval once it has escalated.
fn schedule(
    mut episode: Episode,
    escalates_now: bool,
    now: SystemTime,
    policy: &Policy,
) -> (Option<Episode>, Verdict) {
    let (next_attempt_at, verdict) = match episode.escalated {
        None => {
            let at = now + policy.backoff.delay_after(episode.attempts);
            (at, Verdict::Retry { at })
        }
        Some(escalated) => {
            let observe_at = now + policy.park_interval;
            let verdict = if escalates_now {
                Verdict::Escalate {
                    escalation: escalated.escalation,
                    observe_at,
                }
            } else {
                Verdict::Parked { observe_at }
            };
            (observe_at, verdict)
        }
    };
    episode.next_attempt_at = next_attempt_at;
    (Some(episode), verdict)
}

#[cfg(test)]
mod tests;
