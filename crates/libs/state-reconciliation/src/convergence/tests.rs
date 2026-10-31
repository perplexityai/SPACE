use super::*;

const ESCALATE_AFTER: Duration = Duration::from_secs(600);
const MIN_ATTEMPTS: u32 = 5;
const PARK: Duration = Duration::from_secs(900);
const BACKOFF_BASE: Duration = Duration::from_secs(10);
const BACKOFF_MAX: Duration = Duration::from_secs(120);

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000 + secs)
}

fn policy_for(safety: SubjectSafety, escalation: Escalation) -> Policy {
    Policy::new(
        ESCALATE_AFTER,
        MIN_ATTEMPTS,
        PARK,
        Backoff::exponential(BACKOFF_BASE, BACKOFF_MAX).unwrap(),
        SubjectEscalation::new(safety, escalation).unwrap(),
    )
    .unwrap()
}

fn policy() -> Policy {
    policy_for(SubjectSafety::Disposable, Escalation::FailTerminal)
}

fn stuck(reason: NonConvergenceReason) -> Outcome {
    Outcome::NotConverged {
        reason,
        error: Some(format!("attempt failed: {reason}")),
    }
}

/// Feeds `outcomes` at `times` through `observe`, starting from `prev`.
fn run(
    mut prev: Option<Episode>,
    steps: impl IntoIterator<Item = (SystemTime, Outcome)>,
    policy: &Policy,
) -> (Option<Episode>, Vec<Verdict>) {
    let mut verdicts = Vec::new();
    for (now, outcome) in steps {
        let (next, verdict) = observe(prev, outcome, now, policy);
        prev = next;
        verdicts.push(verdict);
    }
    (prev, verdicts)
}

/// Index of the first attempt at which both bounds hold, for a subject that
/// fails every `interval` seconds starting at zero.
fn expected_escalation_index(interval: u64) -> usize {
    (0..)
        .find(|&index: &u64| {
            index * interval >= ESCALATE_AFTER.as_secs() && index + 1 >= u64::from(MIN_ATTEMPTS)
        })
        .and_then(|index| usize::try_from(index).ok())
        .unwrap()
}

#[test]
fn never_converging_subject_escalates_once_when_both_bounds_hold() {
    // 10 s reaches the attempt bound first and waits for age; 300 s reaches age
    // first and waits for attempts.
    for interval in [10, 60, 300, 1_000] {
        let policy = policy();
        let steps = (0..100).map(|index| {
            (
                at(index * interval),
                stuck(NonConvergenceReason::LeasedProcessAlive),
            )
        });
        let (episode, verdicts) = run(None, steps, &policy);
        let escalation_index = expected_escalation_index(interval);
        for (index, verdict) in verdicts.iter().enumerate() {
            let now = at(index as u64 * interval);
            let expected = match index.cmp(&escalation_index) {
                std::cmp::Ordering::Less => Verdict::Retry {
                    at: now + policy.backoff().delay_after(index as u32 + 1),
                },
                std::cmp::Ordering::Equal => Verdict::Escalate {
                    escalation: Escalation::FailTerminal,
                    observe_at: now + PARK,
                },
                std::cmp::Ordering::Greater => Verdict::Parked {
                    observe_at: now + PARK,
                },
            };
            assert_eq!(*verdict, expected, "interval {interval}s, attempt {index}");
        }
        let episode = episode.expect("a stuck subject keeps its episode");
        assert_eq!(episode.attempts(), 100);
        assert_eq!(episode.first_seen_at(), at(0));
        assert_eq!(
            episode.escalated_at(),
            Some(at(escalation_index as u64 * interval))
        );
        assert_eq!(episode.escalation(), Some(Escalation::FailTerminal));
    }
}

#[test]
fn outages_never_open_extend_or_escalate_an_episode() {
    let policy = policy();
    let outages = |from: u64| (0..1_000).map(move |index| (at(from + index * 60), Outcome::Outage));

    let (episode, verdicts) = run(None, outages(0), &policy);
    assert_eq!(episode, None);
    assert!(verdicts.iter().enumerate().all(|(index, verdict)| *verdict
        == Verdict::Retry {
            at: at(index as u64 * 60) + BACKOFF_BASE
        }));

    // An episode that already has enough attempts but is young: an outage far
    // past `escalate_after` leaves it exactly as it was.
    let young = (0..MIN_ATTEMPTS).map(|index| {
        (
            at(u64::from(index)),
            stuck(NonConvergenceReason::CrossNodeRestoreFailed),
        )
    });
    let (open, _) = run(None, young, &policy);
    let open = open.unwrap();
    let (after_outage, verdicts) = run(Some(open.clone()), outages(10), &policy);
    let after_outage = after_outage.unwrap();
    assert!(
        verdicts
            .iter()
            .all(|verdict| matches!(verdict, Verdict::Retry { .. }))
    );
    assert_eq!(after_outage.attempts(), open.attempts());
    assert_eq!(after_outage.first_seen_at(), open.first_seen_at());
    assert_eq!(after_outage.reason(), open.reason());
    assert_eq!(after_outage.reason_since(), open.reason_since());
    assert_eq!(after_outage.last_error(), open.last_error());
    assert_eq!(after_outage.escalated(), None);

    // The outage did not hide the stuck subject: the next counted attempt
    // escalates it.
    let (_, verdict) = observe(
        Some(after_outage),
        stuck(NonConvergenceReason::CrossNodeRestoreFailed),
        at(100_000),
        &policy,
    );
    assert!(matches!(verdict, Verdict::Escalate { .. }));

    // Below the backoff cap, an outage waits the step for the attempts made
    // so far: two attempts wait `delay_after(2)`, not the next or first step.
    let two = (0..2).map(|index| (at(index), stuck(NonConvergenceReason::NoRestoreSource)));
    let (two, _) = run(None, two, &policy);
    let now = at(50);
    let (_, verdict) = observe(two, Outcome::Outage, now, &policy);
    assert_eq!(
        verdict,
        Verdict::Retry {
            at: now + policy.backoff().delay_after(2)
        }
    );

    // An outage on an escalated episode keeps it parked and moves only its
    // next observation.
    let escalating = (0..10).map(|index| {
        (
            at(index * 300),
            stuck(NonConvergenceReason::LeasedProcessAlive),
        )
    });
    let (escalated, _) = run(None, escalating, &policy);
    let escalated = escalated.unwrap();
    assert!(escalated.escalated().is_some());
    let now = at(50_000);
    let (after_outage, verdict) = observe(Some(escalated.clone()), Outcome::Outage, now, &policy);
    assert_eq!(
        verdict,
        Verdict::Parked {
            observe_at: now + PARK
        }
    );
    assert_eq!(
        after_outage,
        Some(Episode::from_parts(
            escalated.first_seen_at(),
            escalated.reason(),
            escalated.reason_since(),
            escalated.attempts(),
            now + PARK,
            escalated.escalated(),
            escalated.last_error().map(str::to_owned),
        ))
    );
}

#[test]
fn a_clock_stepped_back_delays_escalation() {
    let policy = policy();
    // One attempt short of the bound, starting well past the earlier reading.
    let steps = (0..MIN_ATTEMPTS - 1).map(|index| {
        (
            at(10_000 + u64::from(index)),
            stuck(NonConvergenceReason::NoRestoreSource),
        )
    });
    let (episode, _) = run(None, steps, &policy);
    let now = at(0);
    let (episode, verdict) = observe(
        episode,
        stuck(NonConvergenceReason::NoRestoreSource),
        now,
        &policy,
    );
    assert_eq!(episode.unwrap().attempts(), MIN_ATTEMPTS);
    assert_eq!(
        verdict,
        Verdict::Retry {
            at: now + policy.backoff().delay_after(MIN_ATTEMPTS)
        }
    );
}

#[test]
fn outcome_effect_classifies_each_outcome() {
    let table = [
        (Outcome::Converged, RetryEffect::Ends),
        (Outcome::SubjectGone, RetryEffect::Ends),
        (
            Outcome::NotConverged {
                reason: NonConvergenceReason::NoRestoreSource,
                error: None,
            },
            RetryEffect::Extends,
        ),
        (Outcome::Outage, RetryEffect::Leaves),
    ];
    for (outcome, effect) in table {
        assert_eq!(outcome.effect(), effect, "{outcome:?}");
    }
}

#[test]
fn alternating_reasons_keep_attempts_and_age() {
    use NonConvergenceReason::{NoRestoreSource, PriorGenerationPodLingering};
    let policy = policy();
    let interval = 60;
    // Reasons change on every second attempt, so `reason_since` moves only on
    // a change.
    let reason_at = |index: u64| {
        if (index / 2).is_multiple_of(2) {
            PriorGenerationPodLingering
        } else {
            NoRestoreSource
        }
    };
    let mut prev = None;
    let mut escalated_at = None;
    for index in 0..40_u64 {
        let now = at(index * interval);
        let (next, verdict) = observe(prev, stuck(reason_at(index)), now, &policy);
        let episode = next.clone().unwrap();
        assert_eq!(episode.attempts(), index as u32 + 1);
        assert_eq!(episode.first_seen_at(), at(0));
        assert_eq!(episode.reason(), reason_at(index));
        assert_eq!(episode.reason_since(), at((index - index % 2) * interval));
        if matches!(verdict, Verdict::Escalate { .. }) {
            assert_eq!(escalated_at, None, "escalates once");
            escalated_at = Some(index);
        }
        prev = next;
    }
    assert_eq!(
        escalated_at,
        Some(expected_escalation_index(interval) as u64),
        "the bound is the same as for a constant reason"
    );
}

#[test]
fn success_or_disappearance_clears_any_episode() {
    let policy = policy();
    let stuck_for = |attempts: u64| {
        let steps = (0..attempts).map(|index| {
            (
                at(index * 300),
                stuck(NonConvergenceReason::TeardownSuspendUnfinished),
            )
        });
        run(None, steps, &policy).0
    };
    let below_bound = stuck_for(2).unwrap();
    let escalated = stuck_for(10).unwrap();
    assert_eq!(below_bound.escalated(), None);
    assert!(escalated.escalated().is_some());

    for ending in [Outcome::Converged, Outcome::SubjectGone] {
        for prev in [None, Some(below_bound.clone()), Some(escalated.clone())] {
            let (episode, verdict) = observe(prev, ending.clone(), at(10_000), &policy);
            assert_eq!((episode, verdict), (None, Verdict::Cleared), "{ending:?}");
        }
    }

    // The next failure after a clear starts a new episode.
    let (cleared, _) = observe(Some(escalated), Outcome::Converged, at(10_000), &policy);
    let (reopened, verdict) = observe(
        cleared,
        stuck(NonConvergenceReason::TeardownSuspendUnfinished),
        at(10_060),
        &policy,
    );
    let reopened = reopened.unwrap();
    assert_eq!(reopened.attempts(), 1);
    assert_eq!(reopened.first_seen_at(), at(10_060));
    assert_eq!(reopened.escalated(), None);
    assert_eq!(
        verdict,
        Verdict::Retry {
            at: at(10_060) + BACKOFF_BASE
        }
    );
}

#[test]
fn an_episode_seeded_from_an_existing_record_escalates_on_its_first_attempt() {
    let policy = Policy::new(
        ESCALATE_AFTER,
        1,
        PARK,
        Backoff::exponential(BACKOFF_BASE, BACKOFF_MAX).unwrap(),
        SubjectEscalation::new(SubjectSafety::Disposable, Escalation::FailTerminal).unwrap(),
    )
    .unwrap();
    // A caller seeds `first_seen_at` from a record created before the loop
    // made any counted attempt.
    let seeded = Episode::from_parts(
        at(0),
        NonConvergenceReason::TeardownSuspendUnfinished,
        at(0),
        0,
        at(0),
        None,
        None,
    );
    let now = at(0) + ESCALATE_AFTER;
    let (episode, verdict) = observe(
        Some(seeded),
        stuck(NonConvergenceReason::TeardownSuspendUnfinished),
        now,
        &policy,
    );
    assert_eq!(
        verdict,
        Verdict::Escalate {
            escalation: Escalation::FailTerminal,
            observe_at: now + PARK,
        }
    );
    let episode = episode.unwrap();
    assert_eq!(episode.attempts(), 1);
    assert_eq!(episode.first_seen_at(), at(0));
}

#[test]
fn protected_subjects_escalate_only_to_an_operator() {
    use Escalation::{FailTerminal, OperatorOnly, QuarantineSubject};
    use SubjectSafety::{Disposable, MayHoldOnlyCopy, SecurityRelevant};
    let table = [
        (Disposable, QuarantineSubject, true),
        (Disposable, FailTerminal, true),
        (Disposable, OperatorOnly, true),
        (MayHoldOnlyCopy, QuarantineSubject, false),
        (MayHoldOnlyCopy, FailTerminal, false),
        (MayHoldOnlyCopy, OperatorOnly, true),
        (SecurityRelevant, QuarantineSubject, false),
        (SecurityRelevant, FailTerminal, false),
        (SecurityRelevant, OperatorOnly, true),
    ];
    for (safety, escalation, allowed) in table {
        assert_eq!(
            escalation.allowed_for(safety),
            allowed,
            "{safety} {escalation}"
        );
        let built = SubjectEscalation::new(safety, escalation);
        if allowed {
            assert_eq!(built.unwrap().escalation(), escalation);
        } else {
            assert_eq!(built, Err(IllegalEscalation { safety, escalation }));
        }
    }

    // A policy for a subject that may hold the only copy can only ever hand
    // its loop `OperatorOnly`.
    let policy = policy_for(MayHoldOnlyCopy, OperatorOnly);
    let steps = (0..50).map(|index| (at(index * 60), stuck(NonConvergenceReason::NoRestoreSource)));
    let (_, verdicts) = run(None, steps, &policy);
    let escalations: Vec<_> = verdicts
        .iter()
        .filter_map(|verdict| match verdict {
            Verdict::Escalate { escalation, .. } => Some(*escalation),
            _ => None,
        })
        .collect();
    assert_eq!(escalations, [OperatorOnly]);
}

/// Deterministic xorshift, so a failing sequence reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// The rules' view of an open episode: what the model expects `observe` to
/// have recorded since the last clear.
#[derive(Debug)]
struct ModelEpisode {
    first_seen: u64,
    attempts: u32,
    escalated: bool,
    last_error: Option<String>,
}

/// Random mixes of every outcome, checked against a model of the rules in the
/// module documentation: attempts and age count from the first failure since
/// the last clear, only a counted attempt can escalate, it escalates once, the
/// error is the latest counted attempt's, and an open episode always has a next
/// observation. The model classifies outcomes itself rather than through
/// [`Outcome::effect`].
#[test]
fn random_outcome_sequences_follow_the_episode_rules() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0..2_000 {
        let policy = policy();
        let mut prev: Option<Episode> = None;
        let mut now = 0;
        let mut model: Option<ModelEpisode> = None;
        for step in 0..60 {
            // A caller rebuilds the episode it stored; the rebuild loses nothing.
            if let Some(episode) = prev.take() {
                let rebuilt = Episode::from_parts(
                    episode.first_seen_at(),
                    episode.reason(),
                    episode.reason_since(),
                    episode.attempts(),
                    episode.next_attempt_at(),
                    episode
                        .escalated()
                        .map(|escalated| Escalated::new(escalated.at(), escalated.escalation())),
                    episode.last_error().map(str::to_owned),
                );
                assert_eq!(rebuilt, episode);
                prev = Some(rebuilt);
            }
            now += rng.below(240);
            let outcome = match rng.below(10) {
                0 => Outcome::Converged,
                1 => Outcome::SubjectGone,
                2..=4 => Outcome::Outage,
                _ => {
                    let reasons = NonConvergenceReason::ALL;
                    Outcome::NotConverged {
                        reason: reasons[rng.below(reasons.len() as u64) as usize],
                        error: (rng.below(2) == 0).then(|| format!("attempt {step} failed")),
                    }
                }
            };
            let (next, verdict) = observe(prev.clone(), outcome.clone(), at(now), &policy);
            let escalates_now = match &outcome {
                Outcome::Converged | Outcome::SubjectGone => {
                    model = None;
                    false
                }
                Outcome::Outage => false,
                Outcome::NotConverged { error, .. } => {
                    let open = model.get_or_insert(ModelEpisode {
                        first_seen: now,
                        attempts: 0,
                        escalated: false,
                        last_error: None,
                    });
                    open.attempts += 1;
                    open.last_error = error.clone();
                    let crosses = !open.escalated
                        && now - open.first_seen >= ESCALATE_AFTER.as_secs()
                        && open.attempts >= MIN_ATTEMPTS;
                    open.escalated |= crosses;
                    crosses
                }
            };
            assert_eq!(
                matches!(verdict, Verdict::Escalate { .. }),
                escalates_now,
                "{outcome:?} at {now}"
            );
            match (&next, &model) {
                (None, None) => {
                    assert!(matches!(verdict, Verdict::Cleared | Verdict::Retry { .. }))
                }
                (Some(episode), Some(open)) => {
                    assert_eq!(episode.first_seen_at(), at(open.first_seen));
                    assert_eq!(episode.attempts(), open.attempts);
                    assert_eq!(episode.escalated().is_some(), open.escalated);
                    assert_eq!(episode.last_error(), open.last_error.as_deref());
                    if let Outcome::NotConverged { reason, .. } = &outcome {
                        assert_eq!(episode.reason(), *reason);
                    }
                    if outcome == Outcome::Outage {
                        let prev = prev.as_ref().unwrap();
                        assert_eq!(episode.reason(), prev.reason());
                        assert_eq!(episode.reason_since(), prev.reason_since());
                    }
                    let deadline = match verdict {
                        Verdict::Retry { at } => at,
                        Verdict::Escalate { observe_at, .. } | Verdict::Parked { observe_at } => {
                            observe_at
                        }
                        Verdict::Cleared => panic!("an open episode is never cleared"),
                    };
                    assert!(deadline > at(now));
                    assert_eq!(episode.next_attempt_at(), deadline);
                    assert_eq!(
                        matches!(verdict, Verdict::Escalate { .. } | Verdict::Parked { .. }),
                        open.escalated
                    );
                }
                (next, model) => panic!("episode {next:?} disagrees with model {model:?}"),
            }
            prev = next;
        }
    }
}

#[test]
fn backoff_doubles_up_to_its_cap() {
    let backoff = Backoff::exponential(BACKOFF_BASE, BACKOFF_MAX).unwrap();
    let table = [
        (0, 10),
        (1, 10),
        (2, 20),
        (3, 40),
        (4, 80),
        (5, 120),
        (40, 120),
        (u32::MAX, 120),
    ];
    for (attempts, secs) in table {
        assert_eq!(
            backoff.delay_after(attempts),
            Duration::from_secs(secs),
            "{attempts}"
        );
    }

    // A tiny base keeps doubling past 32 attempts until it reaches the cap.
    let backoff = Backoff::exponential(Duration::from_nanos(1), BACKOFF_MAX).unwrap();
    for (attempts, nanos) in [(32, 1 << 31), (33, 1 << 32), (u32::MAX, 120_000_000_000)] {
        assert_eq!(
            backoff.delay_after(attempts),
            Duration::from_nanos(nanos),
            "{attempts}"
        );
    }
}

#[test]
fn policy_refuses_intervals_that_would_spin_or_overflow() {
    let second = Duration::from_secs(1);
    let too_long = MAX_POLICY_INTERVAL + second;
    let escalation =
        SubjectEscalation::new(SubjectSafety::Disposable, Escalation::OperatorOnly).unwrap();
    let backoff = Backoff::exponential(second, second).unwrap();
    assert_eq!(
        Backoff::exponential(Duration::ZERO, second),
        Err(PolicyError::ZeroInterval {
            field: "backoff base"
        })
    );
    assert_eq!(
        Backoff::exponential(second * 2, second),
        Err(PolicyError::BackoffMaxBelowBase {
            base: second * 2,
            max: second
        })
    );
    assert!(matches!(
        Backoff::exponential(second, too_long),
        Err(PolicyError::IntervalTooLong { .. })
    ));
    assert_eq!(
        Policy::new(second, 1, Duration::ZERO, backoff, escalation),
        Err(PolicyError::ZeroInterval {
            field: "park_interval"
        })
    );
    assert!(matches!(
        Policy::new(too_long, 1, second, backoff, escalation),
        Err(PolicyError::IntervalTooLong { .. })
    ));
}

#[test]
fn wire_names_round_trip_and_are_distinct() {
    fn check<T>(variants: &[T])
    where
        T: Copy + fmt::Debug + fmt::Display + Eq + FromStr,
        T::Err: fmt::Debug,
    {
        let mut names = std::collections::BTreeSet::new();
        for &variant in variants {
            let name = variant.to_string();
            assert!(
                name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{name}"
            );
            assert!(names.insert(name.clone()), "duplicate {name}");
            assert_eq!(name.parse::<T>().unwrap(), variant);
        }
        "".parse::<T>().unwrap_err();
    }
    check(ConvergenceLoop::ALL);
    check(NonConvergenceReason::ALL);
    check(SubjectSafety::ALL);
    check(Escalation::ALL);
}
