use super::*;

/// The generation every header in these tests is at; callers expect
/// another, so each would also be refused as stale.
const GENERATION: u64 = 7;
const STALE: Option<u64> = Some(GENERATION - 1);
const FAILURE_BUDGET: u64 = 2;

#[derive(Debug, Eq, PartialEq)]
struct Refused(Refusal);

impl From<Refusal> for Refused {
    fn from(refusal: Refusal) -> Self {
        Self(refusal)
    }
}

/// A header and its items, recording the steps a verb ran on them.
struct Fake {
    phase: HeaderPhase,
    any_done: bool,
    failed: u64,
    steps: Vec<&'static str>,
}

impl OperatorStore for Fake {
    type Error = Refused;

    async fn lock(&mut self) -> Result<Option<Locked>, Refused> {
        self.steps.push("lock");
        Ok(Some(Locked {
            phase: self.phase,
            generation: GENERATION,
            failure_budget: FAILURE_BUDGET,
        }))
    }

    async fn progress(&mut self) -> Result<(bool, u64), Refused> {
        self.steps.push("progress");
        Ok((self.any_done, self.failed))
    }

    async fn retry_failed(&mut self) -> Result<u64, Refused> {
        self.steps.push("retry_failed");
        Ok(self.failed)
    }

    async fn in_flight(&mut self) -> Result<u64, Refused> {
        self.steps.push("in_flight");
        Ok(0)
    }

    async fn write_phase(&mut self, _phase: HeaderPhase) -> Result<u64, Refused> {
        self.steps.push("write_phase");
        Ok(GENERATION + 1)
    }
}

/// Each header meets the refusal it expects and every one after it in the
/// order the module docs give, stale included, so a resume that checks them
/// in another order reports another refusal. None reaches a write.
#[tokio::test(flavor = "current_thread")]
async fn a_resume_reports_the_first_refusal_in_order_and_writes_nothing() {
    let at_budget = FAILURE_BUDGET;
    let cases = [
        (
            "finished, before reading the items",
            HeaderPhase::Completed,
            false,
            at_budget,
            false,
            Refusal::Finished(HeaderPhase::Completed),
            &["lock"][..],
        ),
        (
            "not resumable, before the budget",
            HeaderPhase::Rolling,
            true,
            at_budget,
            false,
            Refusal::NotResumable(HeaderPhase::Rolling),
            &["lock", "progress"][..],
        ),
        (
            "at the budget, before the generation",
            HeaderPhase::Paused,
            false,
            at_budget,
            false,
            Refusal::AtFailureBudget {
                failed: at_budget,
                failure_budget: FAILURE_BUDGET,
            },
            &["lock", "progress"][..],
        ),
        (
            "stale, before retrying the failed items",
            HeaderPhase::Paused,
            false,
            at_budget,
            true,
            Refusal::Stale {
                generation: GENERATION,
                expected: GENERATION - 1,
            },
            &["lock", "progress"][..],
        ),
    ];
    for (case, phase, any_done, failed, retry_failed, refusal, steps) in cases {
        let mut store = Fake {
            phase,
            any_done,
            failed,
            steps: Vec::new(),
        };
        assert_eq!(
            resume(&mut store, STALE, retry_failed).await,
            Err(Refused(refusal)),
            "{case}"
        );
        assert_eq!(store.steps, steps, "{case}");
    }
}
