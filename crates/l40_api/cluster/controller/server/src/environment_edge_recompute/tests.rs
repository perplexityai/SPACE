use std::sync::{Arc, Mutex};

use pplx_space_test_env::TestResult;

use super::*;

#[derive(Clone, Default)]
struct FakeSink {
    outcomes: Arc<Mutex<Vec<Result<RecomputeOutcome, ()>>>>,
    calls: Arc<Mutex<u32>>,
}

impl EdgeRecomputeSink for FakeSink {
    async fn recompute_all_edges(&self) -> Result<RecomputeOutcome, sqlx::Error> {
        *self.calls.lock().unwrap() += 1;
        match self.outcomes.lock().unwrap().pop() {
            Some(Ok(outcome)) => Ok(outcome),
            Some(Err(())) => Err(sqlx::Error::PoolTimedOut),
            None => Ok(RecomputeOutcome::Recomputed { changed: 0 }),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn the_loop_keeps_recomputing_through_deferrals_and_errors() -> TestResult {
    let sink = FakeSink::default();
    // Popped from the back: an error, then a deferral, then a recompute.
    *sink.outcomes.lock().unwrap() = vec![
        Ok(RecomputeOutcome::Recomputed { changed: 1 }),
        Ok(RecomputeOutcome::Deferred),
        Err(()),
    ];
    let token = CancellationToken::new();
    let handle = tokio::spawn(run(sink.clone(), token.clone()));
    // The first tick fires at once, then one per TICK.
    for _ in 0..3 {
        tokio::task::yield_now().await;
        tokio::time::advance(TICK).await;
    }
    tokio::task::yield_now().await;
    token.cancel();
    handle.await?;
    assert!(*sink.calls.lock().unwrap() >= 3);
    assert!(sink.outcomes.lock().unwrap().is_empty());
    Ok(())
}
