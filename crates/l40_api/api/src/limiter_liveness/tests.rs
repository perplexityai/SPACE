use super::*;

/// The cadence a liveness monitor's window rests on: a process that is serving
/// nothing at all still reports throughout.
#[tokio::test(start_paused = true)]
async fn a_heartbeat_reports_once_per_interval_with_no_other_work() {
    let interval = Duration::from_secs(60);
    let mut reported = 0usize;

    // The loop only ends on cancellation, so a bounded observation window is
    // what returns here.
    let _ = tokio::time::timeout(
        interval * 10,
        run_heartbeat(interval, CancellationToken::new(), || reported += 1),
    )
    .await;

    // Ten ticks are guaranteed across ten intervals (t=0 through t=540), which
    // is what makes the count readable as a cadence; a twelfth would mean a
    // sub-interval gap. The upper slack covers the eleventh, due at t=600
    // alongside the timeout itself — how `timeout` orders its own deadline
    // against the future it wraps is not something this test should pin. The
    // startup tick is pinned by the next test, not this one.
    assert!(
        (10..=11).contains(&reported),
        "reported {reported} heartbeats across ten intervals"
    );
}

/// A process that dies young still contributes a point, so its service's group
/// is not empty for the window it lived in. (Which replica it was is not
/// recoverable from this counter — see the granularity note on `run_heartbeat`.)
#[tokio::test(start_paused = true)]
async fn the_first_heartbeat_does_not_wait_out_the_interval() {
    let mut reported = 0usize;

    let _ = tokio::time::timeout(
        Duration::from_millis(1),
        run_heartbeat(Duration::from_secs(60), CancellationToken::new(), || {
            reported += 1
        }),
    )
    .await;

    assert_eq!(
        reported, 1,
        "only the startup tick can land inside a 1ms window"
    );
}

/// Shutdown has to end the heartbeat. space-api awaits it through its
/// `TaskTracker` drain, and the cluster controller through its reconciler join
/// set on a 10-second budget — and there, blowing that budget does more than
/// delay exit: the controller reports an unclean drain and *abandons* its
/// leadership lease rather than releasing it, so a standby waits the lease out
/// instead of taking over promptly.
#[tokio::test(start_paused = true)]
async fn cancelling_the_shutdown_token_ends_the_heartbeat_mid_flight() {
    let interval = Duration::from_secs(60);
    let shutdown_token = CancellationToken::new();
    let mut reported = 0usize;

    let cancel_after_two_intervals = {
        let shutdown_token = shutdown_token.clone();
        async move {
            tokio::time::sleep(interval * 2 + interval / 2).await;
            shutdown_token.cancel();
        }
    };

    // The timeout only guards against a hang; a heartbeat that honors
    // cancellation completes the join well inside it.
    tokio::time::timeout(interval * 10, async {
        tokio::join!(
            cancel_after_two_intervals,
            run_heartbeat(interval, shutdown_token, || reported += 1),
        );
    })
    .await
    .expect("a cancelled heartbeat must return so shutdown can drain");

    // Startup plus two interval ticks — enough that the run being cut short is
    // what ended it, not an immediate return that would pass this test while
    // reporting nothing.
    assert_eq!(
        reported, 3,
        "reported {reported} heartbeats before shutdown"
    );
}
