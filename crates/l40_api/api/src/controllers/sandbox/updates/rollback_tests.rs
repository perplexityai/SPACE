use std::cell::RefCell;
use std::time::Duration;

use tokio::time::Instant;

use super::{SubstrateError, rollback_node_delete};

#[derive(Clone, Copy, Debug)]
enum Worker {
    Recovers,
    Unavailable,
    Refuses,
    SlowSuccess,
}

/// Fast admission failures must leave time for recovery without spinning.
/// The retry window must not cancel an already-started destructive teardown.
#[tokio::test(start_paused = true)]
async fn rollback_delete_retries_over_time_without_cancelling_teardown() {
    for worker in [
        Worker::Recovers,
        Worker::Unavailable,
        Worker::Refuses,
        Worker::SlowSuccess,
    ] {
        let started = Instant::now();
        let attempts = RefCell::new(Vec::new());
        let result = rollback_node_delete(|| {
            attempts.borrow_mut().push(Instant::now());
            async move {
                match worker {
                    Worker::Recovers if started.elapsed() >= Duration::from_secs(20) => Ok(()),
                    Worker::Recovers | Worker::Unavailable => {
                        Err(SubstrateError::NodeDeleteNotAdmitted {
                            message: "worker temporarily unreachable".to_owned(),
                        })
                    }
                    Worker::Refuses => Err(SubstrateError::RetainedFilesystemOnlyCopy {
                        message: "filesystem must be retained".to_owned(),
                    }),
                    Worker::SlowSuccess => {
                        tokio::time::sleep(Duration::from_secs(145)).await;
                        Ok(())
                    }
                }
            }
        })
        .await;
        let attempts = attempts.into_inner();
        match worker {
            Worker::Recovers => {
                assert!(
                    result.is_ok(),
                    "rollback must survive a brief worker outage"
                );
                assert!(started.elapsed() >= Duration::from_secs(20));
                assert!(started.elapsed() < Duration::from_secs(144));
            }
            Worker::Unavailable => {
                assert!(matches!(
                    result,
                    Err(SubstrateError::NodeDeleteNotAdmitted { .. })
                ));
                assert_eq!(started.elapsed(), Duration::from_secs(144));
                assert!(
                    attempts.last().expect("initial attempt")
                        < &(started + Duration::from_secs(144))
                );
            }
            Worker::Refuses => {
                assert!(matches!(
                    result,
                    Err(SubstrateError::RetainedFilesystemOnlyCopy { .. })
                ));
                assert_eq!(attempts.len(), 1);
                assert_eq!(started.elapsed(), Duration::ZERO);
            }
            Worker::SlowSuccess => {
                assert!(result.is_ok(), "do not cancel a destructive teardown");
                assert_eq!(attempts.len(), 1);
                assert_eq!(started.elapsed(), Duration::from_secs(145));
            }
        }
        for pair in attempts.windows(2) {
            assert!(
                pair[1] - pair[0] >= Duration::from_millis(250),
                "{worker:?}: immediate failures must not spin"
            );
            assert!(
                pair[1] - pair[0] <= Duration::from_secs(5),
                "{worker:?}: backoff must stay bounded"
            );
        }
    }
}
