use super::*;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Success,
    Failure,
    Shutdown,
    Panic,
}

#[test]
fn containment_bounds_parallel_work_and_joins_before_returning() {
    for outcome in [
        Outcome::Success,
        Outcome::Failure,
        Outcome::Shutdown,
        Outcome::Panic,
    ] {
        let namespaces: Vec<_> = (0..11).map(|i| i.to_string()).collect();
        let shutdown = CancellationToken::new();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let (entered, arrivals) = mpsc::channel();
        let worker = {
            let (shutdown, gate, active, peak, completed) = (
                shutdown.clone(),
                gate.clone(),
                active.clone(),
                peak.clone(),
                completed.clone(),
            );
            std::thread::spawn(move || {
                contain_namespaces(
                    &namespaces,
                    NonZeroUsize::new(3).unwrap(),
                    &shutdown,
                    |namespace| {
                        struct InFlight<'a>(&'a AtomicUsize, &'a AtomicUsize);
                        impl Drop for InFlight<'_> {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, Ordering::SeqCst);
                                self.1.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                        let _in_flight = InFlight(&active, &completed);
                        peak.fetch_max(count, Ordering::SeqCst);
                        entered.send(()).unwrap();
                        let (lock, released) = &*gate;
                        let open = released
                            .wait_while(lock.lock().unwrap(), |open| !*open)
                            .unwrap();
                        drop(open);
                        if namespace == "0" {
                            match outcome {
                                Outcome::Failure => {
                                    return Err(PoolOpenError::Configuration(
                                        "synthetic containment failure".into(),
                                    ));
                                }
                                Outcome::Panic => panic!("synthetic containment panic"),
                                _ => {}
                            }
                        }
                        Ok(true)
                    },
                )
            })
        };
        // Explicit synchronization proves overlap without assuming command latency.
        // Always release and join, including when the sequential implementation fails.
        let overlapping = (0..3).all(|_| arrivals.recv_timeout(Duration::from_secs(2)).is_ok());
        assert!(!worker.is_finished());
        if matches!(outcome, Outcome::Shutdown) {
            shutdown.cancel();
        }
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let result = worker.join();
        assert!(
            overlapping,
            "{outcome:?}: independent namespaces must overlap"
        );
        assert_eq!(
            active.load(Ordering::SeqCst),
            0,
            "{outcome:?}: no work may outlive startup"
        );
        assert_eq!(peak.load(Ordering::SeqCst), 3);
        match outcome {
            Outcome::Success => assert_eq!(result.unwrap().unwrap(), 11),
            Outcome::Failure => {
                assert!(matches!(
                    result.unwrap(),
                    Err(PoolOpenError::Configuration(_))
                ));
                assert_eq!(completed.load(Ordering::SeqCst), 11);
            }
            Outcome::Shutdown => {
                assert!(matches!(
                    result.unwrap(),
                    Err(PoolOpenError::StartupCancelled)
                ));
                assert_eq!(completed.load(Ordering::SeqCst), 3);
            }
            Outcome::Panic => {
                result.unwrap_err();
            }
        }
    }
}

#[cfg(target_os = "linux")]
mod linux;
