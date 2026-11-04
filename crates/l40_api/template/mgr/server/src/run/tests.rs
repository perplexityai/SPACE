use std::future::{pending, ready};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use super::*;

#[test]
fn runtime_stops_detached_tasks_before_releasing_service_ownership() {
    struct ObserveOwnershipOnDrop {
        contender: std::fs::File,
        retained: Arc<AtomicBool>,
    }

    impl Drop for ObserveOwnershipOnDrop {
        fn drop(&mut self) {
            self.retained.store(
                matches!(
                    self.contender.try_lock(),
                    Err(std::fs::TryLockError::WouldBlock)
                ),
                Ordering::SeqCst,
            );
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("service.mutex");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let service_mutex = runtime
        .block_on(ServiceMutex::acquire(
            &path,
            CancellationToken::new(),
            Duration::from_millis(1),
            None,
        ))
        .unwrap();
    let service = ServiceRuntime {
        runtime,
        service_mutex: Some(service_mutex),
    };
    let retained = Arc::new(AtomicBool::new(false));
    let witness = ObserveOwnershipOnDrop {
        contender: std::fs::File::open(&path).unwrap(),
        retained: Arc::clone(&retained),
    };
    let (started, running) = tokio::sync::oneshot::channel();
    service.runtime.spawn(async move {
        let _witness = witness;
        started.send(()).unwrap();
        pending::<()>().await;
    });
    service.runtime.block_on(running).unwrap();

    drop(service);

    assert!(retained.load(Ordering::SeqCst));
    std::fs::File::open(path).unwrap().try_lock().unwrap();
}

#[tokio::test]
async fn drain_requires_worker_and_server_quiescence() {
    for (worker_quiescent, server_quiescent) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let completed = drain_service(
            ready(worker_quiescent),
            async {
                if !server_quiescent {
                    pending::<()>().await;
                }
            },
            Duration::ZERO,
        )
        .await;
        assert_eq!(
            completed,
            worker_quiescent && server_quiescent,
            "worker_quiescent={worker_quiescent}, server_quiescent={server_quiescent}",
        );
    }
}
