use super::service_tests::{insert_test_sandbox, runtime_fields};
use super::*;
use crate::service::activation_support::internal_request;
use pplx_space_common_core::types::ExecutionGeneration;

#[tokio::test]
async fn cancelled_cleanup_rpcs_preserve_admitted_termination_strength() {
    // Exercise the actual stop and kill handlers.
    let cases = [
        ("stop", true, false),
        ("stop", true, true),
        ("stop", false, false),
        ("stop", false, true),
        ("kill", true, false),
        ("kill", true, true),
        ("term", true, false),
        ("term", true, true),
    ];
    let mut observed = Vec::new();
    for (operation, immediate, blocking) in cases {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        let run_dir = tempfile::tempdir().unwrap();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions::for_test_in(run_dir.path()),
        );
        service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
            )
            .unwrap();
        let sandbox_dir = run_dir.path().join(&id);
        std::fs::create_dir(&sandbox_dir).unwrap();
        std::os::unix::fs::symlink(run_dir.path(), sandbox_dir.join("fc.pid")).unwrap();
        let barrier = Arc::new(crate::service::cleanup_executor::TestAdmissionBarrier {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        *service.cleanup_executor.admission_barrier.lock().unwrap() = Some(barrier.clone());
        let (dispatched, mut dispatches) = tokio::sync::mpsc::channel(1);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |_, kill| {
            let dispatched = dispatched.clone();
            Box::pin(async move {
                dispatched.send(kill).await.unwrap();
                Ok(())
            })
        }));
        let rpc_service = service.clone();
        let rpc_id = id.clone();
        let rpc = tokio::spawn(async move {
            match operation {
                "stop" => rpc_service
                    .stop(internal_request(StopRequest {
                        sandbox_id: rpc_id,
                        template_filter: None,
                        blocking,
                        immediate_kill: immediate,
                    }))
                    .await
                    .map(|_| ()),
                "kill" | "term" => rpc_service
                    .kill(internal_request(KillRequest {
                        sandbox_id: rpc_id,
                        signal: if operation == "kill" { 9 } else { 15 },
                        blocking,
                    }))
                    .await
                    .map(|_| ()),
                _ => unreachable!(),
            }
        });
        tokio::time::timeout(Duration::from_secs(5), barrier.entered.notified())
            .await
            .unwrap();
        let (job_id, persisted_strength) = {
            let db = service.db.lock().unwrap();
            let job_id = pplx_space_common_jobs::db::find_active_job(
                &db,
                crate::db::cleanup_jobs::KIND,
                &id,
            )
            .unwrap()
            .unwrap();
            let job = pplx_space_common_jobs::db::get_job(&db, &job_id)
                .unwrap()
                .unwrap();
            (
                job_id,
                crate::db::cleanup_jobs::decode(&job.payload)
                    .unwrap()
                    .immediate_kill,
            )
        };
        rpc.abort();
        assert!(rpc.await.unwrap_err().is_cancelled());
        let dispatched_strength = tokio::time::timeout(Duration::from_secs(5), dispatches.recv())
            .await
            .unwrap()
            .unwrap();
        service.wait_cleanup_job(job_id).await.unwrap();
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
        observed.push((persisted_strength, dispatched_strength));
    }
    assert_eq!(
        observed,
        cases.map(|(_, immediate, _)| (immediate, immediate))
    );
}
