//! In-process S3 stand-ins and a store wired to them, shared by the tests
//! that drive the real store over HTTP.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::StalledStreamProtectionConfig;
use aws_sdk_s3::config::retry::RetryConfig;
use axum::Router;

use crate::attempts::{
    AttemptObserver, AttemptReporter, AttemptTarget, FailedAttempt, RecoveredOperation,
};
use crate::timeouts::{ClassTimeouts, S3Timeouts};
use crate::{
    DEFAULT_POOL_IDLE_TIMEOUT, ObjectCredentials, ObjectTarget, S3ObjectStore, s3_base_config,
};

/// Records attempt reports instead of logging them.
#[derive(Debug, Default)]
pub(crate) struct Recorder {
    failures: Mutex<Vec<(AttemptTarget, FailedAttempt)>>,
    recoveries: Mutex<Vec<(AttemptTarget, RecoveredOperation)>>,
}

impl Recorder {
    pub(crate) fn failures(&self) -> Vec<FailedAttempt> {
        self.failed_targets()
            .into_iter()
            .map(|(_, failure)| failure)
            .collect()
    }

    pub(crate) fn failed_targets(&self) -> Vec<(AttemptTarget, FailedAttempt)> {
        self.failures.lock().unwrap().clone()
    }

    pub(crate) fn recoveries(&self) -> Vec<(AttemptTarget, RecoveredOperation)> {
        self.recoveries.lock().unwrap().clone()
    }
}

impl AttemptReporter for Recorder {
    fn attempt_failed(&self, target: &AttemptTarget, failure: &FailedAttempt) {
        self.failures
            .lock()
            .unwrap()
            .push((target.clone(), failure.clone()));
    }

    fn recovered(&self, target: &AttemptTarget, recovery: &RecoveredOperation) {
        self.recoveries
            .lock()
            .unwrap()
            .push((target.clone(), recovery.clone()));
    }
}

/// Serves `router` on a loopback port and returns its endpoint URL.
pub(crate) async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind S3 stand-in");
    let address = listener.local_addr().expect("S3 stand-in address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("serve S3 stand-in");
    });
    format!("http://{address}")
}

/// The same bounds for every timeout class.
pub(crate) fn every_class(attempt: Duration, operation: Duration) -> S3Timeouts {
    let class = ClassTimeouts { attempt, operation };
    S3Timeouts {
        request: class,
        body_upload: class,
        server_side: class,
    }
}

/// A store wired exactly like production apart from the bounds under test.
/// Retry backoff is shortened so wall time is spent in the attempts
/// themselves; the strategy and its three attempts are the SDK standard.
pub(crate) fn store(
    timeouts: S3Timeouts,
    stall_grace: Duration,
    recorder: &Arc<Recorder>,
) -> S3ObjectStore {
    store_with_pool_idle(timeouts, stall_grace, DEFAULT_POOL_IDLE_TIMEOUT, recorder)
}

/// [`store`] with the connection pool's idle timeout under test too.
pub(crate) fn store_with_pool_idle(
    timeouts: S3Timeouts,
    stall_grace: Duration,
    pool_idle_timeout: Duration,
    recorder: &Arc<Recorder>,
) -> S3ObjectStore {
    let attempts = AttemptObserver::new(recorder.clone());
    let conf = s3_base_config(timeouts, pool_idle_timeout, attempts.clone())
        .retry_config(RetryConfig::standard().with_initial_backoff(Duration::from_millis(10)))
        .stalled_stream_protection(
            StalledStreamProtectionConfig::enabled()
                .grace_period(stall_grace)
                .build(),
        )
        .build();
    S3ObjectStore::with_base_config(conf, timeouts, pool_idle_timeout, attempts)
}

pub(crate) fn credentials() -> ObjectCredentials {
    ObjectCredentials {
        access_key_id: "synthetic-access-key".to_string(),
        secret_access_key: "synthetic-secret-key".to_string(),
        session_token: None,
    }
}

pub(crate) fn target(endpoint: &str) -> ObjectTarget<'_> {
    ObjectTarget {
        bucket: "synthetic-bucket",
        key: "snapshots/synthetic/memory.bin",
        region: "us-east-1",
        endpoint: Some(endpoint),
        path_style: true,
    }
}
