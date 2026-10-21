use std::sync::{Arc, Mutex};

use pplx_space_common_core::host::disk::DiskStatus;

use super::*;

const TOTAL_BYTES: u64 = 1_000;

fn config() -> CacheDiskAdmissionConfig {
    CacheDiskAdmissionConfig {
        enabled: true,
        min_available_fraction: 0.05,
        warning_available_fraction: 0.10,
        min_available_bytes: 0,
        allocation_reservation_bytes: 10,
    }
}

/// A probe whose next result the test sets. `None` fails like a timed-out
/// `btrfs filesystem usage`.
fn scripted_probe() -> (Arc<Mutex<Option<u64>>>, CacheDiskProbe) {
    let available = Arc::new(Mutex::new(None));
    let probe = {
        let available = Arc::clone(&available);
        Arc::new(move || {
            let available_bytes = (*available.lock().unwrap())
                .ok_or_else(|| CacheDiskProbeError::Btrfs("timed out".to_string()))?;
            Ok(CacheDiskMeasurement {
                status: DiskStatus {
                    total_bytes: TOTAL_BYTES,
                    free_bytes: available_bytes,
                    available_bytes,
                },
                btrfs: None,
            })
        }) as CacheDiskProbe
    };
    (available, probe)
}

#[tokio::test]
async fn failed_post_allocation_probe_keeps_admission_open_and_reservation_charged() {
    let (available, probe) = scripted_probe();
    let admission = DiskAdmission::with_probe(config(), probe).await;
    *available.lock().unwrap() = Some(65);
    admission
        .refresh_once_result(CacheDiskRefreshFailure::KeepLastGood)
        .await
        .unwrap();

    let reservation = admission
        .reserve(DiskAllocationOperation::PrepareSandbox, None)
        .unwrap();
    // The allocation consumes its 10 bytes, then the follow-up probe fails.
    *available.lock().unwrap() = None;
    admission.release_after_success(reservation).await;

    let decision = admission.decision();
    assert!(
        decision.allows_new_allocation(),
        "one failed probe closed admission: {}",
        decision.reason
    );
    // The cached 65 bytes predate the allocation, so it stays charged.
    assert_eq!(admission.pending_reservation_bytes(), 10);
    assert_eq!(decision.effective_available_bytes, 55);

    *available.lock().unwrap() = Some(55);
    admission
        .refresh_once_result(CacheDiskRefreshFailure::KeepLastGood)
        .await
        .unwrap();
    let decision = admission.decision();
    assert_eq!(admission.pending_reservation_bytes(), 0);
    assert_eq!(decision.effective_available_bytes, 55);

    // Space consumed without a reservation still invalidates on failure.
    *available.lock().unwrap() = None;
    admission.refresh_after_external_allocation().await;
    assert_eq!(
        admission.decision().reason,
        CacheDiskAdmissionReason::DiskUnknownStale
    );
}
