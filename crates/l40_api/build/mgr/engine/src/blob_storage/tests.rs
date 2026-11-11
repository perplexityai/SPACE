use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use pplx_space_common_core::host::disk::DiskStatus;
use rstest::{fixture, rstest};

use super::*;

struct TestStorageProbe {
    available_bytes: AtomicU64,
}

impl TestStorageProbe {
    fn set_available_bytes(&self, available_bytes: u64) {
        self.available_bytes
            .store(available_bytes, Ordering::Release);
    }
}

impl StorageProbe for TestStorageProbe {
    fn status(&self, _root: &Path) -> Option<DiskStatus> {
        let available_bytes = self.available_bytes.load(Ordering::Acquire);
        Some(DiskStatus {
            total_bytes: 1_000,
            free_bytes: available_bytes,
            available_bytes,
        })
    }
}

struct StorageFixture {
    admission: BlobStorageAdmission,
    probe: Arc<TestStorageProbe>,
}

#[fixture]
fn storage_fixture() -> StorageFixture {
    let probe = Arc::new(TestStorageProbe {
        available_bytes: AtomicU64::new(100),
    });
    StorageFixture {
        admission: BlobStorageAdmission::for_test(PathBuf::from("/test/cache"), 20, probe.clone()),
        probe,
    }
}

#[rstest]
fn upload_is_rejected_without_capacity_for_its_full_budget(storage_fixture: StorageFixture) {
    storage_fixture.probe.set_available_bytes(59);

    let result = storage_fixture.admission.reserve(40);

    assert!(matches!(
        result,
        Err(BlobStorageError::Admission(
            StorageAdmissionError::AdmissionRejected {
                available_bytes: 59,
                required_available_bytes: 60,
            }
        ))
    ));
}

#[rstest]
fn active_upload_stops_when_reserved_headroom_is_lost(storage_fixture: StorageFixture) {
    let mut reservation = storage_fixture
        .admission
        .reserve(40)
        .expect("reserve upload");
    storage_fixture.probe.set_available_bytes(58);

    let result = reservation.record_written(1);

    assert!(matches!(
        result,
        Err(BlobStorageError::Admission(
            StorageAdmissionError::HeadroomExhausted {
                available_bytes: 58,
                required_available_bytes: 59,
            }
        ))
    ));
}

#[rstest]
fn concurrent_uploads_share_available_capacity(storage_fixture: StorageFixture) {
    let first = storage_fixture
        .admission
        .reserve(40)
        .expect("reserve first upload");
    let second = storage_fixture
        .admission
        .reserve(40)
        .expect("reserve second upload");

    let result = storage_fixture.admission.reserve(1);

    assert!(matches!(
        result,
        Err(BlobStorageError::Admission(
            StorageAdmissionError::AdmissionRejected {
                available_bytes: 100,
                required_available_bytes: 101,
            }
        ))
    ));
    drop((first, second));
}
