//! Password verification runs off the async workers, under a bounded number
//! of concurrent slots.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pplx_space_test_env::TestResult;

use crate::auth::{
    password_verification_slot, password_verification_slots, verify_password,
    verify_password_blocking,
};

#[test]
fn slots_leave_cores_for_other_requests() {
    let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
    let slots = password_verification_slots();
    assert!(slots >= 1, "{slots}");
    assert!(slots <= cores.div_ceil(2), "{slots} of {cores}");
}

#[tokio::test]
async fn blocking_verification_matches_the_inline_verifier() -> TestResult {
    let hash = bcrypt::hash("right-password", 4)?;
    for (password, hash) in [
        ("right-password", hash.as_str()),
        ("wrong-password", hash.as_str()),
        ("right-password", "!"),
    ] {
        assert_eq!(
            verify_password_blocking(password.to_owned(), hash.to_owned()).await??,
            verify_password(password, hash)?,
            "{hash}"
        );
    }
    let _slot = password_verification_slot().await?;
    Ok(())
}

/// On a single-threaded runtime, another task keeps running while a costly
/// verification is in progress, which it could not if the verification ran
/// on the async worker.
#[tokio::test(flavor = "current_thread")]
async fn verification_does_not_block_the_async_worker() -> TestResult {
    let hash = bcrypt::hash("right-password", 10)?;
    let done = Arc::new(AtomicBool::new(false));
    let verify = {
        let done = Arc::clone(&done);
        async move {
            let verified = verify_password_blocking("right-password".to_owned(), hash).await;
            done.store(true, Ordering::SeqCst);
            verified
        }
    };
    let ticker = async {
        let mut ticks = 0_u32;
        while !done.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
            ticks += 1;
        }
        ticks
    };
    let (verified, ticks) = tokio::join!(verify, ticker);
    assert!(verified??);
    assert!(
        ticks >= 1,
        "the async worker was blocked during verification"
    );
    Ok(())
}
