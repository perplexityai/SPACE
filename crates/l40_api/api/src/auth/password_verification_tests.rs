//! Password verification runs off the async workers, under a bounded number
//! of concurrent slots.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pplx_space_test_env::TestResult;
use uuid::Uuid;

use crate::auth::{
    password_verification_slot, password_verification_slots, verify_password,
    verify_password_blocking,
};

/// A fresh random password per test, as the other fixtures generate theirs.
fn random_password() -> String {
    Uuid::now_v7().to_string()
}

#[test]
fn slots_leave_cores_for_other_requests() {
    let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
    let slots = password_verification_slots();
    assert!(slots >= 1);
    assert!(slots <= cores.div_ceil(2));
}

#[tokio::test]
async fn blocking_verification_matches_the_inline_verifier() -> TestResult {
    let right = random_password();
    let wrong = random_password();
    let hash = bcrypt::hash(&right, 4)?;
    let locked = "!".to_owned();
    for (case, password, hash, expected) in [
        ("right password", &right, &hash, true),
        ("wrong password", &wrong, &hash, false),
        ("no local password", &right, &locked, false),
    ] {
        let slot = password_verification_slot().await?;
        let blocking = verify_password_blocking(password.clone(), hash.clone(), slot).await??;
        assert_eq!(blocking, expected, "{case}");
        assert_eq!(verify_password(password, hash)?, expected, "{case}");
    }
    Ok(())
}

/// The slot is held by the verification itself: cancelling the login that
/// started it does not free it until bcrypt finishes.
#[tokio::test]
async fn a_cancelled_login_keeps_its_slot_until_verification_ends() -> TestResult {
    let semaphore: &'static tokio::sync::Semaphore =
        Box::leak(Box::new(tokio::sync::Semaphore::new(1)));
    let password = random_password();
    let hash = bcrypt::hash(&password, 10)?;
    let slot = semaphore.acquire().await?;
    let verification = tokio::spawn(verify_password_blocking(password, hash, slot));
    // Let the blocking task start, then cancel the login awaiting it.
    tokio::time::sleep(Duration::from_millis(5)).await;
    verification.abort();
    // Wait until the cancelled login has actually been dropped.
    assert!(verification.await.is_err_and(|error| error.is_cancelled()));
    assert!(
        semaphore.try_acquire().is_err(),
        "the slot was freed while bcrypt was still running"
    );
    // Released once the verification ends.
    let reacquired = tokio::time::timeout(Duration::from_secs(30), semaphore.acquire()).await??;
    drop(reacquired);
    Ok(())
}

/// On a single-threaded runtime, another task keeps running while a costly
/// verification is in progress, which it could not if the verification ran
/// on the async worker.
#[tokio::test(flavor = "current_thread")]
async fn verification_does_not_block_the_async_worker() -> TestResult {
    let password = random_password();
    let hash = bcrypt::hash(&password, 10)?;
    let done = Arc::new(AtomicBool::new(false));
    let verify = {
        let done = Arc::clone(&done);
        async move {
            let slot = password_verification_slot().await?;
            let verified = verify_password_blocking(password, hash, slot).await;
            done.store(true, Ordering::SeqCst);
            verified
        }
    };
    let ticker = async {
        let mut ticks = 0_u32;
        while !done.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
            if done.load(Ordering::SeqCst) {
                break;
            }
            ticks += 1;
        }
        ticks
    };
    // Biased, so `verify` is always polled first: an inline verification
    // would finish and set `done` before the ticker ever checks it.
    let (verified, ticks) = tokio::join!(biased; verify, ticker);
    assert!(verified??);
    assert!(
        ticks >= 1,
        "the async worker was blocked during verification"
    );
    Ok(())
}
