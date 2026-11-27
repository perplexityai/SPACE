//! Gives back a Direct restore lease whose owner is dropped before it asks a
//! worker for anything.
//!
//! A restore claims its lease, then pays admission, then asks a worker to
//! start the sandbox. A caller that goes away while admission makes it wait
//! drops the restore there, holding a lease nothing runs under: the row
//! stays `restoring` and every restore is refused until the lease expires.
//! The same holds for a restore dropped after admission refused it, while
//! its own release of the lease still waits. Only those two waits are
//! covered. Once a worker has been asked, the lease fences what that worker
//! may still be doing and has to run out on its own.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::direct as sandbox_direct;
use sqlx::PgPool;

use super::DirectRestoreLease;

/// Holds a claimed lease until the restore has either been admitted or
/// given a refused lease back. [`Self::settled`] then: the restore is still
/// running and ends the lease itself, or already has. Dropped before that,
/// the restore was cancelled, and the lease is released as one admission
/// refused is ([`sandbox_direct::release_undispatched_restore_lease`]).
pub(super) struct UndispatchedRestoreLease {
    held: Option<(PgPool, SandboxId, DirectRestoreLease)>,
}

impl UndispatchedRestoreLease {
    pub(super) fn held(pool: PgPool, sandbox_id: SandboxId, lease: DirectRestoreLease) -> Self {
        Self {
            held: Some((pool, sandbox_id, lease)),
        }
    }

    pub(super) fn settled(&mut self) {
        self.held = None;
    }
}

impl Drop for UndispatchedRestoreLease {
    fn drop(&mut self) {
        let Some((pool, sandbox_id, lease)) = self.held.take() else {
            return;
        };
        // `drop` can't await, so the release runs on a task of its own. It
        // must never panic: with no runtime to run it on, the lease is left
        // to expire, as it was before this guard.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                %sandbox_id,
                "direct restore dropped at admission with no runtime to release its lease on; \
                 the lease expires on its own"
            );
            return;
        };
        runtime.spawn(async move {
            let released = sandbox_direct::release_undispatched_restore_lease(
                &pool,
                sandbox_id,
                lease.token,
                lease.restore_retry,
            )
            .await;
            // `Ok(false)` is a lease the row no longer holds: the dropped
            // restore's own release of a refused lease landed first, or
            // another restore has claimed the row since.
            if let Err(error) = released {
                tracing::warn!(
                    %sandbox_id,
                    %error,
                    "failed to release the lease of a direct restore dropped at admission; \
                     the lease expires on its own"
                );
            }
        });
    }
}
