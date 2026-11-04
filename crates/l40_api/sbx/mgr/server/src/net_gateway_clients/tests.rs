use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pplx_space_common_core::types::{LinuxPid, SandboxId};

use super::KeptClients;

/// Each dial returns the next number, so a returned value says which dial
/// produced the client.
struct Dialer(AtomicU32);

impl Dialer {
    fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    async fn dial(&self) -> Result<u32, &'static str> {
        Ok(self.0.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn dials(&self) -> u32 {
        self.0.load(Ordering::SeqCst)
    }
}

fn pid(raw: u32) -> Option<LinuxPid> {
    Some(LinuxPid::try_from_u32(raw).unwrap())
}

async fn get(
    kept: &KeptClients<u32>,
    dialer: &Dialer,
    sandbox: SandboxId,
    pid: Option<LinuxPid>,
) -> u32 {
    kept.get_or_connect(sandbox, pid, || dialer.dial())
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn reuses_one_dial_per_gateway_process_until_it_ages_out() {
    let kept = KeptClients::new(Duration::from_secs(60), Duration::from_secs(1));
    let dialer = Dialer::new();
    let a = SandboxId::new();
    let b = SandboxId::new();

    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
    assert_eq!(get(&kept, &dialer, b, pid(20)).await, 2);
    tokio::time::advance(Duration::from_secs(59)).await;
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 3);
    assert_eq!(dialer.dials(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_replaced_gateway_is_dialed_afresh() {
    let kept = KeptClients::new(Duration::from_secs(60), Duration::from_secs(1));
    let dialer = Dialer::new();
    let a = SandboxId::new();

    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
    assert_eq!(get(&kept, &dialer, a, pid(11)).await, 2);
    // The replacement's client is what is kept now.
    assert_eq!(get(&kept, &dialer, a, pid(11)).await, 2);
    assert_eq!(get(&kept, &dialer, a, None).await, 3);
}

#[tokio::test(start_paused = true)]
async fn discard_and_forget_drop_the_kept_client() {
    let kept = KeptClients::new(Duration::from_secs(60), Duration::from_secs(1));
    let dialer = Dialer::new();
    let a = SandboxId::new();

    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
    // A failure seen on another process's client leaves this one kept.
    kept.discard(a, pid(9));
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
    kept.discard(a, pid(10));
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 2);
    kept.forget(a);
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 3);
}

#[tokio::test(start_paused = true)]
async fn a_failed_dial_keeps_nothing() {
    let kept = KeptClients::<u32>::new(Duration::from_secs(60), Duration::from_secs(1));
    let dialer = Dialer::new();
    let a = SandboxId::new();

    let failed = kept
        .get_or_connect(a, pid(10), || async { Err("refused") })
        .await;
    assert_eq!(failed, Err("refused"));
    assert_eq!(get(&kept, &dialer, a, pid(10)).await, 1);
}

#[tokio::test(start_paused = true)]
async fn aged_entries_of_other_sandboxes_are_dropped_by_dials_and_lookups() {
    let kept = KeptClients::new(Duration::from_secs(60), Duration::from_secs(1));
    let dialer = Dialer::new();
    let b = SandboxId::new();
    let c = SandboxId::new();
    let gone = SandboxId::new();
    let left = SandboxId::new();

    get(&kept, &dialer, gone, pid(1)).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    get(&kept, &dialer, b, pid(2)).await;
    assert!(!kept.lock().by_sandbox.contains_key(&gone));

    // A lookup that dials nothing still sweeps, once the interval has passed.
    assert_eq!(get(&kept, &dialer, left, pid(3)).await, 3);
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(get(&kept, &dialer, c, pid(4)).await, 4);
    tokio::time::advance(Duration::from_secs(31)).await;
    assert_eq!(get(&kept, &dialer, c, pid(4)).await, 4);
    assert!(!kept.lock().by_sandbox.contains_key(&left));
}
