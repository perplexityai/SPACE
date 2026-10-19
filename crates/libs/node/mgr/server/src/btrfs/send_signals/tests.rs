use super::*;

#[test]
fn fault_delivery_is_preserved_and_unwind_restores_the_thread_mask() {
    std::thread::spawn(|| {
        let before = SigSet::thread_get_mask().unwrap();
        let mut during = None;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _signals = BlockedSignals::new().unwrap();
            during = Some(SigSet::thread_get_mask().unwrap());
            panic!("exercise unwinding through the signal guard");
        }));
        outcome.unwrap_err();
        let during = during.expect("guard must be entered before the deliberate panic");
        assert!(during.contains(Signal::SIGUSR2));
        for fault in [
            Signal::SIGSEGV,
            Signal::SIGBUS,
            Signal::SIGILL,
            Signal::SIGFPE,
            Signal::SIGTRAP,
            Signal::SIGSYS,
        ] {
            assert_eq!(during.contains(fault), before.contains(fault), "{fault}");
        }
        assert_eq!(SigSet::thread_get_mask().unwrap(), before);
    })
    .join()
    .unwrap();
}
