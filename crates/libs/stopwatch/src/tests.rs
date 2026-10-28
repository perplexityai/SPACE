use std::time::Duration;

use super::Stopwatch;

#[test]
fn runs_accumulate_including_early_exits() {
    let pause = Duration::from_millis(5);
    let mut stopwatch = Stopwatch::new();
    stopwatch.time(|| std::thread::sleep(pause));
    let early: Result<(), ()> = (|| {
        let _run = stopwatch.start();
        std::thread::sleep(pause);
        Err(())?;
        Ok(())
    })();
    assert_eq!(early, Err(()));
    let received: Vec<_> = stopwatch
        .time_each(
            [pause]
                .into_iter()
                .inspect(|pause| std::thread::sleep(*pause)),
        )
        .collect();
    assert_eq!(received, [pause]);
    assert!(stopwatch.elapsed() >= pause * 3);
}
