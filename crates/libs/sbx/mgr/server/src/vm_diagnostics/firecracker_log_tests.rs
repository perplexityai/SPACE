use super::*;

fn line(level: &str, message: &str) -> String {
    format!("2026-01-01T00:00:00.000000000 [anonymous-instance:fc_vcpu 0:{level}] {message}\n")
}

/// Firecracker's SIGPIPE line comes in bursts and the VMM continues after it,
/// so it must not draw on the sandbox's allowance and leave a fatal line after
/// it rate limited. Every other ERROR line, including ones that only resemble
/// it, still draws on the budget.
#[test]
fn only_errors_other_than_sigpipe_draw_on_the_budget() {
    let real = [
        "Shutting down VM after intercepting signal 7, code 2.",
        "Received invalid signal 13, code 0.",
        "Received signal 130, code 0.",
    ];
    let mut chunk = String::new();
    for _ in 0..LogBudget::BURST + 5 {
        chunk.push_str(&line("ERROR", "Received signal 13, code 0."));
    }
    for message in real {
        chunk.push_str(&line("ERROR", message));
    }
    let mut tail = LogTail::new(PathBuf::from("unused"), SystemTime::now());
    let mut budget = LogBudget::new();

    tail.accept("sandbox-a", chunk.as_bytes(), &mut budget);

    assert_eq!(budget.tokens, LogBudget::BURST - 3);
}
