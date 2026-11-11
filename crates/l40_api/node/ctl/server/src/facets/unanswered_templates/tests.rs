use super::*;

use std::time::Instant;

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(ToString::to_string).collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(ToString::to_string).collect()
}

#[test]
fn unanswered_templates_wait_out_the_release_before_deletion() {
    let sync = Duration::from_secs(60);
    let after = 10 * sync;
    let limits = |limit| PlanLimits {
        after,
        max_gap: 3 * sync,
        limit,
    };
    let start = Instant::now();
    let minute = |minute: u32| start + minute * sync;
    let local = names(&["generic", "left-b", "left-a"]);
    let mut unanswered = UnansweredTemplates::default();
    let full_sync = |unanswered: &mut UnansweredTemplates, at: u32, released: &[&str]| {
        unanswered.plan(&local, &set(released), true, minute(at), limits(10))
    };

    // `generic` is never released. `left-b` is released from minute 0;
    // `left-a` too, but an answer at minute 5 stops releasing it. The one
    // released longest sorts last by name, so order by name would fail.
    for at in 0..=9 {
        let released: &[&str] = if at == 5 {
            &["left-b"]
        } else {
            &["left-b", "left-a"]
        };
        assert!(
            full_sync(&mut unanswered, at, released).is_empty(),
            "nothing is deleted inside the wait (minute {at})"
        );
    }
    assert_eq!(
        full_sync(&mut unanswered, 10, &["left-b", "left-a"]),
        ["left-b"],
        "an answer that stops releasing a template restarts its wait"
    );
    for at in 11..=15 {
        full_sync(&mut unanswered, at, &["left-b", "left-a"]);
    }
    assert_eq!(
        full_sync(&mut unanswered, 16, &["left-b", "left-a"]),
        ["left-b", "left-a"],
        "longest-released first"
    );
    assert_eq!(
        unanswered.plan(
            &local,
            &set(&["left-b", "left-a"]),
            true,
            minute(17),
            limits(1)
        ),
        ["left-b"],
        "capped per sync"
    );

    // A run scoped to other templates deletes nothing and leaves them alone.
    assert!(
        unanswered
            .plan(
                &names(&["generic"]),
                &set(&[]),
                false,
                minute(18),
                limits(10)
            )
            .is_empty()
    );
    assert_eq!(unanswered.due(minute(18), after, 10), ["left-b", "left-a"]);

    // A gap longer than a few syncs, such as a cluster outage, restarts every
    // wait: the first answer after it starts the clocks again.
    let full_sync = |unanswered: &mut UnansweredTemplates, at: u32, released: &[&str]| {
        unanswered.plan(&local, &set(released), true, minute(at), limits(10))
    };
    assert!(full_sync(&mut unanswered, 22, &["left-b", "left-a"]).is_empty());
    for at in 23..=31 {
        assert!(full_sync(&mut unanswered, at, &["left-b", "left-a"]).is_empty());
    }
    assert_eq!(
        full_sync(&mut unanswered, 32, &["left-b", "left-a"]),
        ["left-a", "left-b"],
        "a full wait after the gap"
    );

    // A full answer drops templates no longer local, and one that releases
    // nothing, as an older cluster-api sends, drops every record.
    unanswered.plan(
        &names(&["left-b"]),
        &set(&["left-b"]),
        true,
        minute(33),
        limits(10),
    );
    assert_eq!(unanswered.due(minute(33), after, 10), ["left-b"]);
    assert!(full_sync(&mut unanswered, 34, &[]).is_empty());
    assert!(unanswered.due(minute(60), after, 10).is_empty());
}

/// Scripted cluster and template-manager answers per template; missing means
/// still released and deleted.
#[derive(Default)]
struct Scripted {
    released: BTreeMap<&'static str, Result<bool, tonic::Status>>,
    deletes: BTreeMap<&'static str, Result<(), tonic::Status>>,
    hang_recheck: BTreeSet<&'static str>,
    hang_delete: BTreeSet<&'static str>,
    rechecked: Vec<String>,
}

impl UnansweredTemplateCalls for Scripted {
    async fn still_released(&mut self, template: &str) -> Result<bool, tonic::Status> {
        self.rechecked.push(template.to_string());
        if self.hang_recheck.contains(template) {
            std::future::pending::<()>().await;
        }
        self.released.get(template).cloned().unwrap_or(Ok(true))
    }

    async fn delete_idle(&mut self, template: &str, idle: Duration) -> Result<(), tonic::Status> {
        assert_eq!(
            idle, UNANSWERED_TEMPLATE_EVICT_AFTER,
            "the idle bound is the wait"
        );
        if self.hang_delete.contains(template) {
            std::future::pending::<()>().await;
        }
        self.deletes.get(template).cloned().unwrap_or(Ok(()))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Next {
    /// No record: deleted, gone, or no longer released.
    Forgotten,
    /// Due again on the next sync.
    DueNextSync,
    /// Waits the full period again.
    WaitsAgain,
}

#[derive(Clone, Copy)]
enum Hang {
    No,
    Recheck,
    Delete,
}

/// One row of `unanswered_template_deletes_act_only_on_a_fresh_release`.
struct DeleteCase {
    template: &'static str,
    local: bool,
    recheck: Result<bool, tonic::Status>,
    delete: Result<(), tonic::Status>,
    hang: Hang,
    deleted: bool,
    next: Next,
}

fn delete_case(
    template: &'static str,
    recheck: Result<bool, tonic::Status>,
    delete: Result<(), tonic::Status>,
    hang: Hang,
    deleted: bool,
    next: Next,
) -> DeleteCase {
    DeleteCase {
        template,
        local: true,
        recheck,
        delete,
        hang,
        deleted,
        next,
    }
}

#[tokio::test(start_paused = true)]
async fn unanswered_template_deletes_act_only_on_a_fresh_release() {
    use tonic::Status;
    let after = UNANSWERED_TEMPLATE_EVICT_AFTER;
    let cases = [
        delete_case("deleted", Ok(true), Ok(()), Hang::No, true, Next::Forgotten),
        DeleteCase {
            local: false,
            ..delete_case(
                "no longer local",
                Ok(true),
                Ok(()),
                Hang::No,
                false,
                Next::Forgotten,
            )
        },
        delete_case(
            "answered again",
            Ok(false),
            Ok(()),
            Hang::No,
            false,
            Next::Forgotten,
        ),
        delete_case(
            "recheck fails",
            Err(Status::unavailable("cluster-api down")),
            Ok(()),
            Hang::No,
            false,
            Next::DueNextSync,
        ),
        delete_case(
            "recheck hangs",
            Ok(true),
            Ok(()),
            Hang::Recheck,
            false,
            Next::DueNextSync,
        ),
        delete_case(
            "already gone",
            Ok(true),
            Err(Status::not_found("gone")),
            Hang::No,
            false,
            Next::Forgotten,
        ),
        delete_case(
            "in use",
            Ok(true),
            Err(Status::failed_precondition("used within the idle period")),
            Hang::No,
            false,
            Next::WaitsAgain,
        ),
        delete_case(
            "gate busy",
            Ok(true),
            Err(Status::aborted("cache GC running")),
            Hang::No,
            false,
            Next::DueNextSync,
        ),
        delete_case(
            "delete hangs",
            Ok(true),
            Ok(()),
            Hang::Delete,
            false,
            Next::DueNextSync,
        ),
        delete_case(
            "old template-manager",
            Ok(true),
            Err(Status::unimplemented("no such method")),
            Hang::No,
            false,
            Next::WaitsAgain,
        ),
        delete_case(
            "delete fails",
            Ok(true),
            Err(Status::internal("disk")),
            Hang::No,
            false,
            Next::WaitsAgain,
        ),
    ];
    for DeleteCase {
        template,
        local,
        recheck,
        delete,
        hang,
        deleted,
        next,
    } in cases
    {
        let tracker = std::sync::Mutex::new(UnansweredTemplates::default());
        let start = Instant::now();
        let limits = PlanLimits {
            after,
            max_gap: after,
            limit: 10,
        };
        tracker
            .lock()
            .unwrap()
            .plan(&names(&[template]), &set(&[template]), true, start, limits);
        let mut calls = Scripted::default();
        calls.released.insert(template, recheck);
        calls.deletes.insert(template, delete);
        match hang {
            Hang::No => {}
            Hang::Recheck => {
                calls.hang_recheck.insert(template);
            }
            Hang::Delete => {
                calls.hang_delete.insert(template);
            }
        }

        let count =
            delete_unanswered_templates(&tracker, names(&[template]), |_| local, &mut calls, after)
                .await;

        assert_eq!(count, u64::from(deleted), "{template}: deleted count");
        let tracker = tracker.lock().unwrap();
        let seen = if !tracker.since.contains_key(template) {
            Next::Forgotten
        } else if tracker.due(start + after, after, 10).is_empty() {
            Next::WaitsAgain
        } else {
            Next::DueNextSync
        };
        assert_eq!(seen, next, "{template}");
        assert_eq!(
            calls.rechecked.is_empty(),
            !local,
            "{template}: rechecked only when local"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn unanswered_template_deletes_stop_at_the_sync_budget() {
    let after = UNANSWERED_TEMPLATE_EVICT_AFTER;
    let due = names(&["a", "b", "c"]);
    let tracker = std::sync::Mutex::new(UnansweredTemplates::default());
    let start = Instant::now();
    tracker.lock().unwrap().plan(
        &due,
        &set(&["a", "b", "c"]),
        true,
        start,
        PlanLimits {
            after,
            max_gap: after,
            limit: 10,
        },
    );
    // Each recheck runs into its deadline, so two use up the budget.
    let mut calls = Scripted {
        hang_recheck: ["a", "b", "c"].into_iter().collect(),
        ..Scripted::default()
    };

    let count = delete_unanswered_templates(&tracker, due, |_| true, &mut calls, after).await;

    assert_eq!(count, 0);
    assert_eq!(
        calls.rechecked,
        ["a", "b"],
        "the third waits for the next sync"
    );
    assert_eq!(
        tracker.lock().unwrap().due(start + after, after, 10),
        ["a", "b", "c"],
        "every record is kept"
    );
}
