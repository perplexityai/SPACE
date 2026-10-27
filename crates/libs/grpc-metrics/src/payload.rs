//! Payload admission for the guest execution service. All labels are bounded.
//!
//! `spaced.guest.payload.admission` counts decisions, tagged by `budget`, `site`,
//! `outcome`. Transport and handler decisions are separate observations of a
//! request, not a count of unique requests. `handler/acquired` includes ordinary
//! native calls and calls whose adapter discarded its extensions; it is not a
//! permit-loss signal. The daemon's permit-handoff regression guards that contract.
//! `reused` means a matching permit; `reacquired` means a supplied permit could
//! not satisfy the requested budget/weight. `rejected` counts capacity refusals
//! only, including transport queue expiry and closed admission semaphores. It
//! excludes invalid input and lifecycle closure refusals: those checks do not
//! attempt payload admission. Service closure does not close the semaphores.
//!
//! Under the same `spaced.guest.payload` prefix, `cancelled_slots` counts weighted
//! slots still owned when cancellation is observed. `cancelled_released_slots`
//! counts their eventual release. `cancelled_hold_ms` measures time from that
//! observation until the last permit owner drops; `cancelled_slot_ms` weights
//! that time by slots. These four metrics carry only `budget`. Observation includes
//! caller cancellation and server-initiated upload aborts or shutdown. Brief
//! transport-only permit retention can produce near-zero samples: these measure
//! slot retention, not exclusively caller cancellation or physical cleanup. They
//! exclude process lifetime and time before cancellation is observed. Incomplete
//! cleanup has no duration sample yet; compare the two slot counters over the
//! same process lifetime.

use pplx_space_metrics_emit::{counter, distribution, tag};

metric_vocabulary! {
    /// Shared guest payload budget, independent of transport.
    Budget {
        GuestFilesystem => "guest_filesystem",
        GuestProcess => "guest_process",
    }
}

metric_vocabulary! {
    /// Where admission was attempted, not the caller's transport identity.
    Site {
        Transport => "transport",
        Handler => "handler",
    }
}

metric_vocabulary! {
    /// Result of one admission decision.
    Outcome {
        Acquired => "acquired",
        Reused => "reused",
        Reacquired => "reacquired",
        Rejected => "rejected",
    }
}

pub fn admission(budget: Budget, site: Site, outcome: Outcome) {
    counter(
        "spaced.guest.payload.admission",
        1,
        &[
            tag("budget", budget.as_str()),
            tag("site", site.as_str()),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn cancelled(budget: Budget, slots: usize) {
    counter(
        "spaced.guest.payload.cancelled_slots",
        slots as u64,
        &[tag("budget", budget.as_str())],
    );
}

pub fn released_after_cancellation(budget: Budget, slots: usize, elapsed: std::time::Duration) {
    let tags = [tag("budget", budget.as_str())];
    let millis = elapsed.as_secs_f64() * 1000.0;
    counter(
        "spaced.guest.payload.cancelled_released_slots",
        slots as u64,
        &tags,
    );
    distribution("spaced.guest.payload.cancelled_hold_ms", millis, &tags);
    distribution(
        "spaced.guest.payload.cancelled_slot_ms",
        millis * slots as f64,
        &tags,
    );
}
