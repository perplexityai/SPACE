use super::*;

async fn recv_drain(rx: &mut mpsc::Receiver<WorkItem>) {
    match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
        Ok(Some(WorkItem::DrainClusterQueue)) => {}
        Ok(Some(other)) => panic!("unexpected work item: {other:?}"),
        Ok(None) => panic!("work queue closed"),
        Err(_) => panic!("timed out waiting for drain work item"),
    }
}

async fn assert_no_drain(rx: &mut mpsc::Receiver<WorkItem>) {
    match tokio::time::timeout(
        DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE + Duration::from_millis(125),
        rx.recv(),
    )
    .await
    {
        Err(_) => {}
        Ok(item) => panic!("unexpected drain work item: {item:?}"),
    }
}

#[tokio::test]
async fn reconcile_trigger_coalesces_pending_pokes() {
    let (tx, mut rx) = mpsc::channel(8);
    let trigger =
        ClusterReconcileTrigger::with_debounce(tx, DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE);

    trigger.trigger("first").await.unwrap();
    trigger.trigger("second").await.unwrap();

    recv_drain(&mut rx).await;
    let start = trigger.mark_started().await;
    assert_eq!(start.reason, "first,second");
    trigger.mark_finished(start.epoch, false).await;
    assert_no_drain(&mut rx).await;
}

#[tokio::test]
async fn reconcile_trigger_schedules_one_followup_for_pokes_during_run() {
    let (tx, mut rx) = mpsc::channel(8);
    let trigger =
        ClusterReconcileTrigger::with_debounce(tx, DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE);

    trigger.trigger("first").await.unwrap();
    recv_drain(&mut rx).await;
    let start = trigger.mark_started().await;

    trigger.trigger("second").await.unwrap();
    trigger.trigger("third").await.unwrap();
    trigger.mark_finished(start.epoch, false).await;

    recv_drain(&mut rx).await;
    let followup = trigger.mark_started().await;
    assert_eq!(followup.reason, "second,third");
    trigger.mark_finished(followup.epoch, false).await;
    assert_no_drain(&mut rx).await;
}

/// Repeated firewall rules wakeups during a drain collapse into its one
/// follow-up drain.
#[tokio::test]
async fn firewall_rules_pokes_during_run_schedule_one_followup() {
    let (tx, mut rx) = mpsc::channel(8);
    let trigger =
        ClusterReconcileTrigger::with_debounce(tx, DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE);
    let firewall_rules = || {
        trigger_request_reason(request(
            &[StateReconciliationReason::FirewallRulesChanged.into()],
            "firewall_rules_changed",
        ))
    };

    trigger.trigger(ReconcileReason::NodeUpdated).await.unwrap();
    recv_drain(&mut rx).await;
    let start = trigger.mark_started().await;

    trigger.trigger(firewall_rules()).await.unwrap();
    trigger.trigger(firewall_rules()).await.unwrap();
    trigger.mark_finished(start.epoch, false).await;

    recv_drain(&mut rx).await;
    let followup = trigger.mark_started().await;
    assert_eq!(followup.reason, "firewall_rules_changed");
    trigger.mark_finished(followup.epoch, false).await;
    assert_no_drain(&mut rx).await;
}

#[tokio::test]
async fn reconcile_trigger_coalesces_typed_reasons() {
    let (tx, mut rx) = mpsc::channel(8);
    let trigger =
        ClusterReconcileTrigger::with_debounce(tx, DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE);

    trigger
        .trigger(ReconcileReason::SandboxDesiredStateUpdated)
        .await
        .unwrap();
    trigger
        .trigger(ReconcileReason::TemplatePoolPolicyUpdated)
        .await
        .unwrap();
    trigger
        .trigger(trigger_request_reason(TriggerStateReconciliationRequest {
            reasons: vec![StateReconciliationReason::FirewallRulesChanged.into()],
            diagnostic_reason: "firewall_rules_changed".to_string(),
        }))
        .await
        .unwrap();

    recv_drain(&mut rx).await;
    let start = trigger.mark_started().await;
    assert_eq!(
        start.reason,
        "sandbox_desired_state_updated,template_pool_policy_updated,firewall_rules_changed"
    );
    trigger.mark_finished(start.epoch, false).await;
    assert_no_drain(&mut rx).await;
}

fn request(reasons: &[i32], diagnostic_reason: &str) -> TriggerStateReconciliationRequest {
    TriggerStateReconciliationRequest {
        reasons: reasons.to_vec(),
        diagnostic_reason: diagnostic_reason.to_string(),
    }
}

fn typed(reasons: impl IntoIterator<Item = ReconcileReason>) -> TriggerRequestReason {
    TriggerRequestReason {
        reasons: reasons.into_iter().collect(),
        diagnostic_reasons: BTreeSet::new(),
    }
}

fn diagnostic(reason: &str) -> TriggerRequestReason {
    TriggerRequestReason {
        reasons: BTreeSet::new(),
        diagnostic_reasons: BTreeSet::from([reason.to_string()]),
    }
}

/// Each reason decodes from the wire number of its proto value, whose name
/// is the reason's spelling; the cluster fanout's tests pin the numbers.
/// Unspecified and numbers this build does not know decode to nothing, so a
/// request naming only them still wakes the node, on its diagnostic text.
#[test]
fn every_wire_number_decodes_to_its_reason() {
    let mut numbers = BTreeSet::new();
    for &reason in ReconcileReason::ALL {
        let name = format!(
            "STATE_RECONCILIATION_REASON_{}",
            reason.as_str().to_ascii_uppercase()
        );
        let number = StateReconciliationReason::from_str_name(&name)
            .unwrap_or_else(|| panic!("no proto value {name}")) as i32;
        assert!(number != 0 && numbers.insert(number), "{name} = {number}");
        assert_eq!(
            trigger_request_reason(request(&[number], "diagnostic")),
            typed([reason]),
            "{number}"
        );
    }
    for number in [0, -1, 999, i32::MAX] {
        assert_eq!(
            trigger_request_reason(request(&[number], "diagnostic")),
            diagnostic("diagnostic"),
            "{number}"
        );
    }
}

/// Typed reasons win over the diagnostic text whenever any is known; the
/// text decides only when none is. Known reasons survive once each, beside
/// duplicates and numbers this build does not know.
#[test]
fn trigger_request_reason_keeps_known_reasons_and_falls_back_to_the_text() {
    use ReconcileReason::{FirewallRulesChanged as Firewall, NodeUpdated as Updated};

    let cases = [
        (request(&[18], "firewall_rules_changed"), typed([Firewall])),
        (
            request(&[18, 2], "firewall_rules_changed,node_updated"),
            typed([Firewall, Updated]),
        ),
        (
            request(&[18, 18, 2, 18], "firewall_rules_changed"),
            typed([Firewall, Updated]),
        ),
        (
            request(&[999, 18, 0, -1], "reason_from_a_newer_build"),
            typed([Firewall]),
        ),
        (request(&[2], "unrelated text"), typed([Updated])),
        // A legacy request, before typed reasons.
        (
            request(&[], "node_updated,manual_node_reconcile"),
            diagnostic("node_updated,manual_node_reconcile"),
        ),
        // Text naming the firewall rules reason is still only text: it wakes
        // the node like any diagnostic and decodes to no typed reason.
        (
            request(&[999], "firewall_rules_changed"),
            diagnostic("firewall_rules_changed"),
        ),
        (request(&[], ""), diagnostic("unspecified")),
    ];
    for (request, expected) in cases {
        let description = format!("{request:?}");
        assert_eq!(trigger_request_reason(request), expected, "{description}");
    }
}

#[test]
fn trigger_jitter_uses_seeded_offset() {
    let base = Duration::from_millis(150);
    let jitter = Duration::from_millis(500);
    assert_eq!(
        jittered_trigger_delay(base, jitter, 0),
        Duration::from_millis(150)
    );
    assert_eq!(
        jittered_trigger_delay(base, jitter, 499),
        Duration::from_millis(649)
    );
    assert_eq!(jittered_trigger_delay(base, Duration::ZERO, 499), base);
}
