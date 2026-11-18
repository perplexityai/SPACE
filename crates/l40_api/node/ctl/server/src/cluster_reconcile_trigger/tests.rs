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

/// Pokes that arrive while a drain runs schedule nothing until it finishes,
/// then one follow-up drain between them, which carries them, and nothing
/// after it. Repeated firewall rules wakeups collapse into that one drain. A
/// poke only ever yields a queue drain: `recv_drain` and `assert_no_drain`
/// fail on any other work item.
#[tokio::test]
async fn reconcile_trigger_schedules_one_followup_for_pokes_during_run() {
    let firewall_rules = || {
        trigger_request_reason(TriggerStateReconciliationRequest {
            reasons: vec![StateReconciliationReason::FirewallRulesChanged.into()],
            diagnostic_reason: "firewall_rules_changed".to_string(),
        })
    };
    let cases: [(TriggerRequestReason, [TriggerRequestReason; 2], &str); 2] = [
        (
            "first".into(),
            ["second".into(), "third".into()],
            "second,third",
        ),
        (
            ReconcileReason::NodeUpdated.into(),
            [firewall_rules(), firewall_rules()],
            "firewall_rules_changed",
        ),
    ];
    for (first, during_run, followup_reason) in cases {
        let (tx, mut rx) = mpsc::channel(8);
        let trigger =
            ClusterReconcileTrigger::with_debounce(tx, DEFAULT_STATE_RECONCILE_TRIGGER_DEBOUNCE);

        trigger.trigger(first).await.unwrap();
        recv_drain(&mut rx).await;
        let start = trigger.mark_started().await;

        for poke in during_run {
            trigger.trigger(poke).await.unwrap();
        }
        assert_no_drain(&mut rx).await;
        trigger.mark_finished(start.epoch, false).await;

        recv_drain(&mut rx).await;
        let followup = trigger.mark_started().await;
        assert_eq!(followup.reason, followup_reason);
        trigger.mark_finished(followup.epoch, false).await;
        assert_no_drain(&mut rx).await;
    }
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

/// Each wire number decodes to its reason. Unspecified and numbers this build
/// does not know decode to nothing, so a request naming only them still
/// wakes the node, on its diagnostic text.
#[test]
fn every_wire_number_decodes_to_its_reason() {
    let reasons = [
        (1, ReconcileReason::NodeRegistered),
        (2, ReconcileReason::NodeUpdated),
        (3, ReconcileReason::ManualNodeStatusRefresh),
        (4, ReconcileReason::ManualNodeReconcile),
        (5, ReconcileReason::ManualNodeNetworkPolicyReconcile),
        (6, ReconcileReason::NodeTemplatePulled),
        (7, ReconcileReason::SandboxRegistered),
        (8, ReconcileReason::SandboxPortExposureUpdated),
        (9, ReconcileReason::SandboxNetworkPolicyUpdated),
        (10, ReconcileReason::SandboxDesiredStateUpdated),
        (11, ReconcileReason::TemplateNetworkPolicyUpdated),
        (12, ReconcileReason::TemplateDesiredStateUpdated),
        (13, ReconcileReason::TemplateBaseSnapshotPolicyUpdated),
        (14, ReconcileReason::TemplatePoolPolicyUpdated),
        (15, ReconcileReason::TemplatePoolPolicyDeleted),
        (16, ReconcileReason::NodeSandboxInventoryPublished),
        (17, ReconcileReason::NodeTemplateInventoryPublished),
        (18, ReconcileReason::FirewallRulesChanged),
    ];
    // The table runs from 1 without gaps, so this fails when a number is
    // added without a row above.
    StateReconciliationReason::try_from(reasons.len() as i32 + 1).unwrap_err();
    for (number, reason) in reasons {
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
