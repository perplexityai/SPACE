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

    recv_drain(&mut rx).await;
    let start = trigger.mark_started().await;
    assert_eq!(
        start.reason,
        "sandbox_desired_state_updated,template_pool_policy_updated"
    );
    trigger.mark_finished(start.epoch, false).await;
    assert_no_drain(&mut rx).await;
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
