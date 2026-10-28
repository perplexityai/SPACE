use super::*;

#[test]
fn standard_schedule_has_exact_facet_mix() -> Result<()> {
    let node = Node {
        id: "synthetic-node".into(),
        sandboxes: vec![PublishedSandboxState::default(); 160],
        rare: PublishedSandboxState::default(),
        retire_after_rare: false,
    };
    let mut counts = [0; 4];
    for step in 0..200 {
        let publish = request(&node, step, 42)?;
        let kind = if publish.sandbox_inventory_complete {
            assert_eq!(publish.sandboxes.len(), 160);
            0
        } else if !publish.sandboxes.is_empty() {
            assert_eq!(publish.sandboxes.len(), 1);
            1
        } else if publish.node_status.is_some() {
            assert!(!publish.template_inventory_complete);
            2
        } else {
            assert!(publish.template_inventory_complete);
            assert_eq!(publish.templates.len(), 1);
            3
        };
        *counts.get_mut(kind).context("invalid publication kind")? += 1;
    }
    assert_eq!(counts, [10, 110, 60, 20]);
    Ok(())
}

#[test]
fn retired_claim_leaves_the_remaining_single_sandbox_publishable() -> Result<()> {
    let node = Node {
        id: "synthetic-node".into(),
        sandboxes: vec![PublishedSandboxState {
            sandbox_id: "remaining".into(),
            ..Default::default()
        }],
        rare: PublishedSandboxState::default(),
        retire_after_rare: true,
    };
    assert_eq!(request(&node, 2, 42)?.sandboxes, node.sandboxes);
    Ok(())
}

#[test]
fn suspended_artifact_is_not_made_stale_by_synthetic_activity() {
    let mut sandboxes = ["started", "suspending"].map(|status| PublishedSandboxState {
        status: Some(status.into()),
        ..Default::default()
    });
    let timestamp = prost_types::Timestamp {
        seconds: 10,
        nanos: 0,
    };
    advance_running_activity(&mut sandboxes, 1, timestamp);
    let [running, suspending] = sandboxes;
    assert_eq!(running.last_access_at, Some(timestamp));
    assert_eq!(suspending.last_access_at, None);
    assert_eq!(suspending.last_command_at, None);
    assert_eq!(suspending.uptime, None);
}
