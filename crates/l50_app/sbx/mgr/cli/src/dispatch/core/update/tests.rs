use super::*;
use clap::Parser;

#[test]
fn update_validation_requires_at_least_one_change() {
    let args = AutoLifecycleArgs::default();
    assert_eq!(
        validate_update_request(None, None, None, false, &args),
        Err(UpdateValidationError::NoUpdateRequested)
    );
    assert_eq!(
        validate_update_request(Some(2), None, None, false, &args),
        Ok(())
    );
}

#[test]
fn update_validation_rejects_inherit_plus_explicit_policy() {
    let args = explicit_auto_lifecycle_args();
    assert_eq!(
        validate_update_request(None, None, None, true, &args),
        Err(UpdateValidationError::InheritTemplateWithExplicitPolicy)
    );
}

#[test]
fn update_change_summary_formats_resource_and_lifecycle_changes() {
    assert_eq!(
        update_change_summary(
            Some(2),
            Some(2048),
            Some(256),
            false,
            Some("off".to_string())
        ),
        vec!["vcpu=2", "memory=2048MiB", "pids=256", "auto_lifecycle=off"]
    );
    assert_eq!(
        update_change_summary(None, None, None, true, None),
        vec!["auto_lifecycle=template-default"]
    );
}

fn explicit_auto_lifecycle_args() -> AutoLifecycleArgs {
    match Cli::parse_from(["space-sbxmgr", "update", "abcd", "--auto-lifecycle-off"]).command {
        Commands::Update { auto_lifecycle, .. } => auto_lifecycle,
        _ => unreachable!("test command should parse as update"),
    }
}
