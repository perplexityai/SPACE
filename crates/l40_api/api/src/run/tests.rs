use std::time::Duration;

use super::{ADMIN_SERVICE_NAME, ApiRunConfig, ApiSurface, PUBLIC_SERVICE_NAME};

fn config(surface: ApiSurface) -> ApiRunConfig {
    ApiRunConfig {
        listen: "0.0.0.0:8080".to_string(),
        shutdown_grace: Duration::from_secs(super::DEFAULT_SHUTDOWN_GRACE_SECS),
        surface,
        database_url: "postgres://space_user:hunter2@postgres:5432/space".to_string(),
    }
}

// The shared host serves the public API in the same process as the admin API,
// so it reports the public service name; only the dedicated admin surface
// reports the admin one. Telemetry, dashboards, and the drain log all key off
// this, and the caller reads it to initialize telemetry before `run`.
#[test]
fn service_name_follows_the_served_surface() {
    assert_eq!(
        config(ApiSurface::Public).service_name(),
        PUBLIC_SERVICE_NAME
    );
    assert_eq!(
        config(ApiSurface::Shared).service_name(),
        PUBLIC_SERVICE_NAME
    );
    assert_eq!(config(ApiSurface::Admin).service_name(), ADMIN_SERVICE_NAME);
}

// Only the public-facing surfaces mint and verify public cursors, so the
// dedicated admin process must not require the cursor secret to start.
#[test]
fn only_public_facing_surfaces_require_the_cursor_secret() {
    assert!(ApiSurface::Public.requires_public_cursor_secret());
    assert!(ApiSurface::Shared.requires_public_cursor_secret());
    assert!(!ApiSurface::Admin.requires_public_cursor_secret());
}

// The connection string can carry a password, so a config dump must not leak
// it even though every other field is useful in a log.
#[test]
fn debug_output_redacts_the_database_url() {
    let rendered = format!("{:?}", config(ApiSurface::Public));
    assert!(!rendered.contains("hunter2"), "{rendered}");
    assert!(
        rendered.contains("database_url: \"<redacted>\""),
        "{rendered}"
    );
    assert!(rendered.contains("listen: \"0.0.0.0:8080\""), "{rendered}");
}
