use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

fn guest(os: &str, arch: &str) -> RuntimeGuest {
    RuntimeGuest {
        guest_os: os.to_owned(),
        guest_arch: arch.to_owned(),
    }
}

fn state_format(version: &str, can_create: bool, can_load: bool) -> RuntimeStateFormat {
    RuntimeStateFormat {
        state_format_name: "firecracker-snapshot".to_owned(),
        state_format_version: version.to_owned(),
        can_create,
        can_load,
    }
}

fn runtime(
    version: &str,
    guests: Vec<RuntimeGuest>,
    state_formats: Vec<RuntimeStateFormat>,
) -> RuntimeInstallation {
    RuntimeInstallation {
        runtime: "firecracker".to_owned(),
        runtime_version: version.to_owned(),
        guests,
        state_formats,
    }
}

fn component(kind: &str, version: Option<&str>, default_for_new: bool) -> ArtifactComponent {
    ArtifactComponent {
        kind: kind.to_owned(),
        role_key: String::new(),
        version_key: version.unwrap_or_default().to_owned(),
        digest_key: String::new(),
        format_key: String::new(),
        version: version.map(ToOwned::to_owned),
        digest: None,
        format: None,
        default_for_new,
        legacy_default: false,
    }
}

/// Replace the node's inventory in one transaction, in a publication's order.
async fn replace(
    pool: &PgPool,
    node_id: NodeId,
    runtimes: &[RuntimeInstallation],
    components: &[ArtifactComponent],
) -> TestResult {
    let mut tx = pool.begin().await?;
    prune_runtime_installations(&mut *tx, node_id, runtimes).await?;
    prune_artifact_components(&mut *tx, node_id, components).await?;
    insert_runtime_installations(&mut *tx, node_id, runtimes).await?;
    prune_runtime_guests(&mut *tx, node_id, runtimes).await?;
    insert_runtime_guests(&mut *tx, node_id, runtimes).await?;
    prune_runtime_state_formats(&mut *tx, node_id, runtimes).await?;
    upsert_runtime_state_formats(&mut *tx, node_id, runtimes).await?;
    upsert_artifact_components(&mut *tx, node_id, components).await?;
    tx.commit().await?;
    Ok(())
}

type Rows = (
    Vec<String>,
    Vec<(String, String, String)>,
    Vec<(String, String, bool, bool)>,
    Vec<(String, Option<String>)>,
);

async fn rows(pool: &PgPool, node_id: NodeId) -> Result<Rows, sqlx::Error> {
    let installations = sqlx::query_scalar(sql!(
        "SELECT runtime_version FROM node_runtime_installations
         WHERE node_id = $1 ORDER BY runtime_version"
    ))
    .bind(node_id)
    .fetch_all(pool)
    .await?;
    let guests = sqlx::query_as(sql!(
        "SELECT runtime_version, guest_os, guest_arch FROM node_runtime_guests
         WHERE node_id = $1 ORDER BY runtime_version, guest_os, guest_arch"
    ))
    .bind(node_id)
    .fetch_all(pool)
    .await?;
    let state_formats = sqlx::query_as(sql!(
        "SELECT runtime_version, state_format_version, can_create, can_load
         FROM node_runtime_state_formats
         WHERE node_id = $1 ORDER BY runtime_version, state_format_version"
    ))
    .bind(node_id)
    .fetch_all(pool)
    .await?;
    let components = sqlx::query_as(sql!(
        "SELECT kind, version FROM node_artifact_components
         WHERE node_id = $1 ORDER BY kind"
    ))
    .bind(node_id)
    .fetch_all(pool)
    .await?;
    Ok((installations, guests, state_formats, components))
}

#[tokio::test]
async fn a_replacement_removes_what_the_report_no_longer_names() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let other = NodeId::new();
    insert_ready_worker_node(&db.pool, other).await?;

    let first = [
        runtime(
            "1.12.0",
            vec![guest("linux", "x86_64"), guest("linux", "aarch64")],
            // A format reported twice takes each capability either report grants.
            vec![
                state_format("4.0.0", true, false),
                state_format("4.0.0", false, true),
                state_format("4.1.0", true, true),
            ],
        ),
        runtime(
            "1.13.0",
            vec![guest("linux", "x86_64")],
            vec![state_format("5.0.0", true, true)],
        ),
    ];
    let components = [
        component("kernel", Some("6.1.155"), true),
        component("rootfs", None, false),
    ];
    replace(&db.pool, node_id, &first, &components).await?;
    replace(&db.pool, other, &first, &components).await?;
    let (installations, guests, state_formats, stored_components) = rows(&db.pool, node_id).await?;
    assert_eq!(installations, ["1.12.0", "1.13.0"]);
    assert_eq!(guests.len(), 3);
    assert_eq!(
        state_formats,
        [
            ("1.12.0".to_owned(), "4.0.0".to_owned(), true, true),
            ("1.12.0".to_owned(), "4.1.0".to_owned(), true, true),
            ("1.13.0".to_owned(), "5.0.0".to_owned(), true, true),
        ]
    );
    assert_eq!(stored_components.len(), 2);

    // The retained runtime loses a guest, a state format and a capability;
    // the other runtime and the rootfs go.
    let second = [runtime(
        "1.12.0",
        vec![guest("linux", "x86_64")],
        vec![state_format("4.0.0", false, true)],
    )];
    replace(&db.pool, node_id, &second, &components[..1]).await?;
    let (installations, guests, state_formats, stored_components) = rows(&db.pool, node_id).await?;
    assert_eq!(installations, ["1.12.0"]);
    assert_eq!(
        guests,
        [("1.12.0".to_owned(), "linux".to_owned(), "x86_64".to_owned())]
    );
    assert_eq!(
        state_formats,
        [("1.12.0".to_owned(), "4.0.0".to_owned(), false, true)]
    );
    assert_eq!(
        stored_components
            .iter()
            .map(|(kind, version)| (kind.as_str(), version.as_deref()))
            .collect::<Vec<_>>(),
        [("kernel", Some("6.1.155"))]
    );

    // Another node's inventory is its own.
    let (installations, guests, state_formats, stored_components) = rows(&db.pool, other).await?;
    assert_eq!(installations.len(), 2);
    assert_eq!(guests.len(), 3);
    assert_eq!(state_formats.len(), 3);
    assert_eq!(stored_components.len(), 2);
    Ok(())
}

#[tokio::test]
async fn installations_list_one_row_per_guest_for_the_requested_nodes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let other = NodeId::new();
    insert_ready_worker_node(&db.pool, other).await?;
    replace(
        &db.pool,
        node_id,
        &[
            runtime(
                "1.13.0",
                vec![guest("linux", "x86_64"), guest("linux", "aarch64")],
                Vec::new(),
            ),
            runtime("1.12.0", Vec::new(), Vec::new()),
        ],
        &[],
    )
    .await?;
    replace(
        &db.pool,
        other,
        &[runtime(
            "1.12.0",
            vec![guest("linux", "x86_64")],
            Vec::new(),
        )],
        &[],
    )
    .await?;

    let listed = list_installations_with_guests(&db.pool, &[node_id]).await?;
    assert_eq!(
        listed
            .iter()
            .map(|row| (
                row.node_id,
                row.runtime_version.as_str(),
                row.guest_os.as_deref(),
                row.guest_arch.as_deref(),
            ))
            .collect::<Vec<_>>(),
        [
            // An installation without guests is listed once, without one.
            (node_id, "1.12.0", None, None),
            (node_id, "1.13.0", Some("linux"), Some("aarch64")),
            (node_id, "1.13.0", Some("linux"), Some("x86_64")),
        ]
    );
    assert!(listed.iter().all(|row| row.runtime == "firecracker"));
    assert_eq!(
        list_installations_with_guests(&db.pool, &[node_id, other])
            .await?
            .len(),
        4
    );
    assert!(
        list_installations_with_guests(&db.pool, &[NodeId::new()])
            .await?
            .is_empty()
    );
    Ok(())
}
