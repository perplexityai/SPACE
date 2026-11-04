use pplx_space_common_core::types::RuntimeKind;

use super::*;

fn template_row(name: &str) -> TemplateRow {
    TemplateRow {
        name: name.to_string(),
        description: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn list_templates_query_defaults_and_caps_limit() {
    assert_eq!(
        ListTemplatesQuery {
            cursor: None,
            limit: None,
        }
        .limit(),
        DEFAULT_TEMPLATE_LIST_LIMIT
    );
    assert_eq!(
        ListTemplatesQuery {
            cursor: None,
            limit: Some(0),
        }
        .limit(),
        DEFAULT_TEMPLATE_LIST_LIMIT
    );
    assert_eq!(
        ListTemplatesQuery {
            cursor: None,
            limit: Some(MAX_TEMPLATE_LIST_LIMIT + 1),
        }
        .limit(),
        MAX_TEMPLATE_LIST_LIMIT
    );
}

#[test]
fn template_page_cursor_requires_overfetch_row() {
    let (rows, next_cursor) =
        template_page_rows(vec![template_row("base"), template_row("seed")], 2).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(next_cursor.is_none());

    let (rows, next_cursor) = template_page_rows(
        vec![
            template_row("base"),
            template_row("code-runtime"),
            template_row("seed"),
        ],
        2,
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        next_cursor
            .as_ref()
            .and_then(|cursor| decode_cursor(CursorKind::Template, cursor.as_str()).ok())
            .as_deref(),
        Some("code-runtime")
    );
}

#[test]
fn public_template_variant_maps_runtime_alias_and_bounds_sizes() {
    let variant = public_template_variant(TemplateVariantRow {
        template_name: "seed".to_string(),
        content_hash: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
            .to_string(),
        runtime: Some("fc".to_string()),
        arch: Some("aarch64".to_string()),
        built_at: Some("2026-05-13T00:00:00Z".to_string()),
        artifact_size_bytes: Some(42),
        has_base_snapshot: true,
        base_snapshot_size_bytes: Some(-1),
        snapshots: SqlJson(vec![PublicTemplateSnapshot {
            vcpu: 2,
            memory_mib: 1024,
            snapshot_hash: "snap".to_string(),
        }]),
        vcpu: Some(2),
        memory_mib: Some(1024),
        disk_size_mb: Some(4096),
    });

    assert_eq!(variant.runtime, Some(RuntimeKind::Firecracker));
    assert_eq!(variant.artifact_size_bytes, Some(42));
    assert_eq!(variant.base_snapshot_size_bytes, None);
    assert_eq!(variant.resources.disk_mib, Some(4096));
    assert_eq!(variant.snapshots.len(), 1);
}
