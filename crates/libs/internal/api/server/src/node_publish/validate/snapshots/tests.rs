use super::*;
use tonic::Code;

#[test]
fn snapshot_numeric_fields_keep_field_specific_rejections() {
    let cases = [
        (
            SnapshotDetail {
                vcpu: u32::MAX,
                ..SnapshotDetail::default()
            },
            "snapshot.vcpu",
            "i32",
        ),
        (
            SnapshotDetail {
                memory_mib: u32::MAX,
                ..SnapshotDetail::default()
            },
            "snapshot.memory_mib",
            "i32",
        ),
        (
            SnapshotDetail {
                size_bytes: Some(u64::MAX),
                ..SnapshotDetail::default()
            },
            "snapshot.size_bytes",
            "i64",
        ),
    ];
    let sandbox = "01990000-0000-7000-8000-000000000001".parse().unwrap();
    for (snapshot, field, integer) in cases {
        let error = snapshot_json(sandbox, &snapshot).unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            format!("{field} exceeds {integer}::MAX: number too large to fit in target type")
        );
    }
}

#[test]
fn unknown_snapshot_types_remain_explicit_unknown_observations() {
    let cases = [
        (SnapshotType::Disk as i32, "disk"),
        (SnapshotType::Full as i32, "full"),
        (SnapshotType::Unspecified as i32, "unknown"),
        (123, "unknown"),
    ];
    let sandbox = "01990000-0000-7000-8000-000000000001".parse().unwrap();
    for (kind, expected) in cases {
        let row = snapshot_json(
            sandbox,
            &SnapshotDetail {
                r#type: kind,
                ..SnapshotDetail::default()
            },
        )
        .unwrap();
        assert_eq!(row["type"], expected);
        assert!(row["size_bytes"].is_null());
        assert_eq!(row["locations"], serde_json::json!([]));
    }
}
