use super::*;

#[tokio::test]
async fn send_source_census_keeps_all_unfinished_jobs_including_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("volume-manager.sqlite");
    crate::run::migrate_volume_manager_db(&path).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let mut expected = Vec::new();
    // Exceed the ordinary jobs-list page size: dropping the second page would
    // delete live inputs. Unknown kinds and terminal rows own no send leases.
    for index in 0..120 {
        for (status, retained) in [
            ("pending", true),
            ("running", true),
            ("cancelling", true),
            ("cancelled", false),
            ("failed", false),
            ("completed", false),
        ] {
            let key = format!("{status}-{index}");
            conn.execute(
                "INSERT INTO jobs (job_id,kind,status,payload) VALUES (?1,'push_btrfs_send',?2,?1)",
                params![key, status],
            )
            .unwrap();
            if retained {
                expected.push(key);
            }
        }
    }
    conn.execute(
        "INSERT INTO jobs (job_id,kind,payload) VALUES ('unrelated','push','not-a-lease')",
        [],
    )
    .unwrap();
    let mut actual = active_btrfs_upload_payloads(&conn).unwrap();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn historical_attachment_keys_remain_addressable_without_merging_aliases() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("volume-manager.sqlite");
    crate::run::migrate_volume_manager_db(&path).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let spellings = [
        "019d84bd-280d-7331-99c6-b63377f3a3a2",
        "019D84BD-280D-7331-99C6-B63377F3A3A2",
        "{019d84bd-280d-7331-99c6-b63377f3a3a2}",
        "urn:uuid:019d84bd-280d-7331-99c6-b63377f3a3a2",
        "019d84bd280d733199c6b63377f3a3a2",
    ];
    // The old writer stored spelling verbatim. Alias rows may disagree, so
    // normalizing the key would either hide a row or merge distinct attachments.
    for (index, raw) in spellings.iter().enumerate() {
        conn.execute(
            "INSERT INTO volume_attachments (sandbox_id, name, s3_bucket, s3_region)
             VALUES (?1, 'data', ?2, 'us-east-1')",
            params![raw, format!("bucket-{index}")],
        )
        .unwrap();
    }
    for (index, raw) in spellings.iter().enumerate() {
        let attachments = list_attachments(&conn, &raw.parse().unwrap()).unwrap();
        assert_eq!(attachments.len(), 1, "{raw}");
        assert_eq!(attachments[0].s3_bucket, format!("bucket-{index}"), "{raw}");
        let mut attachment = get_attachment(&conn, &raw.parse().unwrap(), "data")
            .unwrap()
            .unwrap();
        attachment.s3_bucket = format!("updated-{index}");
        upsert_attachment(&conn, &raw.parse().unwrap(), &attachment).unwrap();
        // Old binaries must see the same update after a rollback.
        let bucket: String = conn
            .query_row(
                "SELECT s3_bucket FROM volume_attachments WHERE sandbox_id = ?1 AND name = 'data'",
                [raw],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bucket, format!("updated-{index}"));
        delete_attachment(&conn, &raw.parse().unwrap(), "data").unwrap();
        assert!(
            list_attachments(&conn, &raw.parse().unwrap())
                .unwrap()
                .is_empty()
        );
        let remaining: i64 = conn
            .query_row("SELECT count(*) FROM volume_attachments", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining, (spellings.len() - index - 1) as i64);
        // A newly inserted key also remains readable by an old binary.
        upsert_attachment(&conn, &raw.parse().unwrap(), &attachment).unwrap();
        let stored: String = conn
            .query_row(
                "SELECT s3_bucket FROM volume_attachments WHERE sandbox_id = ?1 AND name = 'data'",
                [raw],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, attachment.s3_bucket);
        delete_attachment(&conn, &raw.parse().unwrap(), "data").unwrap();
        // Repeated detach remains idempotent.
        delete_attachment(&conn, &raw.parse().unwrap(), "data").unwrap();
    }
}

#[test]
fn attachment_keys_reject_invalid_uuid_spellings() {
    for raw in ["", "not-a-uuid", "sbx_019d84bd-280d-7331-99c6-b63377f3a3a2"] {
        assert!(raw.parse::<AttachmentSandboxKey>().is_err(), "{raw}");
    }
}
