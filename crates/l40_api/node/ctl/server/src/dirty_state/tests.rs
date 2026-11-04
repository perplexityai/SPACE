use super::*;

#[test]
fn full_scope_preserves_claimed_elements_for_clear() {
    let mut batch = DirtyFacetBatch::claimed("claim-1".to_string());
    batch.insert_generation(
        FACET_SANDBOX_INVENTORY.to_string(),
        Some("sandbox-a".to_string()),
        7,
    );

    batch.push_ref(FacetRef::new(FACET_SANDBOX_INVENTORY, None::<String>));

    let scope = batch
        .facets()
        .find(|(facet, _)| *facet == FACET_SANDBOX_INVENTORY)
        .map(|(_, scope)| scope);
    assert_eq!(scope, Some(None));

    let elements = batch
        .facets
        .get(FACET_SANDBOX_INVENTORY)
        .and_then(Option::as_ref)
        .expect("sandbox inventory scope should be tracked");
    assert_eq!(elements.get("sandbox-a"), Some(&7));
    assert_eq!(elements.get(ELEMENT_ALL), Some(&0));
}

#[tokio::test]
async fn mark_refs_revives_backed_off_dirty_row() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().join("node-ctl.db");

    pplx_space_sqlite::migrate("node-ctl-test", &db_path, &crate::NODE_CTL_MIGRATOR).await?;
    let conn = pplx_space_sqlite::open_rusqlite(
        &db_path,
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )?;
    let old_now = crate::now_secs().saturating_sub(10);
    let old_next_attempt = old_now.saturating_add(300);

    conn.execute(
        "INSERT INTO dirty_facets
             (facet, element, dirty, dirty_generation, attempts, last_error,
              updated_at, last_attempt_at, next_attempt_at, claim_token, claimed_at)
         VALUES (?1, ?2, 1, 7, 3, 'previous failure', ?3, ?3, ?4, 'claim-old', ?3)",
        params![FACET_TEMPLATE_INVENTORY, "seed", old_now, old_next_attempt],
    )?;

    drop(conn);
    pplx_space_sqlite::migrate("node-ctl-test", &db_path, &crate::NODE_CTL_MIGRATOR).await?;
    let mut conn = pplx_space_sqlite::open_rusqlite(
        &db_path,
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )?;

    let marked_at = crate::now_secs();
    mark_refs(
        &mut conn,
        &[FacetRef::new(FACET_TEMPLATE_INVENTORY, Some("seed"))],
    )?;

    let row = conn.query_row(
        "SELECT dirty, dirty_generation, attempts, last_error, next_attempt_at,
                claim_token, claimed_at, dead_at, dead_reason
         FROM dirty_facets
         WHERE facet = ?1 AND element = ?2",
        params![FACET_TEMPLATE_INVENTORY, "seed"],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, String>(8)?,
            ))
        },
    )?;

    assert_eq!(row.0, 1);
    assert_eq!(row.1, 8);
    assert_eq!(row.2, 0);
    assert_eq!(row.3, "");
    assert!(row.4 >= marked_at);
    assert!(row.4 < old_next_attempt);
    assert_eq!(row.5, "");
    assert_eq!(row.6, 0);
    assert_eq!(row.7, 0);
    assert_eq!(row.8, "");

    // The retired baseline's actual checksum distinguishes legacy history
    // from a public migration whose version alone was corrupted.
    conn.execute(
        "UPDATE _sqlx_migrations SET version = 4, description = 'baseline',
         checksum = X'622273a5ff539a8ecbbb30b42b3893dcfb168c11483e6ce3123e03c2b0ca0dec35eb9027f104aadee6445b5720ea7563'",
        [],
    )?;
    let history = conn.query_row(
        "SELECT version, checksum FROM _sqlx_migrations",
        [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )?;
    drop(conn);

    let error = pplx_space_sqlite::migrate("node-ctl-test", &db_path, &crate::NODE_CTL_MIGRATOR)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("db/README.md"));

    let conn = rusqlite::Connection::open(&db_path)?;
    assert_eq!(
        conn.query_row(
            "SELECT version, checksum FROM _sqlx_migrations",
            [],
            |row| { Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)) }
        )?,
        history
    );
    assert_eq!(
        conn.query_row(
            "SELECT dirty, dirty_generation FROM dirty_facets WHERE facet = ?1 AND element = ?2",
            params![FACET_TEMPLATE_INVENTORY, "seed"],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?,
        (1, 8)
    );

    Ok(())
}

#[test]
fn runtime_provisioning_refs_are_whole_node_and_round_trip() {
    let reference = FacetRef::new(FACET_RUNTIME_PROVISIONING, None::<String>);
    assert_eq!(
        FacetRef::from_proto(reference.to_proto().unwrap()).unwrap(),
        reference
    );
    let element = FacetRef::new(FACET_RUNTIME_PROVISIONING, Some("e0"));
    element.to_proto().unwrap_err();
    let wire = ProtoFacetRef {
        facet: ProtoReconcileFacet::RuntimeProvisioning as i32,
        element: Some(facet_ref::Element::Template("e0".into())),
    };
    FacetRef::from_proto(wire).unwrap_err();
}
