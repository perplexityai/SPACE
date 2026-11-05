//! Which creates record a requested-size create sample: a cold create at its
//! cluster admission decision and a warm-pool claim when it is adopted (the
//! claim tests in the parent assert those). Only the Kubernetes substrate
//! resolves a cold create's shape, so the cold cases need its feature.

use pplx_space_metrics::test_capture::ValueCaptureGuard;

const MIB: f64 = 1_048_576.0;

/// `(name, value, sorted tags)` of every requested-size sample captured.
pub(super) fn requested_samples(capture: ValueCaptureGuard) -> Vec<(String, f64, Vec<String>)> {
    capture
        .finish()
        .into_iter()
        .filter(|value| value.name.starts_with("sandbox.api.requested."))
        .map(|value| {
            let mut tags = value.tags;
            tags.sort_unstable();
            (value.name, value.value, tags)
        })
        .collect()
}

pub(super) fn create_samples(
    admission: &str,
    template: &str,
    defaulted: bool,
    vcpu: u32,
    memory_mib: u32,
) -> Vec<(String, f64, Vec<String>)> {
    let tags = vec![
        format!("admission:{admission}"),
        "client_id:unknown".to_owned(),
        format!("defaulted:{defaulted}"),
        "kind:create".to_owned(),
        "surface:native".to_owned(),
        format!("template:{template}"),
    ];
    vec![
        (
            "sandbox.api.requested.vcpu".to_owned(),
            f64::from(vcpu),
            tags.clone(),
        ),
        (
            "sandbox.api.requested.memory_bytes".to_owned(),
            f64::from(memory_mib) * MIB,
            tags,
        ),
    ]
}

#[cfg(feature = "kubernetes")]
mod cold {
    use pplx_space_metrics::test_capture::capture_values_guard;

    use super::super::*;
    use super::{create_samples, requested_samples};
    use crate::controllers::substrate::Substrate;
    use crate::controllers::substrate::kubernetes::tests::substrate_with_store;

    /// A pool config gives this template's fallthrough creates their shape.
    const POOLED: &str = "requested-pooled";

    struct Fixture {
        db: pplx_space_db_test_fixtures::TestDb,
        actor: User,
    }

    impl Fixture {
        async fn new() -> TestResult<Self> {
            let db = pg_test_db().await?;
            let actor = insert_actor(&db.pool).await?;
            let pooled =
                insert_template_release(&db.pool, POOLED, "sha256:requested-pooled").await?;
            insert_template_release_tag(&db.pool, pooled, POOLED, "latest").await?;
            sqlx::query(sql!(
                "INSERT INTO kubernetes_pool_configs (
                     id, template_release_id, vcpu, memory_mib, target_ready_slots,
                     refill_per_tick, max_warming_slots, namespace, enabled
                 )
                 VALUES ($1, $2, 2, 1024, 1, 1, 1, 'space', true)"
            ))
            .bind(Uuid::now_v7())
            .bind(pooled)
            .execute(&db.pool)
            .await?;
            // An admitted create stops at its insert, right after admission,
            // so it never reaches the Kubernetes API.
            sqlx::query(sql!(
                "CREATE FUNCTION refuse_requested_metrics_insert() RETURNS trigger
                 LANGUAGE plpgsql AS $$
                 BEGIN
                     RAISE EXCEPTION 'insert refused after admission';
                 END
                 $$"
            ))
            .execute(&db.pool)
            .await?;
            sqlx::query(sql!(
                "CREATE TRIGGER refuse_requested_metrics_insert BEFORE INSERT ON sandboxes
                 FOR EACH ROW EXECUTE FUNCTION refuse_requested_metrics_insert()"
            ))
            .execute(&db.pool)
            .await?;
            Ok(Self { db, actor })
        }

        fn substrate(&self) -> Substrate {
            Substrate::from_kubernetes(substrate_with_store(self.db.pool.clone()))
        }

        fn admitting(&self) -> SandboxController {
            SandboxController::new(self.substrate())
        }

        /// Admission over an empty inventory refuses every activation.
        fn refusing(&self) -> SandboxController {
            let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
                crate::test_support::direct_substrate(&self.db.pool),
                crate::cluster_admission::ClusterSandboxAdmissionConfig {
                    enabled: true,
                    refill_rps: 6.0,
                    burst: 60.0,
                    redis_url: None,
                },
                pplx_space_feature_flags::FeatureFlagClient::disabled(),
            );
            SandboxController::with_cluster_admission(self.substrate(), admission)
        }

        fn request(template: &str, size: Option<(u32, u32)>) -> CreateSandboxRequest {
            let mut req = pooled_create_request("requested-size", None);
            req.template = Some(format!("{template}:latest").parse().unwrap());
            req.boot_mode = None;
            req.vcpu = size.map(|(vcpu, _)| vcpu);
            req.memory_mib = size.map(|(_, memory_mib)| memory_mib);
            req
        }

        async fn create(
            &self,
            controller: &SandboxController,
            req: &CreateSandboxRequest,
        ) -> (ControllerError, Vec<(String, f64, Vec<String>)>) {
            let capture = capture_values_guard();
            let error = controller
                .create(
                    &self.db.pool,
                    &self.actor,
                    req,
                    SandboxApiMetricIdentity::native("create"),
                    FederatedSandboxCreateContext::new(None, None),
                )
                .await
                .expect_err("the fixture refuses every create");
            (error, requested_samples(capture))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn admitted_create_records_the_requested_size() -> TestResult {
        let fixture = Fixture::new().await?;

        let (error, samples) = fixture
            .create(
                &fixture.admitting(),
                &Fixture::request(POOLED, Some((4, 2048))),
            )
            .await;

        assert!(
            error.to_string().contains("insert refused after admission"),
            "{error}"
        );
        assert_eq!(samples, create_samples("accepted", POOLED, false, 4, 2048));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejected_create_records_the_resolved_size() -> TestResult {
        let fixture = Fixture::new().await?;

        let (error, samples) = fixture
            .create(&fixture.refusing(), &Fixture::request(POOLED, None))
            .await;

        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS, "{error}");
        assert_eq!(samples, create_samples("rejected", POOLED, true, 2, 1024));
        Ok(())
    }

    /// The pool config would give a fallthrough fork its shape too, but a
    /// fork's size comes from its source snapshot.
    #[tokio::test(flavor = "current_thread")]
    async fn rejected_fork_records_nothing() -> TestResult {
        let fixture = Fixture::new().await?;
        let egress = crate::egress_profiles::EgressProfileSelection::default();
        let capture = capture_values_guard();

        let error = fixture
            .refusing()
            .restore_fork_disk_as_new(
                &fixture.db.pool,
                &fixture.actor,
                &Fixture::request(POOLED, None),
                SandboxApiMetricIdentity::native("fork"),
                RestoreForkDiskAsNewContext {
                    volume_encryption: false,
                    sandbox_id: SandboxId::new(),
                    federated: FederatedSandboxCreateContext::new(None, None)
                        .with_egress_profiles(Some(&egress)),
                    operation_id: pplx_space_public_api::OperationId::new(),
                    restore_lease_seconds: 300,
                    snapshot: crate::controllers::substrate::SnapshotRef {
                        snapshot_id: "snapshot-1",
                        storage_uri: Some("s3://bucket/snapshot-1"),
                        source_sandbox_id: SandboxId::new(),
                    },
                },
            )
            .await
            .expect_err("the empty cluster admits no fork");

        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS, "{error}");
        assert_eq!(requested_samples(capture), []);
        Ok(())
    }
}
