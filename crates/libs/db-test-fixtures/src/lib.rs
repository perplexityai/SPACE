use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SnapshotId};

use std::error::Error;
use std::ffi::OsStr;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Weak};
use std::thread;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

const DEFAULT_POSTGRES_IMAGE: &str = "public.ecr.aws/docker/library/postgres:16-alpine";
/// Default Redis test image. Mirrored on AWS's public ECR for the same reason
/// as Postgres above: anonymous Docker Hub pulls are rate-limited per source
/// IP, so a shared CI agent exhausts the quota and every container-backed test
/// on the whole pipeline starts failing at once, whatever the change under
/// test. Public ECR needs no credentials and imposes no such limit.
const DEFAULT_REDIS_IMAGE: &str = "public.ecr.aws/docker/library/redis:5.0";
const SHARED_POSTGRES_ADMIN_URL_ENV: &str = "SPACE_TEST_POSTGRES_ADMIN_URL";
const TEMPLATE_DATABASE: &str = "space_template";

/// The `root@local` user row seeded by the PostgreSQL migrations into
/// every migrated test database, in org [`SEED_ORG_ID`] with role
/// `organization_admin`.
pub const SEED_USER_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0010);

/// The `Local` org row seeded by the PostgreSQL migrations into every
/// migrated test database.
pub const SEED_ORG_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0001);

static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../db/postgres");
static SHARED_POSTGRES: OnceCell<Mutex<Weak<SharedPostgres>>> = OnceCell::const_new();

pub type TestDbResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Split an image reference into the `(name, tag)` pair
/// [`testcontainers::ImageExt`] wants, taking `env_var` as an override of
/// `default`.
///
/// Both defaults point at AWS's public ECR mirrors of the official images
/// rather than Docker Hub, so container-backed tests do not depend on an
/// anonymous Docker Hub quota shared across every job on a CI agent.
pub fn test_image_ref(env_var: &str, default: &str) -> TestDbResult<(String, String)> {
    let image = std::env::var(env_var).unwrap_or_else(|_| default.to_string());
    let (name, tag) = image
        .rsplit_once(':')
        .ok_or_else(|| format!("{env_var} must include a tag: {image}"))?;
    Ok((name.to_string(), tag.to_string()))
}

/// The Redis image container-backed tests should start, honoring
/// `TESTCONTAINERS_REDIS_IMAGE`.
pub fn redis_image_ref() -> TestDbResult<(String, String)> {
    test_image_ref("TESTCONTAINERS_REDIS_IMAGE", DEFAULT_REDIS_IMAGE)
}

pub struct TestDb {
    pub pool: PgPool,
    pub database_url: String,
    _cleanup: DatabaseCleanup,
    _shared_postgres: Arc<SharedPostgres>,
}

struct DatabaseCleanup {
    database_name: String,
    admin_database_url: String,
}

impl Drop for DatabaseCleanup {
    fn drop(&mut self) {
        let database_name = self.database_name.clone();
        let admin_database_url = self.admin_database_url.clone();

        let cleanup = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    FixtureError::new(format!("failed to build cleanup runtime: {error}"))
                })?;

            runtime.block_on(async move {
                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&admin_database_url)
                    .await?;
                let drop_database = format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    quote_ident(&database_name)
                );
                sqlx::query(sqlx::AssertSqlSafe(drop_database.as_str()))
                    .execute(&admin_pool)
                    .await?;
                admin_pool.close().await;
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            })
        });

        match cleanup.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("failed to clean up postgres test database: {error}"),
            Err(_) => {
                eprintln!("failed to clean up postgres test database: cleanup thread panicked")
            }
        }
    }
}

/// A real `sqlx::Error` carrying `sqlstate`.
///
/// `PgDatabaseError` cannot be constructed outside `sqlx`, so a test that wants
/// to exercise SQLSTATE classification has to get the error from the server.
/// `RAISE EXCEPTION USING ERRCODE` produces the same error shape a genuinely
/// contended or rejected statement produces, down to the `code()` those
/// classifiers read — which a hand-rolled stand-in would not.
///
/// Shared because the alternative is this exact `DO $$ ... $$` block copied per
/// suite; a change to the injection shape should not need synchronized edits
/// across crates.
///
/// Panics if the statement unexpectedly succeeds — that means the injection
/// itself broke, and every assertion downstream of it would be meaningless.
pub async fn raise_sqlstate(pool: &PgPool, sqlstate: &str) -> sqlx::Error {
    // Interpolated into a SQL literal, and this is `pub` in a shared crate, so
    // constrain the input to the shape a SQLSTATE actually has rather than
    // trusting every future caller. A `'` would otherwise close the literal.
    assert!(
        sqlstate.len() == 5 && sqlstate.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "a SQLSTATE is five alphanumeric characters, got {sqlstate:?}"
    );
    // No `sql!` locator: this crate has `pplx-space-common-core` only as a
    // dev-dependency, which is also why its other runtime-built statements go
    // unwrapped. The locator exists to attribute production SQL in
    // `pg_stat_activity`; nothing here runs outside a test.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '{sqlstate}', \
         MESSAGE = 'injected {sqlstate} for test'; END $$"
    )))
    .execute(pool)
    .await
    .expect_err("RAISE EXCEPTION must fail the statement")
}

/// Wait until another session of this test database waits on a heavyweight
/// lock, failing after five seconds.
///
/// Concurrency tests use it to prove an operation is blocked behind a lock the
/// test holds. The pool needs a connection beyond the holder and the waiter.
pub async fn wait_for_lock_waiter(pool: &PgPool) -> TestDbResult<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND pid <> pg_backend_pid()
                       AND wait_event_type = 'Lock')",
            )
            .fetch_one(pool)
            .await?;
            if blocked {
                return Ok::<(), sqlx::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| FixtureError::boxed("no session waited on a lock within five seconds"))??;
    Ok(())
}

/// Insert a `ready`, `k8s`-managed worker node row at `127.0.0.1` with the
/// given id.
///
/// This is the fixed shape re-declared verbatim across several suites that
/// only need a node row to satisfy a foreign key or placement lookup; suites
/// that vary the address, role, or state still build their own `INSERT`.
pub async fn insert_ready_worker_node(pool: &PgPool, node_id: NodeId) -> TestDbResult<()> {
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'k8s', 'ready')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Pin `local_artifact_id` as the sandbox's current local artifact with kind
/// `disk` / origin `stop`, and register a `ready` `disk` snapshot for that
/// exact identity with a usable storage location at `storage_uri`.
///
/// This is the catalog state a stopped sandbox's evacuated rootfs leaves
/// behind: `BeginArtifactBackup(Disk)` + a `READY` `FinalizeArtifactBackup`
/// register the remote copy. The pin is written here directly:
/// `publish_stopped_artifact` mints it only in the node's own SQLite store,
/// and no Postgres writer sets a `stop` pin yet. Suites that need to vary the snapshot state, timestamp, or
/// artifact identity independently of the pin build their own `INSERT`s;
/// this covers the "the artifact is durable" shape they all otherwise repeat.
pub async fn publish_ready_disk_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    storage_uri: &str,
) -> TestDbResult<()> {
    sqlx::query(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .bind(local_artifact_id)
    .execute(pool)
    .await?;
    let snapshot_id = SnapshotId::new();
    sqlx::query(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, $4, 'disk', 'stop')",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .bind(local_artifact_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)",
    )
    .bind(snapshot_id)
    .bind(storage_uri)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a warm-pool slot receipt in the given state (`bound`, `claimed`,
/// `cleanup_pending`, ...) reserving `reserved_sandbox_id`, seeding the
/// template, template-release, and pool-config rows its foreign keys need.
/// Every seeded identifier derives from the reserved sandbox id, so multiple
/// receipts coexist in one database. `claimed_at` is backdated ten minutes so
/// grace-guarded reapers treat the receipt as settled.
///
/// This is the fixture for suites asserting that lifecycle transitions retire
/// (or preserve) the receipt; suites exercising the pool state machine itself
/// build slots through `WarmPoolStore`.
pub async fn insert_pool_slot_receipt(
    pool: &PgPool,
    reserved_sandbox_id: SandboxId,
    state: &str,
) -> TestDbResult<()> {
    let tag = reserved_sandbox_id.as_uuid().simple().to_string();
    let template_name = format!("pool-receipt-{tag}");
    let template_release_id = Uuid::now_v7();
    let config_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO templates (id, name, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')",
    )
    .bind(Uuid::now_v7())
    .bind(&template_name)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, $2, 'fc', $3)",
    )
    .bind(template_release_id)
    .bind(&template_name)
    .bind(format!("sha256:{tag}"))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace
         )
         VALUES ($1, $2, 2, 1024, 0, 0, 0, 'sandboxes')",
    )
    .bind(config_id)
    .bind(template_release_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO kubernetes_pool_slots (
             id, config_id, template_release_id, reserved_sandbox_id,
             template_name, content_hash, vcpu, memory_mib,
             pod_name, pod_namespace, state, claim_id, claimed_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, 2, 1024,
                 $7, 'sandboxes', $8, $9, NOW() - INTERVAL '10 minutes')",
    )
    .bind(Uuid::now_v7())
    .bind(config_id)
    .bind(template_release_id)
    .bind(reserved_sandbox_id)
    .bind(&template_name)
    .bind(format!("sha256:{tag}"))
    .bind(format!("space-sbx-receipt-{tag}"))
    .bind(state)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

/// `SELECT state FROM kubernetes_pool_slots` for the given reservation:
/// `None` means the receipt is gone. Companion probe for
/// [`insert_pool_slot_receipt`].
pub async fn pool_slot_receipt_state(
    pool: &PgPool,
    reserved_sandbox_id: SandboxId,
) -> TestDbResult<Option<String>> {
    Ok(
        sqlx::query_scalar(
            "SELECT state FROM kubernetes_pool_slots WHERE reserved_sandbox_id = $1",
        )
        .bind(reserved_sandbox_id)
        .fetch_optional(pool)
        .await?,
    )
}

/// Return a migrated Postgres database cloned from the shared template.
///
/// This is the hard-fail API: Docker/container startup, template creation, and
/// per-test cloning errors are returned so tests can fail with `?`.
pub async fn pg_test_db() -> TestDbResult<TestDb> {
    pg_test_db_with_max_connections(2).await
}

/// Return a migrated Postgres database cloned from the process-local template,
/// using the requested pool size for the per-test database connection pool.
pub async fn pg_test_db_with_max_connections(max_connections: u32) -> TestDbResult<TestDb> {
    let Some(shared) = shared_postgres(false).await? else {
        return Err(FixtureError::boxed("postgres testcontainer is unavailable"));
    };

    shared.cloned_database(max_connections).await
}

/// Return a migrated Postgres database cloned from the shared template.
///
/// This is the graceful API for suites that historically no-op when Docker is
/// unavailable. Only container startup failures map to `Ok(None)`; migration
/// and cloning failures still fail the test because they indicate broken setup.
pub async fn try_pg_test_db() -> TestDbResult<Option<TestDb>> {
    try_pg_test_db_with_max_connections(2).await
}

/// Return a migrated Postgres database cloned from the process-local template,
/// preserving graceful container-unavailable semantics and using the requested
/// pool size for the per-test database connection pool.
pub async fn try_pg_test_db_with_max_connections(
    max_connections: u32,
) -> TestDbResult<Option<TestDb>> {
    let Some(shared) = shared_postgres(true).await? else {
        return Ok(None);
    };

    shared.cloned_database(max_connections).await.map(Some)
}

/// Return a **raw, unmigrated** database on the shared run-scoped Postgres.
///
/// Unlike [`pg_test_db`], the returned database is not cloned from the
/// migrated [`TEMPLATE_DATABASE`] template — it is Postgres's own blank
/// default template, with no schema and no `_sqlx_migrations` history. Use
/// this for tests that engineer a specific migration-table state by hand (or
/// run the migrator themselves) and would otherwise collide with a database
/// that already has every migration applied.
///
/// This is the hard-fail API: Docker/container startup and per-test creation
/// errors are returned so tests can fail with `?`.
pub async fn raw_pg_test_db() -> TestDbResult<TestDb> {
    raw_pg_test_db_with_max_connections(2).await
}

/// Like [`raw_pg_test_db`], with the requested pool size for the per-test
/// connection pool.
pub async fn raw_pg_test_db_with_max_connections(max_connections: u32) -> TestDbResult<TestDb> {
    let Some(shared) = shared_postgres(false).await? else {
        return Err(FixtureError::boxed("postgres testcontainer is unavailable"));
    };

    shared.cloned_raw_database(max_connections).await
}

/// Return a raw, unmigrated database on the shared run-scoped Postgres.
///
/// This is the graceful API for suites that historically no-op when Docker is
/// unavailable. Only container startup failures map to `Ok(None)`; database
/// creation failures still fail the test because they indicate broken setup.
pub async fn try_raw_pg_test_db() -> TestDbResult<Option<TestDb>> {
    try_raw_pg_test_db_with_max_connections(2).await
}

/// Like [`try_raw_pg_test_db`], with the requested pool size for the per-test
/// connection pool.
pub async fn try_raw_pg_test_db_with_max_connections(
    max_connections: u32,
) -> TestDbResult<Option<TestDb>> {
    let Some(shared) = shared_postgres(true).await? else {
        return Ok(None);
    };

    shared.cloned_raw_database(max_connections).await.map(Some)
}

/// True when the suite must hard-fail instead of skipping when a test
/// dependency (Postgres/Redis testcontainer) is unavailable.
///
/// Runtime environment check rather than a compile-time cfg so CI can flip
/// strictness per pipeline step without changing compile flags (which would
/// invalidate every sccache key) and can roll back by unsetting one variable.
pub fn require_test_deps() -> bool {
    require_test_deps_value(
        pplx_space_common_core::env::test_hook_var("SPACE_CI_REQUIRE_TEST_DEPS")
            .as_deref()
            .map(OsStr::new),
    )
}

fn require_test_deps_value(value: Option<&OsStr>) -> bool {
    value.is_some_and(|value| !value.is_empty() && value != "0")
}

/// Record that a test skipped because a dependency was unavailable.
///
/// Emits a grep-able `SPACE-TEST-DEP-SKIP:` marker on stderr and, when
/// `SPACE_TEST_DEP_SKIP_LOG` names an absolute file, appends the same line
/// there so CI can count skips across a build even though nextest hides the
/// captured output of passing tests. A configured log is part of the
/// measurement contract: invalid paths and write failures are returned rather
/// than silently producing incomplete soak data.
pub fn report_dependency_skip(message: &str) -> io::Result<()> {
    let path = pplx_space_common_core::env::test_hook_var("SPACE_TEST_DEP_SKIP_LOG");
    report_dependency_skip_to(message, path.as_deref().map(Path::new))
}

fn report_dependency_skip_to(message: &str, path: Option<&Path>) -> io::Result<()> {
    // Keep the append-only measurement format one physical line per skip even
    // when a container runtime returns a multi-line diagnostic.
    let message = message
        .split(['\r', '\n'])
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let record = format!("SPACE-TEST-DEP-SKIP: {message}\n");
    eprint!("{record}");
    if let Some(path) = path {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SPACE_TEST_DEP_SKIP_LOG must be an absolute path",
            ));
        }
        append_skip_record(path, record.as_bytes()).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to append SPACE_TEST_DEP_SKIP_LOG at {}: {error}",
                    path.display()
                ),
            )
        })?;
    }
    Ok(())
}

fn append_skip_record(path: &Path, record: &[u8]) -> io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(record)
}

async fn shared_postgres(graceful: bool) -> TestDbResult<Option<Arc<SharedPostgres>>> {
    let graceful = graceful && !require_test_deps();
    let shared_cell = SHARED_POSTGRES
        .get_or_init(|| async { Mutex::new(Weak::new()) })
        .await;
    let mut shared_guard = shared_cell.lock().await;

    if let Some(shared) = shared_guard.upgrade() {
        return Ok(Some(shared));
    }

    match SharedPostgres::start().await {
        Ok(shared) => {
            let shared = Arc::new(shared);
            *shared_guard = Arc::downgrade(&shared);
            Ok(Some(shared))
        }
        Err(InitError::Unavailable(message)) if graceful => {
            report_dependency_skip(&format!("postgres unavailable: {message}"))?;
            Ok(None)
        }
        Err(InitError::Unavailable(message)) => Err(FixtureError::boxed(format!(
            "postgres testcontainer is unavailable: {message}"
        ))),
        Err(InitError::Failed(message)) => Err(FixtureError::boxed(message)),
    }
}

struct SharedPostgres {
    _container: Option<ContainerAsync<PostgresImage>>,
    admin_database_url: String,
    clone_lock: Mutex<()>,
}

impl SharedPostgres {
    async fn start() -> Result<Self, InitError> {
        if let Some(admin_database_url) =
            pplx_space_common_core::env::test_hook_var(SHARED_POSTGRES_ADMIN_URL_ENV)
        {
            database_url_from_admin(&admin_database_url, TEMPLATE_DATABASE)
                .map_err(|error| InitError::failed(error.to_string()))?;
            return Ok(Self {
                _container: None,
                admin_database_url,
                clone_lock: Mutex::new(()),
            });
        }

        let (image_name, image_tag) =
            test_image_ref("TESTCONTAINERS_POSTGRES_IMAGE", DEFAULT_POSTGRES_IMAGE)
                .map_err(|error| InitError::failed(error.to_string()))?;

        let container = PostgresImage::default()
            .with_host_auth()
            .with_name(image_name)
            .with_tag(image_tag)
            .start()
            .await
            .map_err(|error| InitError::Unavailable(error.to_string()))?;

        let host = container
            .get_host()
            .await
            .map_err(|error| InitError::failed(format!("failed to get postgres host: {error}")))?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|error| InitError::failed(format!("failed to get postgres port: {error}")))?;
        let admin_database_url = database_url(&host.to_string(), port, "postgres");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_database_url)
            .await
            .map_err(|error| {
                InitError::failed(format!("failed to connect to postgres: {error}"))
            })?;

        let template_database_url = database_url(&host.to_string(), port, TEMPLATE_DATABASE);
        create_template_database(&admin_pool, &template_database_url).await?;
        admin_pool.close().await;

        Ok(Self {
            _container: Some(container),
            admin_database_url,
            clone_lock: Mutex::new(()),
        })
    }

    async fn cloned_database(self: &Arc<Self>, max_connections: u32) -> TestDbResult<TestDb> {
        self.create_database(max_connections, Some(TEMPLATE_DATABASE))
            .await
    }

    /// Create a database on the shared server without cloning it from the
    /// migrated [`TEMPLATE_DATABASE`]. The new database is Postgres's own
    /// blank default template (`template1`): no schema, no rows, nothing
    /// applied. For callers that manage their own migration-table state
    /// directly and would collide with a pre-migrated clone.
    async fn cloned_raw_database(self: &Arc<Self>, max_connections: u32) -> TestDbResult<TestDb> {
        self.create_database(max_connections, None).await
    }

    async fn create_database(
        self: &Arc<Self>,
        max_connections: u32,
        template: Option<&str>,
    ) -> TestDbResult<TestDb> {
        let database_name = format!("space_test_{}", Uuid::now_v7().simple());
        // FILE_COPY copies the template's files instead of the WAL_LOG default,
        // which writes every page of the template to WAL and through shared
        // buffers: about 6 MB per clone, and 7.8 GB of WAL across the 1,286
        // clones of one CI run. FILE_COPY pays two checkpoints instead, which
        // cost little on these throwaway servers because neither runs with
        // fsync (see setup-nextest-postgres.sh and the testcontainers default).
        let create_database = match template {
            Some(template) => format!(
                "CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY",
                quote_ident(&database_name),
                quote_ident(template)
            ),
            None => format!(
                "CREATE DATABASE {} STRATEGY FILE_COPY",
                quote_ident(&database_name)
            ),
        };

        let clone_guard = self.clone_lock.lock().await;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_database_url)
            .await?;
        sqlx::query(sqlx::AssertSqlSafe(create_database.as_str()))
            .execute(&admin_pool)
            .await?;
        admin_pool.close().await;
        drop(clone_guard);

        // Arm cleanup as soon as the database exists: a failure deriving the
        // URL or connecting the per-test pool below must still drop the
        // database rather than strand it on the shared run-scoped server.
        let cleanup = DatabaseCleanup {
            database_name,
            admin_database_url: self.admin_database_url.clone(),
        };

        let database_url =
            database_url_from_admin(&self.admin_database_url, &cleanup.database_name)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(&database_url)
            .await?;

        Ok(TestDb {
            pool,
            database_url,
            _cleanup: cleanup,
            _shared_postgres: Arc::clone(self),
        })
    }
}

async fn create_template_database(
    admin_pool: &PgPool,
    template_database_url: &str,
) -> Result<(), InitError> {
    let create_template = format!("CREATE DATABASE {}", quote_ident(TEMPLATE_DATABASE));
    sqlx::query(sqlx::AssertSqlSafe(create_template.as_str()))
        .execute(admin_pool)
        .await
        .map_err(|error| {
            InitError::failed(format!(
                "failed to create postgres template database: {error}"
            ))
        })?;

    let template_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(template_database_url)
        .await
        .map_err(|error| {
            InitError::failed(format!(
                "failed to connect to postgres template database: {error}"
            ))
        })?;

    POSTGRES_MIGRATOR
        .run(&template_pool)
        .await
        .map_err(|error| {
            InitError::failed(format!(
                "failed to migrate postgres template database: {error}"
            ))
        })?;
    // Cross-organization tests use these stable IDs independently of production seeds.
    sqlx::query(
        "INSERT INTO organizations (id, name, slug) VALUES
         ('00000000-0000-0000-0000-000000000002', 'Test Organization A', 'test-org-a'),
         ('00000000-0000-0000-0000-000000000003', 'Test Organization B', 'test-org-b')
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, slug = EXCLUDED.slug",
    )
    .execute(&template_pool)
    .await
    .map_err(|error| {
        InitError::failed(format!("failed to provision test organizations: {error}"))
    })?;
    template_pool.close().await;

    let lock_template = format!(
        "ALTER DATABASE {} WITH IS_TEMPLATE true ALLOW_CONNECTIONS false",
        quote_ident(TEMPLATE_DATABASE)
    );
    sqlx::query(sqlx::AssertSqlSafe(lock_template.as_str()))
        .execute(admin_pool)
        .await
        .map_err(|error| {
            InitError::failed(format!(
                "failed to lock postgres template database: {error}"
            ))
        })?;

    Ok(())
}

/// Initialize the migrated template used by the nextest run-level Postgres.
///
/// This is public for the setup binary shipped by this crate; tests should use
/// [`pg_test_db`] or [`try_pg_test_db`] instead.
#[doc(hidden)]
pub async fn initialize_postgres_template(admin_database_url: &str) -> TestDbResult<()> {
    let template_database_url = database_url_from_admin(admin_database_url, TEMPLATE_DATABASE)?;
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_database_url)
        .await?;
    let result = create_template_database(&admin_pool, &template_database_url).await;
    admin_pool.close().await;
    result.map_err(|error| FixtureError::boxed(error.message()))
}

fn database_url(host: &str, port: u16, database: &str) -> String {
    format!("postgres://postgres@{host}:{port}/{database}?sslmode=disable")
}

fn database_url_from_admin(admin_database_url: &str, database: &str) -> TestDbResult<String> {
    admin_database_url
        .strip_suffix("/postgres?sslmode=disable")
        .map(|prefix| format!("{prefix}/{database}?sslmode=disable"))
        .ok_or_else(|| {
            FixtureError::boxed("postgres admin URL must end with /postgres?sslmode=disable")
        })
}

fn quote_ident(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}

enum InitError {
    Unavailable(String),
    Failed(String),
}

impl InitError {
    fn failed(message: String) -> Self {
        Self::Failed(message)
    }

    fn message(self) -> String {
        match self {
            Self::Unavailable(message) | Self::Failed(message) => message,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct FixtureError {
    message: String,
}

impl FixtureError {
    fn new(message: String) -> Self {
        Self { message }
    }

    fn boxed(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
        Box::new(Self {
            message: message.into(),
        })
    }
}

#[cfg(test)]
mod tests;
