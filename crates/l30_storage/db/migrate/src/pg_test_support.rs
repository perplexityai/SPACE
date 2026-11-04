//! Shared Postgres testcontainer fixture for migrator tests.
//!
//! Starts a throwaway Postgres via Docker. When no container runtime is
//! reachable it fails the test, or returns `Ok(None)` for the test to skip
//! when the run opted out, by the same rule as the shared fixtures
//! (`pplx_space_db_test_fixtures::report_unavailable_dependency`).

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection as _, PgConnection, PgPool};
use testcontainers::{ContainerAsync, ImageExt, core::Mount, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres as PostgresImage;

pub(crate) use pplx_space_test_env::TestResult;

pub(crate) struct PostgresFixture {
    pub(crate) pool: PgPool,
    host: String,
    port: u16,
    _container: ContainerAsync<PostgresImage>,
}

impl PostgresFixture {
    /// Connection URL for `user`. The fixture container trusts all host
    /// connections, so any LOGIN role connects without a password.
    pub(crate) fn database_url(&self, user: &str) -> String {
        format!(
            "postgres://{user}@{}:{}/postgres?sslmode=disable",
            self.host, self.port
        )
    }

    pub(crate) async fn connect_as(&self, user: &str) -> TestResult<PgConnection> {
        Ok(PgConnection::connect(&self.database_url(user)).await?)
    }
}

pub(crate) async fn postgres_fixture() -> TestResult<Option<PostgresFixture>> {
    let (image_name, image_tag) = pplx_space_db_test_fixtures::test_image_ref(
        "TESTCONTAINERS_POSTGRES_IMAGE",
        "public.ecr.aws/docker/library/postgres:16-alpine",
    )?;
    // Durability settings as the shared server's (`space_start_test_postgres`
    // in ci/scripts/lib/test-deps.sh): a throwaway server needs none, and
    // each test here starts its own, initdb included.
    let container = match PostgresImage::default()
        .with_host_auth()
        .with_name(image_name)
        .with_tag(image_tag)
        .with_env_var("POSTGRES_INITDB_ARGS", "--no-sync")
        .with_env_var("PGDATA", "/var/lib/postgresql/data")
        .with_mount(Mount::tmpfs_mount("/var/lib/postgresql/data"))
        .with_cmd([
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
        ])
        .start()
        .await
    {
        Ok(container) => container,
        Err(error) => {
            pplx_space_db_test_fixtures::report_unavailable_dependency("postgres", &error)?;
            return Ok(None);
        }
    };
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres@{host}:{port}/postgres?sslmode=disable");
    // The image's readiness line also comes from the entrypoint's temporary
    // initdb server, which listens on no TCP port, so a connection can land
    // in the moment before the real server starts and be reset.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let pool = loop {
        match PgPoolOptions::new().max_connections(2).connect(&url).await {
            Err(sqlx::Error::Io(_)) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            connected => break connected?,
        }
    };
    Ok(Some(PostgresFixture {
        pool,
        host,
        port,
        _container: container,
    }))
}
