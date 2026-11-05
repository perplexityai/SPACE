//! Shared Postgres testcontainer fixture for migrator tests.
//!
//! Starts a throwaway Postgres via Docker. When no container runtime is
//! reachable it fails the test, or returns `Ok(None)` for the test to skip
//! when the run opted out, by the same rule as the shared fixtures
//! (`pplx_space_db_test_fixtures::report_unavailable_dependency`).
//!
//! [`bare_postgres_server`] instead returns the bare server nextest's
//! `bare-postgres` setup script starts for the run, when it published one. A
//! published server that cannot be reached fails the test rather than
//! skipping it: the run asked for that server.

use std::str::FromStr as _;
use std::sync::Arc;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection as _, PgConnection, PgPool};
use testcontainers::{ContainerAsync, ImageExt, core::Mount, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres as PostgresImage;

pub(crate) use pplx_space_test_env::TestResult;

/// The bare server `ci/scripts/setup-nextest-postgres.sh --bare` starts for a
/// nextest run: nothing applied, and no other suite's tests on it.
const BARE_POSTGRES_ADMIN_URL_ENV: &str = "SPACE_TEST_BARE_POSTGRES_ADMIN_URL";

pub(crate) struct PostgresFixture {
    pub(crate) pool: PgPool,
    /// `host:port`.
    server: String,
    database: String,
    _container: Option<Arc<ContainerAsync<PostgresImage>>>,
}

impl PostgresFixture {
    /// Connection URL for `user`. The fixture's server trusts all host
    /// connections, so any LOGIN role connects without a password.
    pub(crate) fn database_url(&self, user: &str) -> String {
        format!(
            "postgres://{user}@{}/{}?sslmode=disable",
            self.server, self.database
        )
    }

    pub(crate) async fn connect_as(&self, user: &str) -> TestResult<PgConnection> {
        Ok(PgConnection::connect(&self.database_url(user)).await?)
    }
}

/// A server to create databases on, and the container running it when this
/// test started one.
pub(crate) struct PostgresServer {
    /// `host:port`.
    server: String,
    _container: Option<Arc<ContainerAsync<PostgresImage>>>,
}

impl PostgresServer {
    /// A superuser connection to the server's `postgres` database.
    pub(crate) async fn connect_admin(&self) -> TestResult<PgConnection> {
        Ok(PgConnection::connect(&self.database_url("postgres")).await?)
    }

    /// A fixture on this server's `database`, which keeps the container
    /// running.
    pub(crate) async fn database(&self, database: &str) -> TestResult<PostgresFixture> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&self.database_url(database))
            .await?;
        Ok(PostgresFixture {
            pool,
            server: self.server.clone(),
            database: database.to_owned(),
            _container: self._container.clone(),
        })
    }

    fn database_url(&self, database: &str) -> String {
        format!(
            "postgres://postgres@{}/{database}?sslmode=disable",
            self.server
        )
    }
}

/// The run's bare server when nextest's setup script published one, else a
/// container of this test's own. Cluster-global state such as roles is shared
/// with every other test given the bare server.
pub(crate) async fn bare_postgres_server() -> TestResult<Option<PostgresServer>> {
    let admin_url = match std::env::var(BARE_POSTGRES_ADMIN_URL_ENV) {
        Ok(admin_url) => admin_url,
        Err(std::env::VarError::NotPresent) => {
            let Some(fixture) = postgres_fixture().await? else {
                return Ok(None);
            };
            fixture.pool.close().await;
            return Ok(Some(PostgresServer {
                server: fixture.server,
                _container: fixture._container,
            }));
        }
        Err(std::env::VarError::NotUnicode(value)) => {
            return Err(
                format!("{BARE_POSTGRES_ADMIN_URL_ENV}={value:?} is not valid UTF-8").into(),
            );
        }
    };
    let options = PgConnectOptions::from_str(&admin_url)?;
    Ok(Some(PostgresServer {
        server: format!("{}:{}", options.get_host(), options.get_port()),
        _container: None,
    }))
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
        server: format!("{host}:{port}"),
        database: "postgres".to_owned(),
        _container: Some(Arc::new(container)),
    }))
}
