//! Convergent provisioning of control-plane Postgres roles
//!
//! `space-db-migrator bootstrap-roles` is the single owner of role
//! provisioning for the control-plane database. Every run converges the
//! database onto the same model, so it is safe to re-run at will;
//! provisioning flows run it before and after `control-plane up`.
//!
//! Provisioned roles:
//!
//! - `space_superadmin` — break-glass operator login: `CREATEDB CREATEROLE`,
//!   plus `rds_superuser` membership on RDS (or `SUPERUSER` on self-managed
//!   Postgres), plus `space_migrator` membership for application objects.
//! - `space_migrator` — the DDL identity schema migrations run as:
//!   `control-plane up`/`down` connect as it directly or assume it via
//!   `SET ROLE` (see [`assume_migrator_role`]). It owns every application
//!   relation, routine, and standalone type in schema `public` (pre-existing
//!   objects are adopted on each run — replacing a function or extending an
//!   enum requires ownership, not privileges), tables it creates become
//!   readable by `space_user` through default privileges, and it runs under
//!   minutes-scale statement and lock timeouts sized for DDL.
//! - `space_user` — application login: read/write DML on every application
//!   table except credential tables.
//! - `space_ro` — read-only query login (humans, dashboards) with the same
//!   credential fence.
//! - `space_creds` — credential-manager login: read/write DML on every
//!   application table including credential tables; also a member of the
//!   `space_credmgr` capability role whose table grants live in migrations.
//!
//! `space_user`, `space_ro`, and `space_creds` all run under short
//! role-level session timeouts (`statement_timeout`,
//! `idle_in_transaction_session_timeout`). `space_user` and `space_ro` also
//! take an optional `lock_timeout`.
//!
//! Deliberately unmanaged: the connecting admin (the RDS master user, named
//! `space_admin` in SPACE deployments). Live services authenticate as it,
//! so this tool must never rewrite its credentials —
//! [`ensure_no_session_collision`] refuses to run if any managed role name
//! matches the session user.
//!
//! Role creation happens here rather than in SQL migrations because
//! migrations run as `space_migrator`, which deliberately lacks
//! `CREATEROLE`. The `space_credmgr` capability role is pre-created here for
//! the same reason; the guarded legacy migration that introduced it no-ops
//! once the role exists.
//!
//! Credential tables are fenced: they are excluded from `space_user`'s and
//! `space_ro`'s grants and any existing grant is revoked on every run.
//! Because default privileges auto-grant new `space_migrator` tables to
//! those roles, a credential table added by a migration stays accessible
//! until the next `bootstrap-roles` run — which is why provisioning must
//! re-run this command after migrations.

use pplx_space_common_svc::sql;
use std::collections::BTreeSet;

use clap::Parser;
use sqlx::{Connection as _, PgConnection};

use crate::sqlstate;

/// Default `lock_timeout` for `space_migrator` sessions, and so the bound a
/// migration inherits when it sets none of its own. `migration_retry` sizes its
/// retry window against this; a test ties the two together.
pub(crate) const MIGRATOR_LOCK_TIMEOUT_DEFAULT: &str = "5min";

const SUPERADMIN_ROLE: &str = "space_superadmin";
const USER_ROLE: &str = "space_user";
const RO_ROLE: &str = "space_ro";
pub(crate) const MIGRATOR_ROLE: &str = "space_migrator";
const CREDS_ROLE: &str = "space_creds";

/// DML privilege set for the read/write logins; DDL stays with
/// `space_migrator` ownership.
const READ_WRITE_PRIVILEGES: &str = "SELECT, INSERT, UPDATE, DELETE";

/// Read-only capability role granted credential-table access by migrations.
const CREDMGR_CAPABILITY_ROLE: &str = "space_credmgr";

/// RDS's closest analogue to a superuser role; absent on self-managed
/// Postgres, where `space_superadmin` becomes a real superuser instead.
const RDS_SUPERUSER_ROLE: &str = "rds_superuser";

/// Tables only `space_creds`/`space_credmgr` (and owners/admins) may read:
/// credential payloads, KEK metadata, and every wrapped-DEK/ciphertext
/// sidecar. `space_user` and `space_ro` never receive grants on these and
/// have any existing grant revoked on every run. Extend this list in the
/// same PR as any migration that creates a credential-bearing table — the
/// bootstrap end-to-end test cross-checks it against `wrapped_dek` columns
/// in the migrated schema.
const CREDENTIAL_TABLES: &[&str] = &[
    "org_master_keys",
    "org_branching_keys",
    "org_storage_vend_identities",
    "user_credentials",
    "org_credentials",
    "sandbox_encrypted_fields",
    "snapshot_storage_location_encrypted_fields",
    "template_release_storage_location_encrypted_fields",
    "template_encrypted_fields",
    "sandbox_volume_encryption",
];

/// Columns holding password/token hashes or replayable bearer material that
/// the human/dashboard `space_ro` login must not read. The rest of each
/// table stays readable to `space_ro` via a column-scoped grant, and the
/// service logins (`space_user`, `space_creds`) keep full access. Extend in
/// the same PR as any migration adding such a column.
const RO_COLUMN_DENYLIST: &[(&str, &[&str])] = &[
    ("users", &["password_hash"]),
    ("sessions", &["token_hash"]),
    ("api_keys", &["key_hash"]),
    ("data_plane_sessions", &["token_hash"]),
    ("sandbox_federated_service_grants", &["grant_blob"]),
    ("template_system_publication_grants", &["id"]),
    ("template_tenant_publication_grants", &["id"]),
];

#[derive(Parser)]
pub struct BootstrapRolesArgs {
    /// Control-plane Postgres connection URI. Must authenticate as a role
    /// that can create roles: the RDS master user or a local superuser.
    #[arg(long)]
    pub database_url: String,

    /// `statement_timeout` applied to `space_user`, `space_ro`, and
    /// `space_creds` sessions.
    #[arg(long, default_value = "10s")]
    pub app_statement_timeout: String,

    /// `idle_in_transaction_session_timeout` applied to `space_user`,
    /// `space_ro`, and `space_creds` sessions.
    #[arg(long, default_value = "15s")]
    pub app_idle_transaction_timeout: String,

    /// `lock_timeout` applied to the `space_user`, `space_ro`, and
    /// `space_creds` sessions — not `space_migrator`, whose DDL legitimately
    /// needs to wait (see [`converge_session_timeouts`]). Explicit `0` disables
    /// the bound, which is Postgres's own
    /// default and the behavior every environment had before this knob
    /// existed: a statement blocked on a lock waits out `statement_timeout`,
    /// holding its pooled connection for the whole wait.
    ///
    /// **Unset is not `0`.** Omitting this leaves each role's existing
    /// `lock_timeout` untouched, and that distinction is the whole point of
    /// the `Option`: `bootstrap-roles` is convergent and runs from several
    /// entry points that carry no environment config — the bare command in
    /// `db/README.md`, `scripts/space-db-bootstrap-roles-eks.sh`, the devbox
    /// and kind bootstraps. If unset meant `0`, any one of those run against
    /// an enabled environment would silently un-bound every application role
    /// fleet-wide, with nothing in the output saying so. Rolling back is
    /// therefore an explicit `0`, not an omission.
    ///
    /// Enabling it is a per-environment rollout, not a code default, because
    /// it converts lock waits into `55P03` errors for every application
    /// query at once. Set it via `SPACE_DB_APP_LOCK_TIMEOUT` on the
    /// provisioning Job (`space-db-migrate` chart, `appLockTimeout`) so each
    /// environment flips independently. Pick a sub-second value: it has to be
    /// well under `--app-statement-timeout` to bound anything, and the
    /// connection-holding cost of a blocked query is proportional to it.
    #[arg(long, env = "SPACE_DB_APP_LOCK_TIMEOUT")]
    pub app_lock_timeout: Option<String>,

    /// `statement_timeout` applied to `space_migrator` sessions; sized for
    /// DDL and index builds.
    #[arg(long, default_value = "10min")]
    pub migrator_statement_timeout: String,

    /// `lock_timeout` applied to `space_migrator` sessions.
    #[arg(long, default_value = MIGRATOR_LOCK_TIMEOUT_DEFAULT)]
    pub migrator_lock_timeout: String,

    /// Proceed when password environment variables are unset: affected roles
    /// are still created and their grants converged, but their LOGIN state
    /// and password are left untouched.
    #[arg(long)]
    pub allow_missing_passwords: bool,
}

/// `pg_class.relkind` values this tool operates on; the catalog queries
/// filter to exactly this set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelKind {
    Table,
    PartitionedTable,
    Sequence,
    View,
    MaterializedView,
}

impl TryFrom<&str> for RelKind {
    type Error = UnexpectedRelationKindError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "r" => Ok(Self::Table),
            "p" => Ok(Self::PartitionedTable),
            "S" => Ok(Self::Sequence),
            "v" => Ok(Self::View),
            "m" => Ok(Self::MaterializedView),
            other => Err(UnexpectedRelationKindError(other.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unexpected pg_class.relkind {0:?}")]
pub struct UnexpectedRelationKindError(String);

impl RelKind {
    /// The canonical `pg_class.relkind` letter for this kind.
    fn as_str(self) -> &'static str {
        match self {
            Self::Table => "r",
            Self::PartitionedTable => "p",
            Self::Sequence => "S",
            Self::View => "v",
            Self::MaterializedView => "m",
        }
    }

    /// Keyword for `ALTER <keyword> ... OWNER TO`.
    fn alter_keyword(self) -> &'static str {
        match self {
            Self::Table | Self::PartitionedTable => "TABLE",
            Self::Sequence => "SEQUENCE",
            Self::View => "VIEW",
            Self::MaterializedView => "MATERIALIZED VIEW",
        }
    }

    fn is_sequence(self) -> bool {
        self == Self::Sequence
    }
}

impl std::fmt::Display for RelKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Decode `(relkind, relname)` catalog rows at the query boundary.
fn parse_relations(
    rows: Vec<(String, String)>,
) -> Result<Vec<(RelKind, String)>, UnexpectedRelationKindError> {
    rows.into_iter()
        .map(|(relkind, relname)| Ok((RelKind::try_from(relkind.as_str())?, relname)))
        .collect()
}

/// `pg_proc.prokind` values this tool adopts; the catalog query filters to
/// exactly this set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoutineKind {
    Function,
    Procedure,
    Aggregate,
    WindowFunction,
}

impl TryFrom<&str> for RoutineKind {
    type Error = UnexpectedRoutineKindError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "f" => Ok(Self::Function),
            "p" => Ok(Self::Procedure),
            "a" => Ok(Self::Aggregate),
            "w" => Ok(Self::WindowFunction),
            other => Err(UnexpectedRoutineKindError(other.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unexpected pg_proc.prokind {0:?}")]
pub struct UnexpectedRoutineKindError(String);

impl RoutineKind {
    /// Keyword for `ALTER <keyword> ... OWNER TO`.
    fn alter_keyword(self) -> &'static str {
        match self {
            Self::Function | Self::WindowFunction => "FUNCTION",
            Self::Procedure => "PROCEDURE",
            Self::Aggregate => "AGGREGATE",
        }
    }
}

/// `pg_type.typtype` values this tool adopts: the standalone types migrations
/// create and later alter in place. Relation row types follow their relation's
/// owner and array/multirange types follow their element type, so none of
/// those need (or accept) separate adoption; `Composite` here means only
/// free-standing `CREATE TYPE ... AS (...)` types, which the catalog query
/// separates from row types by their `pg_class.relkind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TypeKind {
    Composite,
    Enum,
    Domain,
    Range,
}

impl TryFrom<&str> for TypeKind {
    type Error = UnexpectedTypeKindError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "c" => Ok(Self::Composite),
            "e" => Ok(Self::Enum),
            "d" => Ok(Self::Domain),
            "r" => Ok(Self::Range),
            other => Err(UnexpectedTypeKindError(other.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unexpected pg_type.typtype {0:?}")]
pub struct UnexpectedTypeKindError(String);

impl TypeKind {
    /// Keyword for `ALTER <keyword> ... OWNER TO`.
    fn alter_keyword(self) -> &'static str {
        match self {
            Self::Composite | Self::Enum | Self::Range => "TYPE",
            Self::Domain => "DOMAIN",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapRole {
    Superadmin,
    User,
    Ro,
    Migrator,
    Creds,
}

impl BootstrapRole {
    const ALL: [Self; 5] = [
        Self::Superadmin,
        Self::User,
        Self::Ro,
        Self::Migrator,
        Self::Creds,
    ];

    fn role_name(self) -> &'static str {
        match self {
            Self::Superadmin => SUPERADMIN_ROLE,
            Self::User => USER_ROLE,
            Self::Ro => RO_ROLE,
            Self::Migrator => MIGRATOR_ROLE,
            Self::Creds => CREDS_ROLE,
        }
    }

    fn password_env(self) -> &'static str {
        match self {
            Self::Superadmin => "SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD",
            Self::User => "SPACE_DB_BOOTSTRAP_USER_PASSWORD",
            Self::Ro => "SPACE_DB_BOOTSTRAP_RO_PASSWORD",
            Self::Migrator => "SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD",
            Self::Creds => "SPACE_DB_BOOTSTRAP_CREDS_PASSWORD",
        }
    }
}

#[derive(Default)]
struct BootstrapPasswords {
    superadmin: Option<String>,
    user: Option<String>,
    ro: Option<String>,
    migrator: Option<String>,
    creds: Option<String>,
}

impl BootstrapPasswords {
    fn from_env() -> Self {
        let read = |role: BootstrapRole| {
            std::env::var(role.password_env())
                .ok()
                .filter(|value| !value.is_empty())
        };
        Self {
            superadmin: read(BootstrapRole::Superadmin),
            user: read(BootstrapRole::User),
            ro: read(BootstrapRole::Ro),
            migrator: read(BootstrapRole::Migrator),
            creds: read(BootstrapRole::Creds),
        }
    }

    fn get(&self, role: BootstrapRole) -> Option<&str> {
        match role {
            BootstrapRole::Superadmin => self.superadmin.as_deref(),
            BootstrapRole::User => self.user.as_deref(),
            BootstrapRole::Ro => self.ro.as_deref(),
            BootstrapRole::Migrator => self.migrator.as_deref(),
            BootstrapRole::Creds => self.creds.as_deref(),
        }
    }

    fn missing_password_envs(&self) -> Vec<&'static str> {
        BootstrapRole::ALL
            .into_iter()
            .filter(|role| self.get(*role).is_none())
            .map(BootstrapRole::password_env)
            .collect()
    }

    fn any_present(&self) -> bool {
        BootstrapRole::ALL
            .into_iter()
            .any(|role| self.get(role).is_some())
    }
}

struct SessionTimeouts<'a> {
    app_statement: &'a str,
    app_idle_in_transaction: &'a str,
    /// `None` leaves each role's existing `lock_timeout` untouched; `Some("0")`
    /// is an explicit rollback to unbounded. See
    /// [`BootstrapRolesArgs::app_lock_timeout`].
    app_lock: Option<&'a str>,
    migrator_statement: &'a str,
    migrator_lock: &'a str,
}

/// Why [`run`] left the control-plane roles unconverged.
///
/// Variants name the step that failed rather than the kind of failure: a
/// provisioning run is read by whoever is unblocking a deploy, and which
/// statement died is the whole diagnosis. Every SQL variant keeps the driver
/// error as its source.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapRolesError {
    #[error(
        "missing password environment variables: {missing}; export them or pass --allow-missing-passwords"
    )]
    MissingPasswords { missing: String },
    #[error("failed to connect to control-plane Postgres: {0}")]
    Connect(#[source] sqlx::Error),
    #[error(
        "bootstrap-roles left {skipped} statements unconverged (lock or privilege \
         contention); re-run to converge"
    )]
    Unconverged { skipped: usize },
    #[error("failed to set session lock_timeout: {0}")]
    SetSessionLockTimeout(#[source] sqlx::Error),
    #[error(
        "failed to disable statement logging; refusing to send ALTER ROLE ... PASSWORD \
         statements a server log could capture — connect as a role allowed to SET \
         log_statement, or run grants-only with --allow-missing-passwords: {0}"
    )]
    DisableStatementLogging(#[source] sqlx::Error),
    #[error("failed to read current_user: {0}")]
    ReadCurrentUser(#[source] sqlx::Error),
    #[error(
        "bootstrap-roles manages role {session_user}, but this session is connected as it; \
         refusing to touch the active connection credential — connect as the admin/master \
         user instead"
    )]
    SessionUserIsManaged { session_user: String },
    #[error("failed to create role {role}: {source}")]
    CreateRole {
        role: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to converge login credentials for {role}: {source}")]
    ConvergeLoginCredentials {
        role: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to converge attributes for {role}: {source}")]
    ConvergeRoleAttributes {
        role: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to converge superuser-tier attributes for {role}: {source}")]
    ConvergeSuperuserAttributes {
        role: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to inspect attributes of {role}: {source}")]
    InspectRoleAttributes {
        role: String,
        #[source]
        source: sqlx::Error,
    },
    #[error(
        "managed role {role} carries a privileged attribute (superuser={superuser}, \
         replication={replication}, bypassrls={bypassrls}) this session cannot \
         clear; fix it as a superuser before re-running bootstrap-roles"
    )]
    PrivilegedRoleAttribute {
        role: String,
        superuser: bool,
        replication: bool,
        bypassrls: bool,
    },
    #[error("failed to converge space_superadmin attributes: {0}")]
    ConvergeSuperadminAttributes(#[source] sqlx::Error),
    #[error("failed to make space_superadmin a superuser: {0}")]
    MakeSuperadminSuperuser(#[source] sqlx::Error),
    #[error("failed to check whether role {role} exists: {source}")]
    CheckRoleExists {
        role: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to inspect session superuser state: {0}")]
    InspectSuperuserState(#[source] sqlx::Error),
    #[error("failed to set {parameter} = {value} for {role}: {source}")]
    SetRoleParameter {
        role: String,
        parameter: &'static str,
        value: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to grant {granted} to {member}: {source}")]
    GrantMembership {
        granted: &'static str,
        member: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to revoke {granted} from {member}: {source}")]
    RevokeMembership {
        granted: String,
        member: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to enumerate managed role memberships: {0}")]
    EnumerateMemberships(#[source] sqlx::Error),
    #[error("failed to enumerate {objects} for ownership adoption: {source}")]
    EnumerateForOwnership {
        objects: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to adopt ownership of {object}: {source}")]
    AdoptOwnership {
        object: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to enumerate relations for grants: {0}")]
    EnumerateRelationsForGrants(#[source] sqlx::Error),
    #[error("failed to run: {sql}: {source}")]
    Statement {
        sql: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to fence {relation} off {USER_ROLE} and {RO_ROLE}: {source}")]
    FenceCredentialTable {
        relation: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to fence {relation} columns off {RO_ROLE}: {source}")]
    FenceColumns {
        relation: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to grant column-scoped read on {relation}: {source}")]
    GrantColumnScopedRead {
        relation: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to enumerate columns of {table}: {source}")]
    EnumerateColumns {
        table: String,
        #[source]
        source: sqlx::Error,
    },
    #[error(transparent)]
    RelationKind(#[from] UnexpectedRelationKindError),
    #[error(transparent)]
    RoutineKind(#[from] UnexpectedRoutineKindError),
    #[error(transparent)]
    TypeKind(#[from] UnexpectedTypeKindError),
}

pub(crate) async fn run(args: BootstrapRolesArgs) -> Result<(), BootstrapRolesError> {
    let passwords = BootstrapPasswords::from_env();
    let missing = passwords.missing_password_envs();
    if !missing.is_empty() {
        let missing = missing.join(", ");
        if args.allow_missing_passwords {
            tracing::warn!(
                missing = %missing,
                "password environment variables unset; affected roles keep their current LOGIN state"
            );
        } else {
            return Err(BootstrapRolesError::MissingPasswords { missing });
        }
    }

    // An env var set to the empty string reaches clap as `Some("")`, which is
    // not a value Postgres accepts; treat it as unset so a chart rendering an
    // empty override degrades to "leave it alone" rather than failing the whole
    // provisioning run. Logged rather than silent: in a design whose point is
    // that unset differs from `0`, a supplied-but-discarded value is worth
    // seeing, and an operator who typed `--app-lock-timeout ""` meaning
    // "disable" gets told it did nothing instead of guessing later.
    let app_lock_timeout = args
        .app_lock_timeout
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if args.app_lock_timeout.is_some() && app_lock_timeout.is_none() {
        tracing::warn!(
            "app lock timeout was supplied empty and is being ignored; existing \
             lock_timeout settings are left untouched. Pass '0' to disable the bound"
        );
    }

    let mut conn = PgConnection::connect(&args.database_url)
        .await
        .map_err(BootstrapRolesError::Connect)?;
    let timeouts = SessionTimeouts {
        app_statement: &args.app_statement_timeout,
        app_idle_in_transaction: &args.app_idle_transaction_timeout,
        app_lock: app_lock_timeout,
        migrator_statement: &args.migrator_statement_timeout,
        migrator_lock: &args.migrator_lock_timeout,
    };
    let skipped = bootstrap(&mut conn, &passwords, &timeouts).await?;

    if skipped != 0 {
        // Fail loudly so supervised rollouts retry instead of accepting a
        // partially converged database; every statement is convergent, so a
        // re-run picks up exactly the skipped objects.
        return Err(BootstrapRolesError::Unconverged { skipped });
    }
    println!("bootstrap-roles: converged");
    Ok(())
}

/// Converge roles, memberships, ownership, and grants. Returns the number of
/// per-object statements skipped on lock or privilege contention; those
/// converge on a later run.
async fn bootstrap(
    conn: &mut PgConnection,
    passwords: &BootstrapPasswords,
    timeouts: &SessionTimeouts<'_>,
) -> Result<usize, BootstrapRolesError> {
    ensure_no_session_collision(conn).await?;
    prepare_session(conn, passwords.any_present()).await?;
    ensure_roles(conn, passwords).await?;
    converge_superadmin_attributes(conn).await?;
    converge_session_timeouts(conn, timeouts).await?;
    converge_memberships(conn).await?;
    // Schema privileges must precede ownership adoption: ALTER ... OWNER TO
    // requires the new owner to hold CREATE on the relation's schema, which
    // locked-down (PG15+) public schemas do not grant by default.
    let mut skipped = converge_schema_privileges(conn).await?;
    skipped += adopt_relation_ownership(conn).await?;
    skipped += adopt_routine_ownership(conn).await?;
    skipped += adopt_type_ownership(conn).await?;
    skipped += converge_grants(conn).await?;
    Ok(skipped)
}

/// Run control-plane migration sessions as `space_migrator` even when the
/// connection authenticates as an admin: objects the migrations create are
/// then owned by the migrator and its default privileges fire.
///
/// Fails when the role exists but cannot be assumed (a half-provisioned
/// bootstrap — fix the membership rather than silently migrating as the
/// admin). Falls back with a warning only when the role does not exist, so
/// environments that have never run `bootstrap-roles` keep working.
pub(crate) async fn assume_migrator_role(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    let current_user: String = sqlx::query_scalar(sql!("SELECT current_user"))
        .fetch_one(&mut *conn)
        .await?;
    if current_user == MIGRATOR_ROLE {
        return Ok(());
    }
    let migrator_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)"
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_one(&mut *conn)
    .await?;
    if !migrator_exists {
        tracing::warn!(
            %current_user,
            "{MIGRATOR_ROLE} does not exist; control-plane migrations run as the connecting \
             role — run `space-db-migrator bootstrap-roles` first to provision it"
        );
        return Ok(());
    }
    sqlx::query(sql!("SET ROLE space_migrator"))
        .execute(&mut *conn)
        .await
        .map_err(|error| {
            sqlx::Error::Configuration(
                format!(
                    "{MIGRATOR_ROLE} exists but this session cannot assume it; connect as a \
                     member of {MIGRATOR_ROLE} (the bootstrap admin) or as {MIGRATOR_ROLE} \
                     directly: {error}"
                )
                .into(),
            )
        })?;
    // Role-level settings (`ALTER ROLE ... SET`) only load at login, not on
    // SET ROLE, so the migrator's statement/lock timeouts must be installed
    // explicitly for the admin-connection path to match a direct login.
    let settings: Vec<String> = sqlx::query_scalar(sql!(
        r"
        SELECT unnest(s.setconfig)
        FROM pg_db_role_setting s
        JOIN pg_roles r ON r.oid = s.setrole
        WHERE r.rolname = $1 AND s.setdatabase = 0
        "
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_all(&mut *conn)
    .await?;
    for setting in &settings {
        if let Some((parameter, value)) = setting.split_once('=') {
            sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
                "SET {} = {}",
                quote_ident(parameter),
                quote_literal(value)
            ))))
            .execute(&mut *conn)
            .await?;
        }
    }
    tracing::info!(%current_user, "control-plane statements run as {MIGRATOR_ROLE} via SET ROLE");
    Ok(())
}

/// Bound every lock wait so a hot table cannot stall provisioning or queue
/// traffic behind an ACCESS EXCLUSIVE request, and mute statement logging so
/// role passwords do not land in server logs. Muting is mandatory whenever
/// passwords are present — `ALTER ROLE ... PASSWORD` embeds the plaintext in
/// the statement — and best-effort for grants-only runs.
async fn prepare_session(
    conn: &mut PgConnection,
    passwords_present: bool,
) -> Result<(), BootstrapRolesError> {
    sqlx::query(sql!("SET lock_timeout = '2s'"))
        .execute(&mut *conn)
        .await
        .map_err(BootstrapRolesError::SetSessionLockTimeout)?;
    if let Err(error) = sqlx::query(sql!("SET log_statement = 'none'"))
        .execute(&mut *conn)
        .await
    {
        if passwords_present {
            return Err(BootstrapRolesError::DisableStatementLogging(error));
        }
        tracing::debug!(%error, "could not disable statement logging for the bootstrap session");
    }
    Ok(())
}

async fn ensure_roles(
    conn: &mut PgConnection,
    passwords: &BootstrapPasswords,
) -> Result<(), BootstrapRolesError> {
    for role in BootstrapRole::ALL {
        let name = role.role_name();
        ensure_role_exists(conn, name).await?;
        converge_role_attributes(conn, name).await?;
        if let Some(password) = passwords.get(role) {
            let sql = format!(
                "ALTER ROLE {} WITH LOGIN PASSWORD {}",
                quote_ident(name),
                quote_literal(password)
            );
            execute(conn, &sql).await.map_err(|source| {
                BootstrapRolesError::ConvergeLoginCredentials {
                    role: name.to_owned(),
                    source,
                }
            })?;
        }
    }

    // Pre-created here so control-plane migrations, which run as
    // space_migrator without CREATEROLE, never have to create roles; the
    // guarded migration that introduced this role no-ops once it exists.
    ensure_role_exists(conn, CREDMGR_CAPABILITY_ROLE).await?;
    converge_role_attributes(conn, CREDMGR_CAPABILITY_ROLE).await
}

/// Reset a managed role to the baseline attribute set so a pre-existing role
/// cannot smuggle privileged flags past the model (`space_superadmin` gets
/// its deliberate extras afterwards in [`converge_superadmin_attributes`]).
/// `SUPERUSER`/`REPLICATION`/`BYPASSRLS` can only be altered by a superuser,
/// so on RDS they are verified instead: a managed role carrying one is a
/// hard error rather than a silently retained escalation.
async fn converge_role_attributes(
    conn: &mut PgConnection,
    name: &str,
) -> Result<(), BootstrapRolesError> {
    execute(
        conn,
        &format!(
            "ALTER ROLE {} WITH NOCREATEDB NOCREATEROLE",
            quote_ident(name)
        ),
    )
    .await
    .map_err(|source| BootstrapRolesError::ConvergeRoleAttributes {
        role: name.to_owned(),
        source,
    })?;

    if session_is_superuser(conn).await? {
        execute(
            conn,
            &format!(
                "ALTER ROLE {} WITH NOSUPERUSER NOREPLICATION NOBYPASSRLS",
                quote_ident(name)
            ),
        )
        .await
        .map_err(|source| BootstrapRolesError::ConvergeSuperuserAttributes {
            role: name.to_owned(),
            source,
        })?;
        return Ok(());
    }

    let (rolsuper, rolreplication, rolbypassrls): (bool, bool, bool) = sqlx::query_as(sql!(
        "SELECT rolsuper, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = $1"
    ))
    .bind(name)
    .fetch_one(&mut *conn)
    .await
    .map_err(|source| BootstrapRolesError::InspectRoleAttributes {
        role: name.to_owned(),
        source,
    })?;
    if rolsuper || rolreplication || rolbypassrls {
        return Err(BootstrapRolesError::PrivilegedRoleAttribute {
            role: name.to_owned(),
            superuser: rolsuper,
            replication: rolreplication,
            bypassrls: rolbypassrls,
        });
    }
    Ok(())
}

async fn ensure_role_exists(
    conn: &mut PgConnection,
    name: &str,
) -> Result<(), BootstrapRolesError> {
    if role_exists(conn, name).await? {
        return Ok(());
    }
    execute(conn, &format!("CREATE ROLE {} NOLOGIN", quote_ident(name)))
        .await
        .map_err(|source| BootstrapRolesError::CreateRole {
            role: name.to_owned(),
            source,
        })?;
    println!("bootstrap-roles: created role {name}");
    Ok(())
}

async fn converge_superadmin_attributes(
    conn: &mut PgConnection,
) -> Result<(), BootstrapRolesError> {
    execute(
        conn,
        &format!("ALTER ROLE {SUPERADMIN_ROLE} WITH CREATEDB CREATEROLE"),
    )
    .await
    .map_err(BootstrapRolesError::ConvergeSuperadminAttributes)?;

    if role_exists(conn, RDS_SUPERUSER_ROLE).await? {
        execute(
            conn,
            &format!("GRANT {RDS_SUPERUSER_ROLE} TO {SUPERADMIN_ROLE}"),
        )
        .await
        .map_err(|source| BootstrapRolesError::GrantMembership {
            granted: RDS_SUPERUSER_ROLE,
            member: SUPERADMIN_ROLE.to_owned(),
            source,
        })?;
    } else if session_is_superuser(conn).await? {
        execute(
            conn,
            &format!("ALTER ROLE {SUPERADMIN_ROLE} WITH SUPERUSER"),
        )
        .await
        .map_err(BootstrapRolesError::MakeSuperadminSuperuser)?;
    } else {
        tracing::warn!(
            "session is not a superuser and rds_superuser does not exist; space_superadmin is limited to CREATEDB CREATEROLE"
        );
    }
    Ok(())
}

/// The connecting admin's own credentials are sacrosanct: live services
/// authenticate with them (the RDS master user is `space_admin`). Refuse to
/// run if any managed role name matches the session user — `ensure_roles`
/// would otherwise rewrite the password everything connects with.
async fn ensure_no_session_collision(conn: &mut PgConnection) -> Result<(), BootstrapRolesError> {
    let session_user: String = sqlx::query_scalar(sql!("SELECT current_user"))
        .fetch_one(&mut *conn)
        .await
        .map_err(BootstrapRolesError::ReadCurrentUser)?;
    if BootstrapRole::ALL
        .into_iter()
        .any(|role| role.role_name() == session_user)
    {
        return Err(BootstrapRolesError::SessionUserIsManaged { session_user });
    }
    Ok(())
}

async fn converge_session_timeouts(
    conn: &mut PgConnection,
    timeouts: &SessionTimeouts<'_>,
) -> Result<(), BootstrapRolesError> {
    // Role-level settings only apply to the role that logs in (membership
    // does not propagate them), so they are set on each login role itself.
    for role in [USER_ROLE, RO_ROLE, CREDS_ROLE] {
        for (parameter, value) in [
            ("statement_timeout", timeouts.app_statement),
            (
                "idle_in_transaction_session_timeout",
                timeouts.app_idle_in_transaction,
            ),
        ] {
            set_role_parameter(conn, role, parameter, value).await?;
        }
    }

    // `lock_timeout` is written only when the caller supplied one. Convergence
    // here means converging on the *declared* state, and an omitted flag
    // declares nothing: this command runs from entry points that carry no
    // environment config, so writing a default would let any of them silently
    // un-bound an enabled environment.
    //
    // `space_creds` is included. It was held out while nothing in the
    // credential lane classified `55P03` — the credmgr authority's status
    // mapping and the admin identity handlers turned any SQL error into a
    // permanent-looking `Internal`/500, so bounding the lane would have traded
    // a slow success for a fast failure.
    //
    // Four things had to become true, not two, and neither of the last two is
    // obvious from the credential routes alone:
    //
    // 1. credmgr answers contention `Unavailable` (`contention_status`);
    // 2. the admin identity handlers answer 503/409
    //    (`IdentityAdminError::Contended`);
    // 3. the api honours credmgr's `Unavailable` rather than flattening it
    //    (`SealError::into_response`) — credmgr and the api share this login
    //    and split one seal between them, so classifying only the producer
    //    leaves the disposition discarded one process later;
    // 4. the wrapped-branching-key store keeps its `sqlx::Error` instead of
    //    stringifying it (`WrappedKeyStoreError::Query`), because a `String`
    //    has no SQLSTATE left to classify and every failure became gRPC
    //    `Internal` downstream.
    //
    // Each of 3 and 4 is a whole boundary that would have silently turned this
    // bound back into the trade it was held out of. Pinned by
    // `a_retryable_seal_from_credmgr_is_not_flattened_to_a_500` and
    // `the_store_preserves_the_sqlstate_instead_of_stringifying_it`.
    if let Some(app_lock) = timeouts.app_lock {
        for role in [USER_ROLE, RO_ROLE, CREDS_ROLE] {
            set_role_parameter(conn, role, "lock_timeout", app_lock).await?;
        }
    }

    // Migrations hold DDL locks and rewrite tables; give the migrator room
    // to finish while still bounding a wedged migration.
    for (parameter, value) in [
        ("statement_timeout", timeouts.migrator_statement),
        ("lock_timeout", timeouts.migrator_lock),
    ] {
        set_role_parameter(conn, MIGRATOR_ROLE, parameter, value).await?;
    }
    Ok(())
}

async fn set_role_parameter(
    conn: &mut PgConnection,
    role: &str,
    parameter: &'static str,
    value: &str,
) -> Result<(), BootstrapRolesError> {
    let sql = format!(
        "ALTER ROLE {role} SET {parameter} = {}",
        quote_literal(value)
    );
    execute(conn, &sql)
        .await
        .map_err(|source| BootstrapRolesError::SetRoleParameter {
            role: role.to_owned(),
            parameter,
            value: value.to_owned(),
            source,
        })
}

async fn converge_memberships(conn: &mut PgConnection) -> Result<(), BootstrapRolesError> {
    let session_user: String = sqlx::query_scalar(sql!("SELECT current_user"))
        .fetch_one(&mut *conn)
        .await
        .map_err(BootstrapRolesError::ReadCurrentUser)?;
    // Membership in space_migrator lets this session adopt relation
    // ownership and set default privileges on the migrator's behalf.
    if session_user != MIGRATOR_ROLE {
        execute(
            conn,
            &format!("GRANT {MIGRATOR_ROLE} TO {}", quote_ident(&session_user)),
        )
        .await
        .map_err(|source| BootstrapRolesError::GrantMembership {
            granted: MIGRATOR_ROLE,
            member: session_user.clone(),
            source,
        })?;
    }
    execute(conn, &format!("GRANT {MIGRATOR_ROLE} TO {SUPERADMIN_ROLE}"))
        .await
        .map_err(|source| BootstrapRolesError::GrantMembership {
            granted: MIGRATOR_ROLE,
            member: SUPERADMIN_ROLE.to_owned(),
            source,
        })?;
    execute(
        conn,
        &format!("GRANT {CREDMGR_CAPABILITY_ROLE} TO {CREDS_ROLE}"),
    )
    .await
    .map_err(|source| BootstrapRolesError::GrantMembership {
        granted: CREDMGR_CAPABILITY_ROLE,
        member: CREDS_ROLE.to_owned(),
        source,
    })?;
    revoke_unexpected_memberships(conn, &session_user).await?;
    Ok(())
}

/// Strip managed roles of privileged memberships outside the intended
/// matrix, so a pre-existing `space_user` that was granted `space_migrator`
/// (or `rds_superuser`, or any superuser role) cannot `SET ROLE` past the
/// fences after a successful bootstrap. Scoped to memberships in the roles
/// this tool reasons about plus superuser-flagged roles — unrelated
/// organizational group memberships are left alone. The connecting session
/// user's own memberships are never touched.
async fn revoke_unexpected_memberships(
    conn: &mut PgConnection,
    session_user: &str,
) -> Result<(), BootstrapRolesError> {
    let expected: &[(&str, &str)] = &[
        (SUPERADMIN_ROLE, MIGRATOR_ROLE),
        (SUPERADMIN_ROLE, RDS_SUPERUSER_ROLE),
        (CREDS_ROLE, CREDMGR_CAPABILITY_ROLE),
    ];
    let managed = [
        SUPERADMIN_ROLE,
        USER_ROLE,
        RO_ROLE,
        MIGRATOR_ROLE,
        CREDS_ROLE,
        CREDMGR_CAPABILITY_ROLE,
    ];

    let memberships: Vec<(String, String)> = sqlx::query_as(sql!(
        r"
        SELECT member.rolname, granted.rolname
        FROM pg_auth_members m
        JOIN pg_roles member ON member.oid = m.member
        JOIN pg_roles granted ON granted.oid = m.roleid
        WHERE member.rolname = ANY($1)
          AND (granted.rolname = ANY($2) OR granted.rolname = $3 OR granted.rolsuper)
        ORDER BY member.rolname, granted.rolname
        "
    ))
    .bind(&managed[..])
    .bind(&managed[..])
    .bind(RDS_SUPERUSER_ROLE)
    .fetch_all(&mut *conn)
    .await
    .map_err(BootstrapRolesError::EnumerateMemberships)?;

    for (member, granted) in &memberships {
        if member == session_user || expected.iter().any(|(m, g)| m == member && g == granted) {
            continue;
        }
        tracing::warn!(%member, %granted, "revoking unexpected privileged membership");
        execute(
            conn,
            &format!(
                "REVOKE {} FROM {}",
                quote_ident(granted),
                quote_ident(member)
            ),
        )
        .await
        .map_err(|source| BootstrapRolesError::RevokeMembership {
            granted: granted.clone(),
            member: member.clone(),
            source,
        })?;
    }
    Ok(())
}

/// Move ownership of application relations in schema `public` onto
/// `space_migrator` so migrations connecting as that role can ALTER and DROP
/// pre-existing objects. Relations that cannot be adopted right now are
/// skipped with a warning and converge on a later run.
async fn adopt_relation_ownership(conn: &mut PgConnection) -> Result<usize, BootstrapRolesError> {
    // Tables first: sequences owned by their columns follow the table, and a
    // linked sequence can only be reassigned once its table has moved.
    let relations: Vec<(String, String)> = sqlx::query_as(sql!(
        r"
        SELECT c.relkind::text, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_roles owner ON owner.oid = c.relowner
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'S', 'v', 'm')
          AND owner.rolname <> $1
          AND NOT EXISTS (
              SELECT FROM pg_depend d
              WHERE d.classid = 'pg_class'::regclass
                AND d.objid = c.oid
                AND d.deptype = 'e'
          )
        ORDER BY CASE WHEN c.relkind IN ('r', 'p') THEN 0 ELSE 1 END, c.relname
        "
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_all(&mut *conn)
    .await
    .map_err(|source| BootstrapRolesError::EnumerateForOwnership {
        objects: "relations",
        source,
    })?;
    let relations = parse_relations(relations)?;

    let mut skipped = 0_usize;
    for (relkind, relname) in &relations {
        let sql = format!(
            "ALTER {} public.{} OWNER TO {MIGRATOR_ROLE}",
            relkind.alter_keyword(),
            quote_ident(relname)
        );
        match execute(conn, &sql).await {
            Ok(()) => {}
            Err(error) if is_recoverable_adoption_error(&error) => {
                tracing::warn!(relation = %relname, %error, "skipping ownership adoption");
                skipped += 1;
            }
            Err(source) => {
                return Err(BootstrapRolesError::AdoptOwnership {
                    object: relname.clone(),
                    source,
                });
            }
        }
    }
    Ok(skipped)
}

/// Move ownership of application routines in schema `public` onto
/// `space_migrator`. `CREATE OR REPLACE FUNCTION` and `DROP FUNCTION` require
/// ownership of the existing function — table privileges and schema CREATE do
/// not confer it — so a migration that evolves a pre-existing trigger
/// function in place fails as `space_migrator` until the function is adopted.
/// Routines that cannot be adopted right now are skipped with a warning and
/// converge on a later run.
async fn adopt_routine_ownership(conn: &mut PgConnection) -> Result<usize, BootstrapRolesError> {
    let routines: Vec<(String, String, String)> = sqlx::query_as(sql!(
        r"
        SELECT p.prokind::text, p.proname, pg_get_function_identity_arguments(p.oid)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_roles owner ON owner.oid = p.proowner
        WHERE n.nspname = 'public'
          AND owner.rolname <> $1
          AND NOT EXISTS (
              SELECT FROM pg_depend d
              WHERE d.classid = 'pg_proc'::regclass
                AND d.objid = p.oid
                AND d.deptype = 'e'
          )
        ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
        "
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_all(&mut *conn)
    .await
    .map_err(|source| BootstrapRolesError::EnumerateForOwnership {
        objects: "routines",
        source,
    })?;

    let mut skipped = 0_usize;
    for (prokind, proname, identity_args) in &routines {
        let kind = RoutineKind::try_from(prokind.as_str())?;
        let sql = format!(
            "ALTER {} public.{}({identity_args}) OWNER TO {MIGRATOR_ROLE}",
            kind.alter_keyword(),
            quote_ident(proname)
        );
        match execute(conn, &sql).await {
            Ok(()) => {}
            Err(error) if is_recoverable_adoption_error(&error) => {
                tracing::warn!(routine = %proname, %error, "skipping ownership adoption");
                skipped += 1;
            }
            Err(source) => {
                return Err(BootstrapRolesError::AdoptOwnership {
                    object: proname.clone(),
                    source,
                });
            }
        }
    }
    Ok(skipped)
}

/// Move ownership of standalone application types (enums, domains, ranges) in
/// schema `public` onto `space_migrator`: `ALTER TYPE ... ADD VALUE` and
/// `DROP TYPE` require ownership just like function replacement does.
async fn adopt_type_ownership(conn: &mut PgConnection) -> Result<usize, BootstrapRolesError> {
    let types: Vec<(String, String)> = sqlx::query_as(sql!(
        r"
        SELECT t.typtype::text, t.typname
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_roles owner ON owner.oid = t.typowner
        WHERE n.nspname = 'public'
          AND (
              t.typtype IN ('e', 'd', 'r')
              OR (
                  t.typtype = 'c'
                  AND EXISTS (
                      SELECT FROM pg_class c
                      WHERE c.oid = t.typrelid
                        AND c.relkind = 'c'
                  )
              )
          )
          AND owner.rolname <> $1
          AND NOT EXISTS (
              SELECT FROM pg_depend d
              WHERE d.classid = 'pg_type'::regclass
                AND d.objid = t.oid
                AND d.deptype = 'e'
          )
        ORDER BY t.typname
        "
    ))
    .bind(MIGRATOR_ROLE)
    .fetch_all(&mut *conn)
    .await
    .map_err(|source| BootstrapRolesError::EnumerateForOwnership {
        objects: "types",
        source,
    })?;

    let mut skipped = 0_usize;
    for (typtype, typname) in &types {
        let kind = TypeKind::try_from(typtype.as_str())?;
        let sql = format!(
            "ALTER {} public.{} OWNER TO {MIGRATOR_ROLE}",
            kind.alter_keyword(),
            quote_ident(typname)
        );
        match execute(conn, &sql).await {
            Ok(()) => {}
            Err(error) if is_recoverable_adoption_error(&error) => {
                tracing::warn!(r#type = %typname, %error, "skipping ownership adoption");
                skipped += 1;
            }
            Err(source) => {
                return Err(BootstrapRolesError::AdoptOwnership {
                    object: typname.clone(),
                    source,
                });
            }
        }
    }
    Ok(skipped)
}

/// Converge schema privileges, per-role table grants, and the credential
/// fence. Returns the number of skipped statements.
///
/// Grant model: `space_user` and `space_creds` get read/write DML,
/// `space_ro` gets `SELECT` — everywhere except credential tables, which
/// only `space_creds` (and owners/admins) may touch.
/// Schema-level privileges, converged before ownership adoption because
/// `ALTER ... OWNER TO` requires the prospective owner to hold CREATE on the
/// relation's schema (PG15+ locked-down `public` grants nothing by default).
async fn converge_schema_privileges(conn: &mut PgConnection) -> Result<usize, BootstrapRolesError> {
    let mut skipped = 0_usize;
    for sql in [
        format!(
            "GRANT USAGE ON SCHEMA public TO {MIGRATOR_ROLE}, {USER_ROLE}, {RO_ROLE}, \
             {CREDS_ROLE}"
        ),
        format!("GRANT CREATE ON SCHEMA public TO {MIGRATOR_ROLE}"),
        // The dev-only `control-plane reset` path re-opens schema CREATE to
        // everyone; converge back onto the PG15+ default.
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC".to_string(),
    ] {
        match execute(conn, &sql).await {
            Ok(()) => {}
            Err(error) if is_recoverable_grant_error(&error) => {
                tracing::warn!(statement = %sql, %error, "skipping schema privilege statement");
                skipped += 1;
            }
            Err(source) => return Err(BootstrapRolesError::Statement { sql, source }),
        }
    }
    Ok(skipped)
}

async fn converge_grants(conn: &mut PgConnection) -> Result<usize, BootstrapRolesError> {
    let mut skipped = 0_usize;

    for sql in [
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {MIGRATOR_ROLE} IN SCHEMA public \
             GRANT {READ_WRITE_PRIVILEGES} ON TABLES TO {USER_ROLE}, {CREDS_ROLE}"
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {MIGRATOR_ROLE} IN SCHEMA public \
             GRANT SELECT ON TABLES TO {RO_ROLE}"
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {MIGRATOR_ROLE} IN SCHEMA public \
             GRANT USAGE, SELECT ON SEQUENCES TO {USER_ROLE}, {CREDS_ROLE}"
        ),
    ] {
        execute(conn, &sql)
            .await
            .map_err(|source| BootstrapRolesError::Statement { sql, source })?;
    }

    let relations: Vec<(String, String)> = sqlx::query_as(sql!(
        r"
        SELECT c.relkind::text, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p', 'S', 'v', 'm')
          AND NOT EXISTS (
              SELECT FROM pg_depend d
              WHERE d.classid = 'pg_class'::regclass
                AND d.objid = c.oid
                AND d.deptype = 'e'
          )
        ORDER BY c.relname
        "
    ))
    .fetch_all(&mut *conn)
    .await
    .map_err(BootstrapRolesError::EnumerateRelationsForGrants)?;
    let relations = parse_relations(relations)?;

    let credential_tables: BTreeSet<&str> = CREDENTIAL_TABLES.iter().copied().collect();
    let ro_column_denylist: std::collections::BTreeMap<&str, &[&str]> =
        RO_COLUMN_DENYLIST.iter().copied().collect();
    for (relkind, relname) in &relations {
        let target = quote_ident(relname);
        let statements = if relkind.is_sequence() {
            vec![format!(
                "GRANT USAGE, SELECT ON SEQUENCE public.{target} TO {USER_ROLE}, {CREDS_ROLE}"
            )]
        } else if credential_tables.contains(relname.as_str()) {
            vec![format!(
                "GRANT {READ_WRITE_PRIVILEGES} ON TABLE public.{target} TO {CREDS_ROLE}"
            )]
        } else if ro_column_denylist.contains_key(relname.as_str()) {
            // space_ro's column-scoped access is converged in the fence
            // section below.
            vec![format!(
                "GRANT {READ_WRITE_PRIVILEGES} ON TABLE public.{target} TO {USER_ROLE}, \
                 {CREDS_ROLE}"
            )]
        } else {
            vec![
                format!(
                    "GRANT {READ_WRITE_PRIVILEGES} ON TABLE public.{target} TO {USER_ROLE}, \
                     {CREDS_ROLE}"
                ),
                format!("GRANT SELECT ON TABLE public.{target} TO {RO_ROLE}"),
            ]
        };
        for sql in statements {
            match execute(conn, &sql).await {
                Ok(()) => {}
                Err(error) if is_recoverable_grant_error(&error) => {
                    tracing::warn!(relation = %relname, %error, "skipping grant");
                    skipped += 1;
                }
                Err(source) => return Err(BootstrapRolesError::Statement { sql, source }),
            }
        }
    }

    // The credential fence is a hard invariant: fail rather than leave
    // space_user or space_ro with access to a credential table.
    for (_, relname) in relations.iter().filter(|(relkind, relname)| {
        !relkind.is_sequence() && credential_tables.contains(relname.as_str())
    }) {
        let sql = format!(
            "REVOKE ALL ON TABLE public.{} FROM {USER_ROLE}, {RO_ROLE}",
            quote_ident(relname)
        );
        execute(conn, &sql)
            .await
            .map_err(|source| BootstrapRolesError::FenceCredentialTable {
                relation: relname.clone(),
                source,
            })?;
    }

    // Column fence, same hardness: hash/bearer columns are hidden from
    // space_ro while the rest of the table stays readable. Table-level
    // grants (including the default-privilege auto-grant on new tables) are
    // revoked first, then a column-scoped SELECT re-opens the safe columns.
    for (_, relname) in relations
        .iter()
        .filter(|(relkind, _)| !relkind.is_sequence())
    {
        let Some(denied) = ro_column_denylist.get(relname.as_str()).copied() else {
            continue;
        };
        let target = quote_ident(relname);
        execute(
            conn,
            &format!("REVOKE ALL ON TABLE public.{target} FROM {RO_ROLE}"),
        )
        .await
        .map_err(|source| BootstrapRolesError::FenceColumns {
            relation: relname.clone(),
            source,
        })?;
        let columns = ro_visible_columns(conn, relname, denied).await?;
        if !columns.is_empty() {
            let column_list = columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", ");
            execute(
                conn,
                &format!("GRANT SELECT ({column_list}) ON public.{target} TO {RO_ROLE}"),
            )
            .await
            .map_err(|source| BootstrapRolesError::GrantColumnScopedRead {
                relation: relname.clone(),
                source,
            })?;
        }
    }

    Ok(skipped)
}

/// Every column of `table` except the denied set, for the column-scoped
/// `space_ro` grant.
async fn ro_visible_columns(
    conn: &mut PgConnection,
    table: &str,
    denied: &[&str],
) -> Result<Vec<String>, BootstrapRolesError> {
    let all: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position"
    ))
    .bind(table)
    .fetch_all(conn)
    .await
    .map_err(|source| BootstrapRolesError::EnumerateColumns {
        table: table.to_owned(),
        source,
    })?;
    Ok(all
        .into_iter()
        .filter(|column| !denied.contains(&column.as_str()))
        .collect())
}

async fn execute(conn: &mut PgConnection, sql: &str) -> sqlx::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(sql!(sql)))
        .execute(conn)
        .await
        .map(|_| ())
}

async fn role_exists(conn: &mut PgConnection, role: &str) -> Result<bool, BootstrapRolesError> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)"
    ))
    .bind(role)
    .fetch_one(conn)
    .await
    .map_err(|source| BootstrapRolesError::CheckRoleExists {
        role: role.to_owned(),
        source,
    })
}

async fn session_is_superuser(conn: &mut PgConnection) -> Result<bool, BootstrapRolesError> {
    sqlx::query_scalar(sql!("SELECT current_setting('is_superuser') = 'on'"))
        .fetch_one(conn)
        .await
        .map_err(BootstrapRolesError::InspectSuperuserState)
}

/// Lock timeouts (55P03), privilege gaps on foreign-owned objects (42501),
/// and prerequisite-state failures such as a sequence linked to a table
/// whose adoption was skipped (55000) all converge on a later run.
fn is_recoverable_adoption_error(error: &sqlx::Error) -> bool {
    matches!(
        sqlstate(error).as_deref(),
        Some("55P03" | "42501" | "55000")
    )
}

fn is_recoverable_grant_error(error: &sqlx::Error) -> bool {
    matches!(sqlstate(error).as_deref(), Some("55P03" | "42501"))
}

/// Quote an SQL identifier (role or relation name).
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Quote an SQL string literal. `standard_conforming_strings` is on by
/// default on every supported Postgres, so doubling single quotes is a
/// complete escape.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests;
