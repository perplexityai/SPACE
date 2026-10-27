//! The publisher's durable upload receipts in its own SQLite catalog.
//! Transport acknowledgments precede these writes; registration follows them.

use pplx_space_common_core::{sql, types::Digest};
use pplx_space_sqlite::SqliteU64;
use rusqlite::{Connection, OptionalExtension, params};

use super::super::delivery::{Binding, Location, Object};

/// The builder owns this database; it never opens template-manager's live
/// cache registry. The schema and all SQL stay in this storage module.
pub struct Catalog(Connection);

impl Catalog {
    pub async fn open(path: &std::path::Path) -> Result<Self, pplx_space_sqlite::SqliteError> {
        static MIGRATOR: sqlx::migrate::Migrator =
            sqlx::migrate!("../../../../db/template-chunk-publisher");
        pplx_space_sqlite::migrate("template-chunk-publisher", path, &MIGRATOR).await?;
        let connection = pplx_space_sqlite::open_rusqlite(
            path,
            pplx_space_sqlite::RusqliteOpenOptions {
                foreign_keys: true,
                synchronous_normal: false,
                ..Default::default()
            },
        )?;
        Ok(Self(connection))
    }

    pub fn publish(
        &mut self,
        source: std::fs::File,
        sink: &mut impl super::ObjectSink,
        staging: &std::path::Path,
        chunk_size: super::super::ChunkSize,
        expected: Digest,
    ) -> Result<super::UploadedPublication, super::PublishError> {
        super::publish(source, &mut self.0, sink, staging, chunk_size, expected)
    }

    pub fn repack(
        &mut self,
        manifest: &super::super::delivery::AuthenticatedManifest,
        source: &impl super::repack::PackSource,
        sink: &mut impl super::ObjectSink,
        staging: &std::path::Path,
        target: super::repack::PackTarget,
    ) -> Result<super::UploadedPublication, super::repack::RepackError> {
        super::repack::repack(manifest, &mut self.0, source, sink, staging, target)
    }

    pub fn acknowledge_registration(
        &mut self,
        namespace: &str,
        manifests: &[Digest],
    ) -> rusqlite::Result<()> {
        let transaction = self.0.transaction()?;
        for digest in manifests {
            if phase(&transaction, namespace, *digest)? == Some(Phase::Registered) {
                continue;
            }
            if !advance(
                &transaction,
                namespace,
                *digest,
                Phase::Uploaded,
                Phase::Registered,
            )? {
                return Err(rusqlite::Error::InvalidQuery);
            }
        }
        transaction.commit()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Prepared,
    Uploaded,
    Registered,
}

impl Phase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Uploaded => "uploaded",
            Self::Registered => "registered",
        }
    }
}

impl std::fmt::Display for Phase {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unrecognized chunk publication phase")]
pub struct InvalidPhase;

impl std::str::FromStr for Phase {
    type Err = InvalidPhase;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "uploaded" => Ok(Self::Uploaded),
            "registered" => Ok(Self::Registered),
            _ => Err(InvalidPhase),
        }
    }
}

pub struct UploadedChunk {
    pub location: Location,
    pub object: Object,
}

fn digest_column(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<Digest> {
    row.get::<_, String>(column)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

pub fn find_chunk(
    conn: &Connection,
    namespace: &str,
    digest: Digest,
    length: u64,
) -> rusqlite::Result<Option<UploadedChunk>> {
    conn.query_row(
        sql!(
            "SELECT l.object_digest, l.compressed_offset, l.compressed_length,
                    o.compressed_bytes, o.version_id
             FROM chunk_uploaded_locations l
             JOIN chunk_uploaded_objects o
               ON o.namespace = l.namespace AND o.object_digest = l.object_digest
             WHERE l.namespace = ?1 AND l.chunk_digest = ?2 AND l.raw_length = ?3"
        ),
        params![namespace, digest.to_string(), SqliteU64(length)],
        |row| {
            Ok(UploadedChunk {
                location: Location {
                    object: digest_column(row, 0)?,
                    compressed_offset: row.get::<_, SqliteU64>(1)?.0,
                    compressed_length: row.get::<_, SqliteU64>(2)?.0,
                },
                object: Object {
                    compressed_bytes: row.get::<_, SqliteU64>(3)?.0,
                    version_id: row.get(4)?,
                },
            })
        },
    )
    .optional()
}

pub struct PackedChunk {
    pub digest: Digest,
    pub raw_length: u64,
    pub offset: u64,
    pub compressed_length: u64,
}

/// Object and chunk receipts commit together. Concurrent publishers may keep
/// their own immutable copy in their manifest; the catalog keeps its first copy.
pub fn record_object(
    conn: &mut Connection,
    namespace: &str,
    digest: Digest,
    object: &Object,
    chunks: &[PackedChunk],
) -> rusqlite::Result<()> {
    let transaction = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    transaction.execute(
        sql!(
            "INSERT INTO chunk_uploaded_objects
                 (namespace, object_digest, compressed_bytes, version_id)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (namespace, object_digest) DO NOTHING"
        ),
        params![
            namespace,
            digest.to_string(),
            SqliteU64(object.compressed_bytes),
            object.version_id
        ],
    )?;
    {
        let mut insert = transaction.prepare_cached(sql!(
            "INSERT INTO chunk_uploaded_locations
                 (namespace, chunk_digest, raw_length, object_digest,
                  compressed_offset, compressed_length)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (namespace, chunk_digest, raw_length) DO NOTHING"
        ))?;
        for chunk in chunks {
            insert.execute(params![
                namespace,
                chunk.digest.to_string(),
                SqliteU64(chunk.raw_length),
                digest.to_string(),
                SqliteU64(chunk.offset),
                SqliteU64(chunk.compressed_length),
            ])?;
        }
    }
    transaction.commit()
}

pub fn prepare(
    conn: &Connection,
    namespace: &str,
    binding: Binding,
    image_blake3: Digest,
    bytes: &[u8],
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO chunk_publications
                 (namespace, manifest_digest, index_digest, rootfs_digest,
                  image_blake3, manifest_bytes, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (namespace, manifest_digest) DO NOTHING"
        ),
        params![
            namespace,
            binding.manifest_digest.to_string(),
            binding.index_digest.to_string(),
            binding.rootfs_digest.to_string(),
            image_blake3.to_string(),
            bytes,
            Phase::Prepared.as_str()
        ],
    )?;
    Ok(())
}

pub fn phase(
    conn: &Connection,
    namespace: &str,
    manifest: Digest,
) -> rusqlite::Result<Option<Phase>> {
    conn.query_row(
        sql!("SELECT phase FROM chunk_publications WHERE namespace = ?1 AND manifest_digest = ?2"),
        params![namespace, manifest.to_string()],
        |row| {
            row.get::<_, String>(0)?.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        },
    )
    .optional()
}

pub fn advance(
    conn: &Connection,
    namespace: &str,
    manifest: Digest,
    expected: Phase,
    next: Phase,
) -> rusqlite::Result<bool> {
    Ok(conn.execute(
        sql!(
            "UPDATE chunk_publications SET phase = ?4
             WHERE namespace = ?1 AND manifest_digest = ?2 AND phase = ?3"
        ),
        params![
            namespace,
            manifest.to_string(),
            expected.as_str(),
            next.as_str()
        ],
    )? == 1)
}
