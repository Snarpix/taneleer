use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

use super::Storage;
use crate::artifact::ArtifactItemInfo;
use crate::error::Result;
use crate::source::{Hashsum, Sha256};

#[derive(Debug)]
pub enum SqliteError {
    InvalidVersion,
}

impl std::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for SqliteError {}

pub struct SqliteStorage {
    pool: SqlitePool,
}

const CURRENT_DB_VERSION: u32 = 1;

impl SqliteStorage {
    pub async fn new(path: &std::path::PathBuf) -> Result<SqliteStorage> {
        let conn_opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(conn_opts)
            .await?;

        let res = SqliteStorage { pool };

        let mut db_version = res.get_db_version().await?;
        if db_version != CURRENT_DB_VERSION {
            while db_version != CURRENT_DB_VERSION {
                match db_version {
                    0 => res.migrate_0().await?,
                    _ => return Err(SqliteError::InvalidVersion.into()),
                }
                db_version += 1;
            }
            res.update_db_version().await?;
        }
        Ok(res)
    }

    async fn get_db_version(&self) -> Result<u32> {
        let res = sqlx::query_as::<_, (u32,)>("PRAGMA user_version;")
            .fetch_one(&self.pool)
            .await?;
        Ok(res.0)
    }

    async fn update_db_version(&self) -> Result<()> {
        sqlx::query(&format!("PRAGMA user_version = {};", CURRENT_DB_VERSION))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn migrate_0(&self) -> Result<()> {
        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_classes;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_classes(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_reserves;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_reserves(
    id INTEGER PRIMARY KEY,
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    uuid BLOB NOT NULL UNIQUE CHECK (LENGTH(uuid) = 16),
    reserve_time INTEGER NOT NULL
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_blobs;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_blobs(
    id INTEGER PRIMARY KEY,
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    digest BLOB NOT NULL CHECK (LENGTH(digest) = 32),
    size INTEGER NOT NULL,
    upload_time INTEGER NOT NULL,
    UNIQUE(digest, class_id)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_manifests;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_manifests(
    id INTEGER PRIMARY KEY,
    uuid BLOB NOT NULL UNIQUE CHECK (LENGTH(uuid) = 16),
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    digest BLOB NOT NULL CHECK (LENGTH(digest) = 32),
    size INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    upload_time INTEGER NOT NULL,
    UNIQUE(digest, class_id)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_blob_links;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_blob_links(
    id INTEGER PRIMARY KEY,
    manifest_id INTEGER NOT NULL REFERENCES artifact_manifests(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    blob_id INTEGER NOT NULL REFERENCES artifact_blobs(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE(manifest_id, blob_id)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    #[must_use]
    async fn create_class(&self, name: &str) -> Result<()> {
        sqlx::query(r#"INSERT INTO artifact_classes(name) VALUES (?1);"#)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[must_use]
    async fn create_artifact_reserve(&self, artifact_uuid: Uuid, class_name: &str) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO artifact_reserves(class_id, uuid, reserve_time) 
            VALUES ((SELECT id FROM artifact_classes WHERE name = ?1), ?2, UNIXEPOCH());"#,
        )
        .bind(class_name)
        .bind(artifact_uuid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[must_use]
    async fn commit_blob(&self, class_name: &str, digest: Sha256, size: i64) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO artifact_blobs(class_id, digest, size, upload_time) 
            VALUES ((SELECT id FROM artifact_classes WHERE name = ?1), ?2, ?3, UNIXEPOCH());"#,
        )
        .bind(class_name)
        .bind(digest.as_slice())
        .bind(size)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[must_use]
    async fn commit_artifact(
        &self,
        artifact_uuid: Uuid,
        manifest_digest: Sha256,
        manifest_size: i64,
        manifest_type: &str,
        blob_digests: Vec<Sha256>,
    ) -> Result<()> {
        let mut t = self.pool.begin().await?;
        sqlx::query(
            r#"
INSERT INTO artifact_manifests(uuid, class_id, digest, size, content_type, upload_time) 
VALUES (?1, (SELECT class_id FROM artifact_reserves WHERE uuid = ?1), ?2, ?3, ?4, UNIXEPOCH());"#,
        )
        .bind(artifact_uuid)
        .bind(manifest_digest.as_slice())
        .bind(manifest_size)
        .bind(manifest_type)
        .execute(&mut t)
        .await?;

        for digest in blob_digests {
            sqlx::query(
                r#"
INSERT INTO artifact_blob_links(manifest_id, blob_id) 
VALUES ((SELECT id FROM artifact_manifests WHERE uuid = ?1), (SELECT id FROM artifact_blobs WHERE digest = ?2));
            "#,
            )
            .bind(artifact_uuid)
            .bind(digest.as_slice())
            .execute(&mut t)
            .await?;
        }

        sqlx::query(r#"DELETE FROM artifact_reserves WHERE uuid = ?1;"#)
            .bind(artifact_uuid)
            .execute(&mut t)
            .await?;

        t.commit().await?;
        Ok(())
    }

    #[must_use]
    async fn get_artifact_items(
        &self,
        _class_name: &str,
        artifact_uuid: Uuid,
    ) -> Result<Vec<ArtifactItemInfo>> {
        let (manifest_id, mani_digest, mani_size): (i64, Vec<u8>, i64) =
            sqlx::query_as("SELECT id, digest, size FROM artifact_manifests WHERE uuid = ?1")
                .bind(artifact_uuid)
                .fetch_one(&self.pool)
                .await?;

        let mut res: Vec<ArtifactItemInfo> = sqlx::query_as::<_, (Vec<u8>, i64)>(r#"
SELECT digest, size FROM artifact_blob_links AS BL JOIN artifact_blobs AS AB ON BL.blob_id = AB.id WHERE BL.manifest_id = ?1;"#
            )
            .bind(manifest_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|(digest, size)| {
                ArtifactItemInfo{
                    id: format!("blobs/sha256:{}", hex::encode(&digest)),
                    size: size.try_into().unwrap(),
                    hash: Hashsum::Sha256(digest.try_into().unwrap()),
                }
            })
            .collect();
        res.push(ArtifactItemInfo {
            id: format!("manifests/sha256:{}", hex::encode(&mani_digest)),
            size: mani_size.try_into().unwrap(),
            hash: Hashsum::Sha256(mani_digest.try_into().unwrap()),
        });
        Ok(res)
    }
}
