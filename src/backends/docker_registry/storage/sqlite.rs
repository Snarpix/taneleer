use async_trait::async_trait;
use log::info;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

use super::{Storage, Result, StorageError};
use crate::artifact::ArtifactItemInfo;
use crate::source::{Hashsum, Sha256};

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
                    _ => return Err(StorageError::InvalidVersion.into()),
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
DROP TABLE IF EXISTS artifact_uploads;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_uploads(
    id INTEGER PRIMARY KEY,
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    uuid BLOB NOT NULL UNIQUE CHECK (LENGTH(uuid) = 16),
    create_time INTEGER NOT NULL,
    lock INTEGER NOT NULL
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
    digest BLOB NOT NULL UNIQUE CHECK (LENGTH(digest) = 32),
    size INTEGER NOT NULL,
    create_time INTEGER NOT NULL
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
    blob_id INTEGER NOT NULL REFERENCES artifact_blobs(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    create_time INTEGER NOT NULL,
    UNIQUE(blob_id, class_id)
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
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    digest BLOB NOT NULL CHECK (LENGTH(digest) = 32),
    size INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    create_time INTEGER NOT NULL,
    UNIQUE(digest, class_id)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_blob_usage;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_blob_usage(
    id INTEGER PRIMARY KEY,
    manifest_id INTEGER NOT NULL REFERENCES artifact_manifests(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    link_id INTEGER NOT NULL REFERENCES artifact_blob_links(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE(manifest_id, link_id)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_tags;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_tags(
    id INTEGER PRIMARY KEY,
    tag BLOB NOT NULL UNIQUE CHECK (LENGTH(tag) = 16),
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    manifest_id INTEGER NOT NULL REFERENCES artifact_manifests(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    create_time INTEGER NOT NULL,
    UNIQUE(tag, class_id)
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
    async fn create_upload(&self, name: &str, upload_uuid: Uuid) -> Result<()> {
        sqlx::query(
            r#"
INSERT INTO artifact_uploads(class_id, uuid, create_time, lock) 
    VALUES ((SELECT id FROM artifact_classes WHERE name = ?1), ?2, UNIXEPOCH(), 0);
    "#,
        )
        .bind(name)
        .bind(upload_uuid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[must_use]
    async fn lock_upload(&self, upload_uuid: Uuid) -> Result<()> {
        let res =
            sqlx::query(r#"UPDATE artifact_uploads SET lock = 1 WHERE uuid = ?1 AND lock = 0;"#)
                .bind(upload_uuid)
                .execute(&self.pool)
                .await?;
        if res.rows_affected() != 1 {
            return Err(StorageError::LockFailed.into());
        }
        Ok(())
    }

    #[must_use]
    async fn unlock_upload(&self, upload_uuid: Uuid) -> Result<()> {
        let res =
            sqlx::query(r#"UPDATE artifact_uploads SET lock = 0 WHERE uuid = ?1 AND lock = 1;"#)
                .bind(upload_uuid)
                .execute(&self.pool)
                .await?;
        if res.rows_affected() != 1 {
            return Err(StorageError::LockFailed.into());
        }
        Ok(())
    }

    #[must_use]
    async fn remove_upload(&self, upload_uuid: Uuid) -> Result<()> {
        let res = sqlx::query(r#"DELETE FROM artifact_uploads WHERE uuid = ?1 AND lock = 1;"#)
            .bind(upload_uuid)
            .execute(&self.pool)
            .await?;
        if res.rows_affected() != 1 {
            return Err(StorageError::LockFailed.into());
        }
        Ok(())
    }

    #[must_use]
    async fn commit_blob(&self, digest: Sha256, size: i64, upload_uuid: Uuid) -> Result<()> {
        let mut t = self.pool.begin().await?;

        sqlx::query(r#"INSERT INTO artifact_blobs(digest, size, create_time) VALUES (?1, ?2, UNIXEPOCH());"#)
        .bind(digest.as_slice())
        .bind(size)
        .execute(&mut t)
        .await?;

        let res = sqlx::query(r#"DELETE FROM artifact_uploads WHERE uuid = ?1 AND lock = 1;"#)
            .bind(upload_uuid)
            .execute(&mut t)
            .await?;
        if res.rows_affected() != 1 {
            return Err(StorageError::CommitFailed.into());
        }

        t.commit().await?;
        Ok(())
    }

    #[must_use]
    async fn link_blob(&self, digest: Sha256, class_name: &str) -> Result<()> {
        sqlx::query(r#"
INSERT INTO artifact_blob_links(blob_id, class_id, create_time)
    VALUES ((SELECT id FROM artifact_blobs WHERE digest = ?1), (SELECT id FROM artifact_classes WHERE name = ?2), UNIXEPOCH());"#)
        .bind(digest.as_slice())
        .bind(class_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[must_use]
    async fn commit_manifest(
        &self,
        class_name: &str,
        manifest_digest: Sha256,
        manifest_size: i64,
        manifest_type: &str,
        blob_digests: Vec<Sha256>,
    ) -> Result<()> {
        let mut t = self.pool.begin().await?;
        let row_id = sqlx::query(
            r#"
INSERT INTO artifact_manifests(class_id, digest, size, content_type, create_time) 
    VALUES ((SELECT id FROM artifact_classes WHERE name = ?1), ?2, ?3, ?4, UNIXEPOCH());"#,
        )
        .bind(class_name)
        .bind(manifest_digest.as_slice())
        .bind(manifest_size)
        .bind(manifest_type)
        .execute(&mut t)
        .await?
        .last_insert_rowid();

        for digest in blob_digests {
            sqlx::query(
                r#"
INSERT INTO artifact_blob_usage(manifest_id, link_id) 
    VALUES (?1,
        (SELECT ABL.id FROM artifact_blob_links AS ABL 
            JOIN artifact_blobs AS AB ON ABL.blob_id = AB.id 
            WHERE AB.digest = ?2));
            "#,
            )
            .bind(row_id)
            .bind(digest.as_slice())
            .execute(&mut t)
            .await?;
        }

        t.commit().await?;
        Ok(())
    }

    async fn commit_tag(
        &self,
        artifact_uuid: Uuid,
        class_name: &str,
        manifest_digest: Sha256,
    ) -> Result<()> {
        sqlx::query(
            r#"
INSERT INTO artifact_tags(tag, class_id, manifest_id, create_time)
    VALUES (?1, 
        (SELECT id FROM artifact_classes WHERE name = ?2),
        (SELECT id FROM artifact_manifests WHERE digest = ?3),
        UNIXEPOCH());"#,
        )
        .bind(artifact_uuid)
        .bind(class_name)
        .bind(manifest_digest.as_slice())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn remove_tag_if_exists(&self, artifact_uuid: Uuid) -> Result<()> {
        let rows = sqlx::query(r#"DELETE FROM artifact_tags WHERE tag = ?1;"#)
            .bind(artifact_uuid)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if rows != 0 {
            info!("Removed tag: {}", artifact_uuid);
        }
        Ok(())
    }

    async fn get_artifact_items(&self, artifact_uuid: Uuid) -> Result<Vec<ArtifactItemInfo>> {
        let mut t = self.pool.begin().await?;
        let (manifest_id, mani_digest, mani_size): (i64, Vec<u8>, i64) = sqlx::query_as(
            r#"
SELECT AM.id, AM.digest, AM.size FROM artifact_tags AS AT
    JOIN artifact_manifests AS AM ON AT.manifest_id = AM.id
    WHERE tag = ?1"#,
        )
        .bind(artifact_uuid)
        .fetch_one(&mut t)
        .await?;

        let mut res: Vec<ArtifactItemInfo> = sqlx::query_as::<_, (Vec<u8>, i64)>(
            r#"
SELECT AB.digest, AB.size FROM artifact_blob_usage AS ABU
    JOIN artifact_blob_links AS ABL ON ABU.link_id = ABL.id 
    JOIN artifact_blobs AS AB ON ABL.blob_id = AB.id
    WHERE ABU.manifest_id = ?1;"#,
        )
        .bind(manifest_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|(digest, size)| ArtifactItemInfo {
            id: format!("blobs/sha256:{}", hex::encode(&digest)),
            size: size.try_into().unwrap(),
            hash: Hashsum::Sha256(digest.try_into().unwrap()),
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
