use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

use super::Storage;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::error::Result;
use crate::source::{Hashsum, Source};

#[derive(Debug)]
pub enum SqliteError {
    InvalidVersion,
    NotFound,
    InvalidArtifactType,
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
    name TEXT NOT NULL UNIQUE,
    backend TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    state INTEGER NOT NULL DEFAULT 0
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifacts;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifacts(
    id INTEGER PRIMARY KEY,
    uuid BLOB NOT NULL UNIQUE CHECK (LENGTH(uuid) = 16),
    class_id INTEGER NOT NULL REFERENCES artifact_classes(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    reserve_time INTEGER NOT NULL,
    commit_time INTEGER,
    use_count INTEGER NOT NULL DEFAULT 0,
    state INTEGER NOT NULL DEFAULT 0
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS external_sources;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE external_sources(
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    url TEXT NOT NULL,
    hash_type TEXT NOT NULL,
    hash BLOB NOT NULL
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS internal_sources;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE internal_sources(
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    name TEXT NOT NULL,
    source_artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT
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
    async fn create_uninit_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO artifact_classes(name, backend, artifact_type) VALUES (?1, ?2, ?3);"#,
        )
        .bind(name)
        .bind(&data.backend_name)
        .bind(data.art_type.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn commit_class_init(&mut self, name: &str) -> Result<()> {
        let rows =
            sqlx::query(r#"UPDATE artifact_classes SET state = 1 WHERE name = ?1 AND state = 0;"#)
                .bind(name)
                .execute(&self.pool)
                .await?
                .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        Ok(())
    }

    async fn remove_uninit_class(&mut self, name: &str) -> Result<()> {
        let rows = sqlx::query(r#"DELETE FROM artifact_classes WHERE name = ?1 AND state = 0;"#)
            .bind(name)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        Ok(())
    }

    async fn get_classes(&self) -> Result<Vec<String>> {
        sqlx::query_scalar::<_, String>(r#"SELECT name FROM artifact_classes;"#)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.into())
    }

    async fn reserve_artifact(
        &mut self,
        artifact_uuid: Uuid,
        class_name: &str,
        sources: &Vec<(String, Source)>,
    ) -> Result<(String, ArtifactType)> {
        let mut t = self.pool.begin().await?;
        let (artifact_class_id, backend_name, artifact_type): (i64, String, String) = sqlx::query_as(
            r#"SELECT id, backend, artifact_type FROM artifact_classes WHERE name = ?1 AND state = 1;"#,
        )
        .bind(class_name)
        .fetch_one(&mut t)
        .await?;
        let artifact_type: ArtifactType = artifact_type
            .parse()
            .map_err(|_| SqliteError::InvalidArtifactType)?;

        sqlx::query(
            r#"INSERT INTO artifacts(uuid, class_id, reserve_time) VALUES (?1, ?2, UNIXEPOCH());"#,
        )
        .bind(artifact_uuid.as_bytes().as_ref())
        .bind(artifact_class_id)
        .execute(&mut t)
        .await?;

        for (source_name, source_meta) in sources {
            match source_meta {
                Source::Artifact { uuid } => {
                    sqlx::query(
                        r#"
INSERT INTO internal_sources(artifact_id, name, source_artifact_id) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, (SELECT id FROM artifacts WHERE uuid = ?3));
                    "#,
                    )
                    .bind(artifact_uuid.as_bytes().as_ref())
                    .bind(source_name)
                    .bind(uuid.as_bytes().as_ref())
                    .execute(&mut t)
                    .await?;
                }
                Source::Url { url, hash } => match hash {
                    Hashsum::Sha256(hash) => {
                        sqlx::query(
                            r#"
INSERT INTO external_sources(artifact_id, name, type, url, hash_type, hash) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, "url", ?3, "sha256", ?4);
                            "#,
                        )
                        .bind(artifact_uuid.as_bytes().as_ref())
                        .bind(source_name)
                        .bind(url)
                        .bind(hash.as_ref())
                        .execute(&mut t)
                        .await?;
                    }
                },
                Source::Git { repo, commit } => {
                    sqlx::query(
                        r#"
INSERT INTO external_sources(artifact_id, name, type, url, hash_type, hash) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, "git", ?3, "sha1", ?4);
                    "#,
                    )
                    .bind(artifact_uuid.as_bytes().as_ref())
                    .bind(source_name)
                    .bind(repo)
                    .bind(commit.as_ref())
                    .execute(&mut t)
                    .await?;
                }
            }
        }
        t.commit().await?;
        Ok((backend_name, artifact_type))
    }

    async fn commit_artifact_reservation(&mut self, artifact_uuid: Uuid) -> Result<()> {
        let rows = sqlx::query(r#"UPDATE artifacts SET state = 1 WHERE uuid = ?1 AND state = 0;"#)
            .bind(artifact_uuid.as_bytes().as_ref())
            .execute(&self.pool)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        Ok(())
    }

    async fn rollback_artifact_reserve(&mut self, artifact_uuid: Uuid) -> Result<()> {
        let mut t = self.pool.begin().await?;
        sqlx::query(r#"DELETE FROM internal_sources WHERE artifact_id = (SELECT id FROM artifacts WHERE uuid = ?1 AND state = 0);"#)
        .bind(artifact_uuid.as_bytes().as_ref())
        .execute(&mut t)
        .await?;
        sqlx::query(r#"DELETE FROM external_sources WHERE artifact_id = (SELECT id FROM artifacts WHERE uuid = ?1 AND state = 0);"#)
        .bind(artifact_uuid.as_bytes().as_ref())
        .execute(&mut t)
        .await?;
        sqlx::query(r#"DELETE FROM artifacts WHERE uuid = ?1 AND state = 0;"#)
            .bind(artifact_uuid.as_bytes().as_ref())
            .execute(&mut t)
            .await?;
        t.commit().await?;
        Ok(())
    }
}
