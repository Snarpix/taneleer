use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

use super::Storage;
use crate::artifact::{ArtifactItemInfo, ArtifactState};
use crate::class::{ArtifactClassData, ArtifactClassState, ArtifactType};
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
    state INTEGER NOT NULL
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
    state INTEGER NOT NULL,
    next_state INTEGER,
    error TEXT
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

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_items;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_items(
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    identifier TEXT NOT NULL,
    size INTEGER NOT NULL,
    hash_type TEXT NOT NULL,
    hash BLOB NOT NULL,
    UNIQUE(artifact_id, identifier)
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
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    name TEXT NOT NULL,
    value TEXT,
    UNIQUE(artifact_id, name)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP TABLE IF EXISTS artifact_usage;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE TABLE artifact_usage(
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    uuid BLOB NOT NULL UNIQUE CHECK (LENGTH(uuid) = 16),
    reserve_time INTEGER NOT NULL
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
            r#"INSERT INTO artifact_classes(name, backend, artifact_type, state) VALUES (?1, ?2, ?3, ?4);"#,
        )
        .bind(name)
        .bind(&data.backend_name)
        .bind(data.art_type.to_string())
        .bind(ArtifactClassState::Uninit)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn commit_class_init(&mut self, name: &str) -> Result<()> {
        let rows = sqlx::query(
            r#"UPDATE artifact_classes SET state = ?1 WHERE name = ?2 AND state = ?3;"#,
        )
        .bind(ArtifactClassState::Init)
        .bind(name)
        .bind(ArtifactClassState::Uninit)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        Ok(())
    }

    async fn remove_uninit_class(&mut self, name: &str) -> Result<()> {
        let rows = sqlx::query(r#"DELETE FROM artifact_classes WHERE name = ?1 AND state = ?2;"#)
            .bind(name)
            .bind(ArtifactClassState::Init)
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

    async fn get_artifacts(&self) -> Result<Vec<(String, Uuid, ArtifactState)>> {
        sqlx::query_as::<_, (String, Uuid, ArtifactState)>(r#"SELECT AC.name, A.uuid, A.state FROM artifacts AS A JOIN artifact_classes AS AC ON A.class_id = AC.id;"#)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn begin_reserve_artifact(
        &mut self,
        artifact_uuid: Uuid,
        class_name: &str,
        sources: &[(String, Source)],
        tags: &[(String, Option<String>)],
    ) -> Result<(String, ArtifactType)> {
        let mut t = self.pool.begin().await?;
        let (artifact_class_id, backend_name, artifact_type): (i64, String, String) = sqlx::query_as(
            r#"SELECT id, backend, artifact_type FROM artifact_classes WHERE name = ?1 AND state = ?2;"#,
        )
        .bind(class_name)
        .bind(ArtifactClassState::Init)
        .fetch_one(&mut t)
        .await?;
        let artifact_type: ArtifactType = artifact_type
            .parse()
            .map_err(|_| SqliteError::InvalidArtifactType)?;

        sqlx::query(
            r#"INSERT INTO artifacts(uuid, class_id, reserve_time, state, next_state) VALUES (?1, ?2, UNIXEPOCH(), ?3, ?4);"#,
        )
        .bind(artifact_uuid.as_bytes().as_ref())
        .bind(artifact_class_id)
        .bind(ArtifactState::Created)
        .bind(ArtifactState::Reserved)
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
        for (tag_name, tag_value) in tags {
            sqlx::query(
                r#"
INSERT INTO artifact_tags(artifact_id, name, value) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, ?3);
            "#,
            )
            .bind(artifact_uuid.as_bytes().as_ref())
            .bind(tag_name)
            .bind(tag_value)
            .execute(&mut t)
            .await?;
        }
        t.commit().await?;
        Ok((backend_name, artifact_type))
    }

    async fn commit_artifact_reserve(&mut self, artifact_uuid: Uuid) -> Result<()> {
        let rows = sqlx::query(r#"UPDATE artifacts SET state = ?1, next_state = NULL WHERE uuid = ?2 AND state = ?3 AND next_state = ?4;"#)
            .bind(ArtifactState::Reserved)
            .bind(artifact_uuid.as_bytes().as_ref())
            .bind(ArtifactState::Created)
            .bind(ArtifactState::Reserved)
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

        let artifact_id = sqlx::query_scalar::<_, i64>(
            r#"SELECT id FROM artifacts WHERE uuid = ?1 AND state = ?2 AND next_state = ?3;"#,
        )
        .bind(artifact_uuid)
        .bind(ArtifactState::Created)
        .bind(ArtifactState::Reserved)
        .fetch_one(&mut t)
        .await?;

        sqlx::query(r#"DELETE FROM internal_sources WHERE artifact_id = ?1;"#)
            .bind(artifact_id)
            .execute(&mut t)
            .await?;
        sqlx::query(r#"DELETE FROM external_sources WHERE artifact_id = ?1;"#)
            .bind(artifact_id)
            .execute(&mut t)
            .await?;
        sqlx::query(r#"DELETE FROM artifact_tags WHERE artifact_id = ?1;"#)
            .bind(artifact_id)
            .execute(&mut t)
            .await?;
        let rows = sqlx::query(r#"DELETE FROM artifacts WHERE id = ?1;"#)
            .bind(artifact_id)
            .execute(&mut t)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        t.commit().await?;
        Ok(())
    }

    async fn begin_artifact_commit(
        &mut self,
        artifact_uuid: Uuid,
        tags: &[(String, Option<String>)],
    ) -> Result<(String, String, ArtifactType)> {
        let mut t = self.pool.begin().await?;
        let (artifact_class_name, backend_name, artifact_type): (String, String, ArtifactType) =
            sqlx::query_as(
                r#"
SELECT AC.name, AC.backend, AC.artifact_type 
FROM artifacts AS A 
JOIN artifact_classes AS AC ON A.class_id = AC.id 
WHERE A.uuid = ?1 AND A.state = ?2 AND A.next_state IS NULL;"#,
            )
            .bind(artifact_uuid)
            .bind(ArtifactState::Reserved)
            .fetch_one(&mut t)
            .await?;

        for (tag_name, tag_value) in tags {
            if let Err(e) = sqlx::query(
                r#"
INSERT INTO artifact_tags(artifact_id, name, value) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, ?3);
            "#,
            )
            .bind(artifact_uuid)
            .bind(tag_name)
            .bind(tag_value)
            .execute(&mut t)
            .await
            {
                if let sqlx::Error::Database(e) = &e {
                    if e.code().map(|code| code == "2067").unwrap_or(false) {
                        // Tag exists, update value
                        sqlx::query(
                            r#"
UPDATE artifact_tags SET value = ?1 
WHERE artifact_id = (SELECT id FROM artifacts WHERE uuid = ?2) AND name = ?3;
                        "#,
                        )
                        .bind(tag_value)
                        .bind(artifact_uuid)
                        .bind(tag_name)
                        .execute(&mut t)
                        .await?;
                        continue;
                    }
                }
                return Err(e.into());
            }
        }

        let rows = sqlx::query(r#"UPDATE artifacts SET next_state = ?1 WHERE uuid = ?2 AND state = ?3 AND next_state IS NULL;"#)
            .bind(ArtifactState::Committed)
            .bind(artifact_uuid)
            .bind(ArtifactState::Reserved)
            .execute(&mut t)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }

        t.commit().await?;
        Ok((artifact_class_name, backend_name, artifact_type))
    }

    async fn commit_artifact_commit(
        &mut self,
        artifact_uuid: Uuid,
        artifact_items: Vec<ArtifactItemInfo>,
    ) -> Result<()> {
        let mut t = self.pool.begin().await?;
        let artifact_id = sqlx::query_scalar::<_, i64>(
            r#"SELECT id FROM artifacts WHERE uuid = ?1 AND state = ?2 AND next_state = ?3;"#,
        )
        .bind(artifact_uuid)
        .bind(ArtifactState::Reserved)
        .bind(ArtifactState::Committed)
        .fetch_one(&mut t)
        .await?;

        for item in artifact_items {
            match item.hash {
                Hashsum::Sha256(hash) => {
                    sqlx::query(
                        r#"
INSERT INTO artifact_items(artifact_id, identifier, size, hash_type, hash) 
VALUES (?1, ?2, ?3, "sha256", ?4);
                        "#,
                    )
                    .bind(artifact_id)
                    .bind(item.id)
                    .bind(item.size as i64)
                    .bind(hash.as_ref())
                    .execute(&mut t)
                    .await?;
                }
            }
        }
        let rows = sqlx::query(r#"UPDATE artifacts SET state = ?1, next_state = NULL, commit_time = UNIXEPOCH() WHERE id = ?2 AND state = ?3 AND next_state = ?4;"#)
            .bind(ArtifactState::Committed)
            .bind(artifact_id)
            .bind(ArtifactState::Reserved)
            .bind(ArtifactState::Committed)
            .execute(&mut t)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        t.commit().await?;
        Ok(())
    }

    async fn fail_artifact_commit(&mut self, artifact_uuid: Uuid) -> Result<()> {
        let rows = sqlx::query(r#"UPDATE artifacts SET error = ?1 WHERE uuid = ?2 AND state = ?3 AND next_state = ?4;"#)
            .bind("commit_failed: none")
            .bind(artifact_uuid)
            .bind(ArtifactState::Reserved)
            .bind(ArtifactState::Committed)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        Ok(())
    }

    async fn get_artifact(
        &mut self,
        artifact_uuid: Uuid,
    ) -> Result<(Uuid, String, String, ArtifactType)> {
        let mut t = self.pool.begin().await?;

        let (artifact_id, artifact_class_name, backend_name, artifact_type): (
            i64,
            String,
            String,
            ArtifactType,
        ) = sqlx::query_as(
            r#"
SELECT A.id, AC.name, AC.backend, AC.artifact_type 
FROM artifacts AS A 
JOIN artifact_classes AS AC ON A.class_id = AC.id 
WHERE A.uuid = ?1 AND A.state = ?2 AND A.next_state IS NULL;"#,
        )
        .bind(artifact_uuid)
        .bind(ArtifactState::Committed)
        .fetch_one(&mut t)
        .await?;

        let artifact_use_uuid = Uuid::new_v4();
        sqlx::query(
            r#"
INSERT INTO artifact_usage(artifact_id, uuid, reserve_time) 
VALUES (?1, ?2, UNIXEPOCH());
            "#,
        )
        .bind(artifact_id)
        .bind(artifact_use_uuid)
        .execute(&mut t)
        .await?;

        t.commit().await?;

        Ok((
            artifact_use_uuid,
            artifact_class_name,
            backend_name,
            artifact_type,
        ))
    }

    async fn release_artifact_usage(&mut self, artifact_usage_uuid: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM artifact_usage WHERE uuid = ?1;
            "#,
        )
        .bind(artifact_usage_uuid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
