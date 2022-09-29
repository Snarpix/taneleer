use std::collections::HashMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

use super::Storage;
use crate::artifact::{Artifact, ArtifactData, ArtifactItem, ArtifactItemInfo, ArtifactState};
use crate::class::{ArtifactClass, ArtifactClassData, ArtifactClassState, ArtifactType};
use crate::error::Result;
use crate::source::{Hashsum, Source, SourceType, SourceTypeDiscriminants};
use crate::tag::{ArtifactTag, Tag};
use crate::usage::{ArtifactUsage, Usage};

#[derive(Debug)]
pub enum SqliteError {
    InvalidVersion,
    NotFound,
    InvalidArtifactType,
    InvalidExternalSourceType,
    InvalidHashType,
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
    hash BLOB NOT NULL,
    UNIQUE(artifact_id, name)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP INDEX IF EXISTS external_sources_hash_idx;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE INDEX external_sources_hash_idx ON external_sources(hash);
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
    source_artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    UNIQUE(artifact_id, name)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP INDEX IF EXISTS internal_sources_src_idx;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE INDEX internal_sources_src_idx ON internal_sources(source_artifact_id);
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
DROP INDEX IF EXISTS artifact_items_hash_idx;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE INDEX artifact_items_hash_idx ON artifact_items(hash);
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
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    name TEXT NOT NULL,
    value TEXT,
    UNIQUE(name, artifact_id)
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP INDEX IF EXISTS artifact_tags_artifact_id_idx;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE INDEX artifact_tags_artifact_id_idx ON artifact_tags(artifact_id, name);
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
    id INTEGER PRIMARY KEY,
    artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    uuid BLOB NOT NULL UNIQUE CHECK (LENGTH(uuid) = 16),
    reserve_time INTEGER NOT NULL
) STRICT;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
DROP INDEX IF EXISTS artifact_usage_artifact_id_idx;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
CREATE INDEX artifact_usage_artifact_id_idx ON artifact_usage(artifact_id);
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

fn parse_ext_source_type(
    typ: &str,
    url: String,
    hash_type: &str,
    hash: &[u8],
) -> Result<SourceType> {
    let typ: SourceTypeDiscriminants = typ.parse()?;
    match typ {
        SourceTypeDiscriminants::Url => Ok(SourceType::Url {
            url,
            hash: Hashsum::from_split(hash_type, hash)?,
        }),
        SourceTypeDiscriminants::Git => {
            if hash_type != "sha1" {
                return Err(SqliteError::InvalidHashType.into());
            }
            Ok(SourceType::Git {
                repo: url,
                commit: hash.try_into().map_err(|_| SqliteError::InvalidHashType)?,
            })
        }
        _ => Err(SqliteError::InvalidExternalSourceType.into()),
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
            .bind(ArtifactClassState::Uninit)
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

    async fn get_classes_info(&self) -> Result<Vec<ArtifactClass>> {
        Ok(
            sqlx::query_as(r#"SELECT name, backend AS backend_name, artifact_type AS art_type, state FROM artifact_classes;"#)
                .fetch_all(&self.pool)
                .await?
        )
    }

    async fn get_artifacts(&self) -> Result<Vec<(String, Uuid, ArtifactState)>> {
        sqlx::query_as::<_, (String, Uuid, ArtifactState)>(r#"SELECT AC.name, A.uuid, A.state FROM artifacts AS A JOIN artifact_classes AS AC ON A.class_id = AC.id;"#)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_artifacts_info(&self) -> Result<Vec<Artifact>> {
        sqlx::query_as::<_, Artifact>(
            r#"
            SELECT A.uuid AS uuid, AC.name AS class_name, AC.artifact_type AS art_type,
                reserve_time, commit_time, use_count, A.state AS state, next_state, error
                FROM artifacts AS A JOIN artifact_classes AS AC ON A.class_id = AC.id;"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_artifact_info(&self, artifact_uuid: Uuid) -> Result<ArtifactData> {
        sqlx::query_as::<_, ArtifactData>(
            r#"
            SELECT AC.name AS class_name, AC.artifact_type AS art_type,
                reserve_time, commit_time, use_count, A.state AS state, next_state, error
                FROM artifacts AS A JOIN artifact_classes AS AC ON A.class_id = AC.id
                WHERE A.uuid = ?1;"#,
        )
        .bind(artifact_uuid)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_sources(&self) -> Result<Vec<(Uuid, Source)>> {
        let mut res = Vec::new();
        while let Some((artifact_uuid, name, typ, url, hash_type, hash)) =
            sqlx::query_as::<_, (Uuid, String, String, String, String, Vec<u8>)>(
                r#"
                SELECT A.uuid, name, type, url, hash_type, hash
                    FROM external_sources AS ES JOIN artifacts AS A ON ES.artifact_id = A.id;"#,
            )
            .fetch(&self.pool)
            .try_next()
            .await?
        {
            let source = parse_ext_source_type(&typ, url, &hash_type, &hash)?;
            res.push((artifact_uuid, Source { name, source }));
        }

        while let Some((artifact_uuid, name, target_uuid)) =
            sqlx::query_as::<_, (Uuid, String, Uuid)>(
                r#"
            SELECT A.uuid, name, TA.uuid
                FROM internal_sources AS ISRC
                JOIN artifacts AS A ON ISRC.artifact_id = A.id
                JOIN artifacts AS TA ON ISRC.source_artifact_id = TA.id;"#,
            )
            .fetch(&self.pool)
            .try_next()
            .await?
        {
            res.push((
                artifact_uuid,
                Source {
                    name,
                    source: SourceType::Artifact { uuid: target_uuid },
                },
            ));
        }

        Ok(res)
    }

    async fn get_artifact_sources(&self, artifact_uuid: Uuid) -> Result<Vec<Source>> {
        let mut res = Vec::new();
        while let Some((name, typ, url, hash_type, hash)) =
            sqlx::query_as::<_, (String, String, String, String, Vec<u8>)>(
                r#"
                SELECT name, type, url, hash_type, hash
                    FROM external_sources AS ES JOIN artifacts AS A ON ES.artifact_id = A.id
                    WHERE A.uuid = ?1;"#,
            )
            .bind(artifact_uuid)
            .fetch(&self.pool)
            .try_next()
            .await?
        {
            let source = parse_ext_source_type(&typ, url, &hash_type, &hash)?;
            res.push(Source { name, source });
        }

        while let Some((name, target_uuid)) = sqlx::query_as::<_, (String, Uuid)>(
            r#"
            SELECT name, TA.uuid
                FROM internal_sources AS ISRC
                JOIN artifacts AS A ON ISRC.artifact_id = A.id
                JOIN artifacts AS TA ON ISRC.source_artifact_id = TA.id
                WHERE A.uuid = ?1;"#,
        )
        .bind(artifact_uuid)
        .fetch(&self.pool)
        .try_next()
        .await?
        {
            res.push(Source {
                name,
                source: SourceType::Artifact { uuid: target_uuid },
            });
        }

        Ok(res)
    }

    async fn get_items(&self) -> Result<Vec<ArtifactItem>> {
        sqlx::query_as::<_, ArtifactItem>(
            r#"
            SELECT A.uuid AS uuid, identifier AS id, size, hash_type, hash
                FROM artifact_items AS AI JOIN artifacts AS A ON AI.artifact_id = A.id;"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_artifact_items(&self, artifact_uuid: Uuid) -> Result<Vec<ArtifactItemInfo>> {
        sqlx::query_as::<_, ArtifactItemInfo>(
            r#"
            SELECT identifier AS id, size, hash_type, hash
                FROM artifact_items AS AI JOIN artifacts AS A ON AI.artifact_id = A.id
                WHERE A.uuid = ?1;"#,
        )
        .bind(artifact_uuid)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_tags(&self) -> Result<Vec<ArtifactTag>> {
        sqlx::query_as::<_, ArtifactTag>(
            r#"
            SELECT A.uuid AS artifact_uuid, name, value
                FROM artifact_tags AS AT JOIN artifacts AS A ON AT.artifact_id = A.id;"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_artifact_tags(&self, artifact_uuid: Uuid) -> Result<Vec<Tag>> {
        sqlx::query_as::<_, Tag>(
            r#"
            SELECT name, value
                FROM artifact_tags AS AT JOIN artifacts AS A ON AT.artifact_id = A.id
                WHERE A.uuid = ?1;"#,
        )
        .bind(artifact_uuid)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_usages(&self) -> Result<Vec<ArtifactUsage>> {
        sqlx::query_as::<_, ArtifactUsage>(
            r#"
            SELECT A.uuid AS artifact_uuid, AU.uuid AS uuid, AU.reserve_time AS reserve_time
                FROM artifact_usage AS AU JOIN artifacts AS A ON AU.artifact_id = A.id;"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn get_artifact_usages(&self, artifact_uuid: Uuid) -> Result<Vec<Usage>> {
        sqlx::query_as::<_, Usage>(
            r#"
            SELECT AU.uuid AS uuid, AU.reserve_time AS reserve_time
                FROM artifact_usage AS AU JOIN artifacts AS A ON AU.artifact_id = A.id
                WHERE A.uuid = ?1;"#,
        )
        .bind(artifact_uuid)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    async fn begin_reserve_artifact(
        &mut self,
        artifact_uuid: Uuid,
        class_name: &str,
        sources: &[Source],
        tags: &[Tag],
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

        for Source { name, source } in sources {
            match source {
                SourceType::Artifact { uuid } => {
                    sqlx::query(
                        r#"
INSERT INTO internal_sources(artifact_id, name, source_artifact_id) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, (SELECT id FROM artifacts WHERE uuid = ?3));
                    "#,
                    )
                    .bind(artifact_uuid.as_bytes().as_ref())
                    .bind(name)
                    .bind(uuid.as_bytes().as_ref())
                    .execute(&mut t)
                    .await?;
                }
                SourceType::Url { url, hash } => match hash {
                    Hashsum::Sha256(hash) => {
                        sqlx::query(
                            r#"
INSERT INTO external_sources(artifact_id, name, type, url, hash_type, hash) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, "url", ?3, "sha256", ?4);
                            "#,
                        )
                        .bind(artifact_uuid.as_bytes().as_ref())
                        .bind(name)
                        .bind(url)
                        .bind(hash.as_ref())
                        .execute(&mut t)
                        .await?;
                    }
                },
                SourceType::Git { repo, commit } => {
                    sqlx::query(
                        r#"
INSERT INTO external_sources(artifact_id, name, type, url, hash_type, hash) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, "git", ?3, "sha1", ?4);
                    "#,
                    )
                    .bind(artifact_uuid.as_bytes().as_ref())
                    .bind(name)
                    .bind(repo)
                    .bind(commit.as_ref())
                    .execute(&mut t)
                    .await?;
                }
            }
        }
        for Tag { name, value } in tags {
            sqlx::query(
                r#"
INSERT INTO artifact_tags(artifact_id, name, value) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, ?3);
            "#,
            )
            .bind(artifact_uuid.as_bytes().as_ref())
            .bind(name)
            .bind(value)
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

    #[must_use]
    async fn begin_reserve_abort(
        &mut self,
        artifact_uuid: Uuid,
    ) -> Result<(String, String, ArtifactType)> {
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
        .bind(ArtifactState::Reserved)
        .fetch_one(&mut t)
        .await?;
        let rows = sqlx::query(r#"UPDATE artifacts SET next_state = ?1 WHERE id = ?2 AND state = ?3 AND next_state IS NULL;"#)
            .bind(ArtifactState::Removed)
            .bind(artifact_id)
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

    #[must_use]
    async fn commit_reserve_abort(&mut self, artifact_uuid: Uuid) -> Result<()> {
        let mut t = self.pool.begin().await?;

        let artifact_id = sqlx::query_scalar::<_, i64>(
            r#"SELECT id FROM artifacts WHERE uuid = ?1 AND state = ?2 AND next_state = ?3;"#,
        )
        .bind(artifact_uuid)
        .bind(ArtifactState::Reserved)
        .bind(ArtifactState::Removed)
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

    #[must_use]
    async fn fail_reserve_abort(&mut self, artifact_uuid: Uuid, error: &str) -> Result<()> {
        let rows = sqlx::query(r#"UPDATE artifacts SET error = ?1 WHERE uuid = ?2 AND state = ?3 AND next_state = ?4;"#)
            .bind(error)
            .bind(artifact_uuid)
            .bind(ArtifactState::Reserved)
            .bind(ArtifactState::Removed)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if rows != 1 {
            return Err(SqliteError::NotFound.into());
        }
        Ok(())
    }

    async fn begin_artifact_commit(
        &mut self,
        artifact_uuid: Uuid,
        tags: &[Tag],
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

        for Tag { name, value } in tags {
            if let Err(e) = sqlx::query(
                r#"
INSERT INTO artifact_tags(artifact_id, name, value) 
VALUES ((SELECT id FROM artifacts WHERE uuid = ?1), ?2, ?3);
            "#,
            )
            .bind(artifact_uuid)
            .bind(name)
            .bind(value)
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
                        .bind(value)
                        .bind(artifact_uuid)
                        .bind(name)
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

    async fn fail_artifact_commit(&mut self, artifact_uuid: Uuid, error: &str) -> Result<()> {
        let rows = sqlx::query(r#"UPDATE artifacts SET error = ?1 WHERE uuid = ?2 AND state = ?3 AND next_state = ?4;"#)
            .bind(error)
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

    async fn use_artifact(
        &mut self,
        artifact_uuid: Uuid,
    ) -> Result<(Uuid, String, String, ArtifactType)> {
        let mut t = self.pool.begin().await?;

        let (artifact_id, use_count, artifact_class_name, backend_name, artifact_type): (
            i64,
            i64,
            String,
            String,
            ArtifactType,
        ) = sqlx::query_as(
            r#"
            SELECT A.id, A.use_count, AC.name, AC.backend, AC.artifact_type
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

        sqlx::query(r#"UPDATE artifacts SET use_count = ?1 WHERE id = ?2;"#)
            .bind(use_count + 1)
            .bind(artifact_id)
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

    async fn find_last_artifact(
        &mut self,
        class_name: &str,
        sources: &[Source],
        tags: &[Tag],
    ) -> Result<Uuid> {
        let mut t = self.pool.begin().await?;

        let class_id: i64 = sqlx::query_scalar(
            r#"SELECT id FROM artifact_classes
            WHERE name = ?1 AND state = ?2;"#,
        )
        .bind(class_name)
        .bind(ArtifactClassState::Init)
        .fetch_one(&mut t)
        .await?;

        let mut res = None;

        for Tag { name, value } in tags {
            let first = res.is_none();
            let item = res.get_or_insert_with(HashMap::new);
            let mut stream = sqlx::query_as::<_, (i64, i64)>(
                r#"
                SELECT A.id, A.commit_time FROM artifacts AS A 
                    JOIN artifact_tags AS AT ON A.id = AT.artifact_id
                    WHERE AT.name = ?1
                        AND ((?2 IS NULL) OR (AT.value = ?2))
                        AND A.class_id = ?3
                        AND A.state = ?4 AND A.next_state IS NULL;"#,
            )
            .bind(name)
            .bind(value)
            .bind(class_id)
            .bind(ArtifactState::Committed)
            .fetch(&mut t);
            if first {
                while let Some((artifact_id, commit_time)) = stream.try_next().await? {
                    if item
                        .insert(artifact_id, (1u64, 0u64, commit_time))
                        .is_some()
                    {
                        panic!("Duplicate tag")
                    }
                }
            } else {
                while let Some((artifact_id, _)) = stream.try_next().await? {
                    if let Some(item) = item.get_mut(&artifact_id) {
                        item.0 += 1;
                    }
                }
            }
        }

        for Source { name, source } in sources {
            let first = res.is_none();
            let item = res.get_or_insert_with(HashMap::new);
            let mut stream = match source {
                SourceType::Artifact { uuid } => sqlx::query_as::<_, (i64, i64)>(
                    r#"
                    SELECT A.id, A.commit_time FROM artifacts AS A 
                        JOIN internal_sources AS InS ON A.id = InS.artifact_id
                        JOIN artifacts AS TA ON InS.source_artifact_id = TA.id
                        WHERE TA.uuid = ?1
                            AND InS.name = ?2
                            AND A.class_id = ?3
                            AND A.state = ?4 AND A.next_state IS NULL;"#,
                )
                .bind(uuid)
                .bind(name)
                .bind(class_id)
                .bind(ArtifactState::Committed)
                .fetch(&mut t),
                SourceType::Url { url, hash } => {
                    let Hashsum::Sha256(hash) = hash;
                    sqlx::query_as::<_, (i64, i64)>(
                        r#"
                        SELECT A.id, A.commit_time FROM artifacts AS A 
                            JOIN external_sources AS ES ON A.id = ES.artifact_id
                            WHERE ES.hash = ?1
                                AND ES.url = ?2
                                AND ES.name = ?3
                                AND ES.type = "url"
                                AND ES.hash_type = "sha256"
                                AND A.class_id = ?4
                                AND A.state = ?5 AND A.next_state IS NULL;"#,
                    )
                    .bind(hash.as_ref())
                    .bind(url)
                    .bind(name)
                    .bind(class_id)
                    .bind(ArtifactState::Committed)
                    .fetch(&mut t)
                }
                SourceType::Git { repo, commit } => sqlx::query_as::<_, (i64, i64)>(
                    r#"
                    SELECT A.id, A.commit_time FROM artifacts AS A 
                        JOIN external_sources AS ES ON A.id = ES.artifact_id
                        WHERE ES.hash = ?1
                            AND ES.url = ?2
                            AND ES.name = ?3
                            AND ES.type = "git"
                            AND ES.hash_type = "sha1"
                            AND A.class_id = ?4
                            AND A.state = ?5 AND A.next_state IS NULL;"#,
                )
                .bind(commit.as_ref())
                .bind(repo)
                .bind(name)
                .bind(class_id)
                .bind(ArtifactState::Committed)
                .fetch(&mut t),
            };
            if first {
                while let Some((artifact_id, commit_time)) = stream.try_next().await? {
                    if item
                        .insert(artifact_id, (0u64, 1u64, commit_time))
                        .is_some()
                    {
                        panic!("Duplicate src")
                    }
                }
            } else {
                while let Some((artifact_id, _)) = stream.try_next().await? {
                    if let Some(item) = item.get_mut(&artifact_id) {
                        item.1 += 1;
                    }
                }
            }
        }

        let resulting_id = match res {
            None => {
                sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT id FROM artifacts
                        WHERE class_id = ?1 AND state = ?2 AND next_state IS NULL
                        ORDER BY commit_time DESC
                        LIMIT 1;"#,
                )
                .bind(class_id)
                .bind(ArtifactState::Committed)
                .fetch_one(&mut t)
                .await?
            }
            Some(res) => {
                let mut max = i64::MIN;
                let mut max_id = None;
                for (artifact_id, (tag_count, src_count, commited_time)) in res {
                    if tag_count == tags.len() as u64
                        && src_count == sources.len() as u64
                        && commited_time >= max
                    {
                        max = commited_time;
                        max_id = Some(artifact_id);
                    }
                }
                if let Some(id) = max_id {
                    id
                } else {
                    return Err(SqliteError::NotFound.into());
                }
            }
        };

        let artifact_uuid =
            sqlx::query_scalar::<_, Uuid>(r#"SELECT uuid FROM artifacts WHERE id = ?1"#)
                .bind(resulting_id)
                .fetch_one(&mut t)
                .await?;

        t.commit().await?;

        Ok(artifact_uuid)
    }
}
