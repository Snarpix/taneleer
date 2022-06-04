use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use super::Storage;
use crate::class::ArtifactClassData;
use crate::error::Result;

#[derive(Debug)]
pub enum SqliteError {
    InvalidVersion,
    NotFound,
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
);
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

    async fn mark_class_init(&mut self, name: &str) -> Result<()> {
        let rows = sqlx::query(r#"UPDATE artifact_classes SET state = 1 WHERE name = ?1;"#)
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
}
