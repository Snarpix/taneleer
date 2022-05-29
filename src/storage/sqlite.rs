use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use super::Storage;

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
    pub async fn new(
        path: &std::path::PathBuf,
    ) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
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

    async fn get_db_version(&self) -> Result<u32, Box<dyn std::error::Error>> {
        let res = sqlx::query_as::<_, (u32,)>("PRAGMA user_version;")
            .fetch_one(&self.pool)
            .await?;
        Ok(res.0)
    }

    async fn update_db_version(&self) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(&format!("PRAGMA user_version = {};", CURRENT_DB_VERSION))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn migrate_0(&self) -> Result<(), Box<dyn std::error::Error>> {
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
    name TEXT
);
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

impl Storage for SqliteStorage {}
