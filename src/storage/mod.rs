mod sqlite;

use crate::config::storage::{ConfigSqliteStorage, ConfigStorage};
pub trait Storage {}

pub async fn from_config(
    config: &ConfigStorage,
) -> Result<Box<dyn Storage + Send>, Box<dyn std::error::Error>> {
    match config {
        ConfigStorage::Sqlite(ConfigSqliteStorage { path }) => {
            Ok(Box::new(sqlite::SqliteStorage::new(path).await?))
        }
    }
}
