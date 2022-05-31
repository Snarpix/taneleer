mod sqlite;

use async_trait::async_trait;

use crate::{
    class::ArtifactClassData,
    config::storage::{ConfigSqliteStorage, ConfigStorage},
};

#[async_trait]
pub trait Storage {
    async fn create_class(
        &mut self,
        name: &str,
        data: &ArtifactClassData,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn from_config(
    config: &ConfigStorage,
) -> Result<Box<dyn Storage + Send + Sync>, Box<dyn std::error::Error>> {
    match config {
        ConfigStorage::Sqlite(ConfigSqliteStorage { path }) => {
            Ok(Box::new(sqlite::SqliteStorage::new(path).await?))
        }
    }
}
