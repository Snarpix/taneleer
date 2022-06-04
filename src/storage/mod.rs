mod sqlite;

use async_trait::async_trait;

use crate::{
    class::ArtifactClassData,
    config::storage::{ConfigSqliteStorage, ConfigStorage},
    error::Result,
};

#[async_trait]
pub trait Storage {
    #[must_use]
    async fn create_uninit_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()>;

    #[must_use]
    async fn mark_class_init(&mut self, name: &str) -> Result<()>;

    #[must_use]
    async fn remove_uninit_class(&mut self, name: &str) -> Result<()>;

    #[must_use]
    async fn get_classes(&self) -> Result<Vec<String>>;
}

pub async fn from_config(config: &ConfigStorage) -> Result<Box<dyn Storage + Send + Sync>> {
    match config {
        ConfigStorage::Sqlite(ConfigSqliteStorage { path }) => {
            Ok(Box::new(sqlite::SqliteStorage::new(path).await?))
        }
    }
}
