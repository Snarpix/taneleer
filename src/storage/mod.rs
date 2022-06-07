mod sqlite;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    class::{ArtifactClassData, ArtifactType},
    config::storage::{ConfigSqliteStorage, ConfigStorage},
    error::Result,
    source::Source,
};

#[async_trait]
pub trait Storage {
    #[must_use]
    async fn create_uninit_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()>;

    #[must_use]
    async fn commit_class_init(&mut self, name: &str) -> Result<()>;

    #[must_use]
    async fn remove_uninit_class(&mut self, name: &str) -> Result<()>;

    #[must_use]
    async fn get_classes(&self) -> Result<Vec<String>>;

    #[must_use]
    async fn reserve_artifact(
        &mut self,
        artifact_uuid: Uuid,
        class_name: &str,
        sources: &[(String, Source)],
    ) -> Result<(String, ArtifactType)>;

    #[must_use]
    async fn commit_artifact_reservation(&mut self, artifact_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn rollback_artifact_reserve(&mut self, artifact_uuid: Uuid) -> Result<()>;
}

pub async fn from_config(config: &ConfigStorage) -> Result<Box<dyn Storage + Send + Sync>> {
    match config {
        ConfigStorage::Sqlite(ConfigSqliteStorage { path }) => {
            Ok(Box::new(sqlite::SqliteStorage::new(path).await?))
        }
    }
}
