mod sqlite;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    artifact::{Artifact, ArtifactItemInfo, ArtifactState},
    class::{ArtifactClassData, ArtifactClassState, ArtifactType},
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
    async fn get_classes_info(
        &self,
    ) -> Result<Vec<(String, ArtifactClassData, ArtifactClassState)>>;

    #[must_use]
    async fn get_artifacts(&self) -> Result<Vec<(String, Uuid, ArtifactState)>>;

    #[must_use]
    async fn get_artifacts_info(&self) -> Result<Vec<Artifact>>;

    #[must_use]
    async fn get_sources(&self) -> Result<Vec<(Uuid, Source)>>;

    #[must_use]
    async fn begin_reserve_artifact(
        &mut self,
        artifact_uuid: Uuid,
        class_name: &str,
        sources: &[Source],
        tags: &[(String, Option<String>)],
    ) -> Result<(String, ArtifactType)>;

    #[must_use]
    async fn commit_artifact_reserve(&mut self, artifact_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn rollback_artifact_reserve(&mut self, artifact_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn begin_artifact_commit(
        &mut self,
        artifact_uuid: Uuid,
        tags: &[(String, Option<String>)],
    ) -> Result<(String, String, ArtifactType)>;

    #[must_use]
    async fn commit_artifact_commit(
        &mut self,
        artifact_uuid: Uuid,
        artifact_items: Vec<ArtifactItemInfo>,
    ) -> Result<()>;

    #[must_use]
    async fn fail_artifact_commit(&mut self, artifact_uuid: Uuid) -> Result<()>;

    #[must_use]
    async fn get_artifact(
        &mut self,
        artifact_uuid: Uuid,
    ) -> Result<(Uuid, String, String, ArtifactType)>;

    #[must_use]
    async fn release_artifact_usage(&mut self, artifact_usage_uuid: Uuid) -> Result<()>;
}

pub async fn from_config(config: &ConfigStorage) -> Result<Box<dyn Storage + Send + Sync>> {
    match config {
        ConfigStorage::Sqlite(ConfigSqliteStorage { path }) => {
            Ok(Box::new(sqlite::SqliteStorage::new(path).await?))
        }
    }
}
