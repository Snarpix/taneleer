use std::sync::Arc;

use log::warn;
use tokio::sync::broadcast::{self, Sender as BSender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::BroadcastStream;
use url::Url;
use uuid::Uuid;

use crate::artifact::{Artifact, ArtifactItem, ArtifactState};
use crate::backend_pack::Backends;
use crate::class::{ArtifactClassData, ArtifactClassState};
use crate::error::Result;
use crate::source::Source;
use crate::storage::Storage;
use crate::tag::{ArtifactTag, Tag};
use crate::usage::ArtifactUsage;

pub type SharedArtifactManager = Arc<Mutex<ArtifactManager>>;
pub type ManagerMessageStream = BroadcastStream<ManagerMessage>;

#[derive(Clone, Debug)]
pub enum ManagerMessage {
    NewClass(String),
    NewArtifact(String, Uuid, ArtifactState),
    ArtifactUpdate(String, Uuid, ArtifactState),
    RemoveArtifact(String, Uuid, ArtifactState),
}

pub struct ArtifactManager {
    storage: Box<dyn Storage + Send + Sync>,
    backends: Backends,
    message_broadcast: BSender<ManagerMessage>,
}

#[derive(Debug)]
pub enum ManagerError {
    BackendNotExists,
    ProxyNotExists,
}

impl std::fmt::Display for ManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ManagerError {}

impl ArtifactManager {
    pub fn new(storage: Box<dyn Storage + Send + Sync>, backends: Backends) -> ArtifactManager {
        let (message_broadcast, _) = broadcast::channel(16);
        ArtifactManager {
            storage,
            backends,
            message_broadcast,
        }
    }

    pub fn subscribe(&self) -> ManagerMessageStream {
        BroadcastStream::new(self.message_broadcast.subscribe())
    }

    pub async fn create_class(&mut self, name: String, data: ArtifactClassData) -> Result<()> {
        let backend = self
            .backends
            .get_mut(&data.backend_name)
            .ok_or(ManagerError::BackendNotExists)?;
        self.storage.create_uninit_class(&name, &data).await?;
        match backend.create_class(&name, &data).await {
            Ok(()) => (),
            Err(e) => {
                self.storage.remove_uninit_class(&name).await?;
                return Err(e);
            }
        }
        self.storage.commit_class_init(&name).await?;
        self.message_broadcast
            .send(ManagerMessage::NewClass(name))
            .ok();
        Ok(())
    }

    pub async fn get_init_stream(&self) -> Result<Vec<ManagerMessage>> {
        let classes = self
            .storage
            .get_classes()
            .await?
            .into_iter()
            .map(ManagerMessage::NewClass);
        let artifacts = self
            .storage
            .get_artifacts()
            .await?
            .into_iter()
            .map(|(class, uuid, state)| ManagerMessage::NewArtifact(class, uuid, state));
        Ok(classes.chain(artifacts).collect())
    }

    pub async fn get_artifact_classes(
        &self,
    ) -> Result<Vec<(String, ArtifactClassData, ArtifactClassState)>> {
        self.storage.get_classes_info().await
    }

    pub async fn get_artifacts_info(&self) -> Result<Vec<Artifact>> {
        self.storage.get_artifacts_info().await
    }

    pub async fn get_sources(&self) -> Result<Vec<(Uuid, Source)>> {
        self.storage.get_sources().await
    }

    pub async fn get_items(&self) -> Result<Vec<ArtifactItem>> {
        self.storage.get_items().await
    }

    pub async fn get_tags(&self) -> Result<Vec<ArtifactTag>> {
        self.storage.get_tags().await
    }

    pub async fn get_usages(&self) -> Result<Vec<ArtifactUsage>> {
        self.storage.get_usages().await
    }

    pub async fn reserve_artifact(
        &mut self,
        class_name: String,
        sources: Vec<Source>,
        tags: Vec<Tag>,
        proxy: Option<String>,
    ) -> Result<(Uuid, Url)> {
        let artifact_uuid = Uuid::new_v4();
        let (backend_name, artifact_type) = self
            .storage
            .begin_reserve_artifact(artifact_uuid, &class_name, &sources, &tags)
            .await?;
        let res = async {
            let backend = self
                .backends
                .get_mut(&backend_name)
                .ok_or(ManagerError::BackendNotExists)?;
            backend
                .reserve_artifact(proxy.as_deref(), &class_name, artifact_type, artifact_uuid)
                .await
        }
        .await;
        match res {
            Ok(url) => {
                self.storage.commit_artifact_reserve(artifact_uuid).await?;
                self.message_broadcast
                    .send(ManagerMessage::NewArtifact(
                        class_name,
                        artifact_uuid,
                        ArtifactState::Reserved,
                    ))
                    .ok();
                Ok((artifact_uuid, url))
            }
            Err(e) => {
                warn!("Error during creating reserve: {:?}", &e);
                self.storage
                    .rollback_artifact_reserve(artifact_uuid)
                    .await?;
                Err(e)
            }
        }
    }

    pub async fn commit_artifact_reserve(
        &mut self,
        artifact_uuid: Uuid,
        tags: Vec<Tag>,
    ) -> Result<()> {
        let (class_name, backend_name, artifact_type) = self
            .storage
            .begin_artifact_commit(artifact_uuid, &tags)
            .await?;
        let res = async {
            let backend = self
                .backends
                .get_mut(&backend_name)
                .ok_or(ManagerError::BackendNotExists)?;
            backend
                .commit_artifact(&class_name, artifact_type, artifact_uuid)
                .await
        }
        .await;
        match res {
            Ok(artifact_items) => {
                self.storage
                    .commit_artifact_commit(artifact_uuid, artifact_items)
                    .await?;
                self.message_broadcast
                    .send(ManagerMessage::ArtifactUpdate(
                        class_name,
                        artifact_uuid,
                        ArtifactState::Committed,
                    ))
                    .ok();
                Ok(())
            }
            Err(e) => {
                self.storage.fail_artifact_commit(artifact_uuid).await?;
                Err(e)
            }
        }
    }

    pub async fn abort_artifact_reserve(&mut self, _artifact_uuid: Uuid) -> Result<()> {
        todo!();
    }

    pub async fn get_artifact(
        &mut self,
        artifact_uuid: Uuid,
        proxy: Option<String>,
    ) -> Result<(Uuid, Url)> {
        let (artifact_usage_uuid, class_name, backend_name, artifact_type) =
            self.storage.get_artifact(artifact_uuid).await?;
        let res = async {
            let backend = self
                .backends
                .get_mut(&backend_name)
                .ok_or(ManagerError::BackendNotExists)?;
            backend
                .get_artifact(proxy.as_deref(), &class_name, artifact_type, artifact_uuid)
                .await
        }
        .await;
        match res {
            Ok(url) => Ok((artifact_usage_uuid, url)),
            Err(e) => {
                self.storage
                    .release_artifact_usage(artifact_usage_uuid)
                    .await?;
                Err(e)
            }
        }
    }
}
