use std::sync::Arc;

use log::warn;
use tokio::sync::broadcast::{self, Sender as BSender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::BroadcastStream;
use url::Url;
use uuid::Uuid;

use crate::artifact::{Artifact, ArtifactData, ArtifactItem, ArtifactItemInfo, ArtifactState};
use crate::backend_pack::Backends;
use crate::class::{ArtifactClass, ArtifactClassData};
use crate::error::Result;
use crate::source::Source;
use crate::storage::Storage;
use crate::tag::{ArtifactTag, Tag};
use crate::usage::{ArtifactUsage, Usage};

pub type SharedArtifactManager = Arc<Mutex<ArtifactManager>>;
#[allow(dead_code)]
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
    InternalError,
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

    pub async fn get_artifact_classes(&self) -> Result<Vec<ArtifactClass>> {
        self.storage.get_classes_info().await
    }

    pub async fn get_artifacts_info(&self) -> Result<Vec<Artifact>> {
        self.storage.get_artifacts_info().await
    }

    pub async fn get_artifact_info(&self, artifact_uuid: Uuid) -> Result<ArtifactData> {
        self.storage.get_artifact_info(artifact_uuid).await
    }

    pub async fn get_sources(&self) -> Result<Vec<(Uuid, Source)>> {
        self.storage.get_sources().await
    }

    pub async fn get_artifact_sources(&self, artifact_uuid: Uuid) -> Result<Vec<Source>> {
        self.storage.get_artifact_sources(artifact_uuid).await
    }

    pub async fn get_items(&self) -> Result<Vec<ArtifactItem>> {
        self.storage.get_items().await
    }

    pub async fn get_artifact_items(&self, artifact_uuid: Uuid) -> Result<Vec<ArtifactItemInfo>> {
        self.storage.get_artifact_items(artifact_uuid).await
    }

    pub async fn get_tags(&self) -> Result<Vec<ArtifactTag>> {
        self.storage.get_tags().await
    }

    pub async fn get_artifact_tags(&self, artifact_uuid: Uuid) -> Result<Vec<Tag>> {
        self.storage.get_artifact_tags(artifact_uuid).await
    }

    pub async fn get_usages(&self) -> Result<Vec<ArtifactUsage>> {
        self.storage.get_usages().await
    }

    pub async fn get_artifact_usages(&self, artifact_uuid: Uuid) -> Result<Vec<Usage>> {
        self.storage.get_artifact_usages(artifact_uuid).await
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
                self.storage
                    .fail_artifact_commit(artifact_uuid, &format!("{:?}", e))
                    .await?;
                Err(e)
            }
        }
    }

    pub async fn abort_artifact_reserve(&mut self, artifact_uuid: Uuid) -> Result<()> {
        let (class_name, backend_name, artifact_type) =
            self.storage.begin_reserve_abort(artifact_uuid).await?;
        let res = async {
            let backend = self
                .backends
                .get_mut(&backend_name)
                .ok_or(ManagerError::BackendNotExists)?;
            backend
                .abort_reserve(&class_name, artifact_type, artifact_uuid)
                .await
        }
        .await;
        match res {
            Ok(()) => {
                self.storage.commit_reserve_abort(artifact_uuid).await?;
                self.message_broadcast
                    .send(ManagerMessage::RemoveArtifact(
                        class_name,
                        artifact_uuid,
                        ArtifactState::Reserved,
                    ))
                    .ok();
                Ok(())
            }
            Err(e) => {
                self.storage
                    .fail_reserve_abort(artifact_uuid, &format!("{:?}", e))
                    .await?;
                Err(e)
            }
        }
    }

    pub async fn use_artifact(
        &mut self,
        artifact_uuid: Uuid,
        proxy: Option<String>,
    ) -> Result<(Uuid, Url)> {
        let (artifact_usage_uuid, class_name, backend_name, artifact_type) =
            self.storage.use_artifact(artifact_uuid).await?;
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

    pub async fn find_last_artifact(
        &mut self,
        class_name: String,
        sources: Vec<Source>,
        tags: Vec<Tag>,
    ) -> Result<Uuid> {
        let artifact_uuid = self
            .storage
            .find_last_artifact(&class_name, &sources, &tags)
            .await?;
        Ok(artifact_uuid)
    }

    pub async fn use_last_artifact(
        &mut self,
        class_name: String,
        sources: Vec<Source>,
        tags: Vec<Tag>,
        proxy: Option<String>,
    ) -> Result<(Uuid, Uuid, Url)> {
        let artifact_uuid = self
            .storage
            .find_last_artifact(&class_name, &sources, &tags)
            .await?;
        let (artifact_usage_uuid, a_class_name, backend_name, artifact_type) =
            self.storage.use_artifact(artifact_uuid).await?;
        if class_name != a_class_name {
            return Err(ManagerError::InternalError.into());
        }
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
            Ok(url) => Ok((artifact_usage_uuid, artifact_uuid, url)),
            Err(e) => {
                dbg!(&e);
                self.storage
                    .release_artifact_usage(artifact_usage_uuid)
                    .await?;
                Err(e)
            }
        }
    }
}
