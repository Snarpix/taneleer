use std::sync::Arc;

use tokio::sync::broadcast::{self, Receiver as BReceiver, Sender as BSender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::backends::Backends;
use crate::class::ArtifactClassData;
use crate::error::Result;
use crate::source::Source;
use crate::storage::Storage;

pub type SharedArtifactManager = Arc<Mutex<ArtifactManager>>;
pub type ManagerMessageStream = BroadcastStream<ManagerMessage>;

#[derive(Clone, Debug)]
pub enum ManagerMessage {
    NewClass(String),
}

pub struct ArtifactManager {
    storage: Box<dyn Storage + Send>,
    backends: Backends,
    message_broadcast: BSender<ManagerMessage>,
}

#[derive(Debug)]
pub enum ManagerError {
    BackendNotExists,
}

impl std::fmt::Display for ManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ManagerError {}

impl ArtifactManager {
    pub fn new(storage: Box<dyn Storage + Send>, backends: Backends) -> ArtifactManager {
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
            .unwrap();
        Ok(())
    }

    pub async fn get_clases(&self) -> Result<Vec<String>> {
        self.storage.get_classes().await
    }

    pub async fn reserve_artifact(
        &mut self,
        class_name: String,
        sources: Vec<(String, Source)>,
    ) -> Result<(String, String)> {
        let artifact_uuid = Uuid::new_v4();
        let (backend_name, artifact_type) = self
            .storage
            .reserve_artifact(artifact_uuid, &class_name, &sources)
            .await?;
        let res = async {
            let backend = self
                .backends
                .get_mut(&backend_name)
                .ok_or(ManagerError::BackendNotExists)?;
            backend
                .reserve_artifact(&class_name, artifact_type, artifact_uuid)
                .await
        }
        .await;
        match res {
            Ok(url) => Ok((artifact_uuid.to_string(), url)),
            Err(e) => {
                self.storage
                    .rollback_artifact_reserve(artifact_uuid)
                    .await?;
                Err(e)
            }
        }
    }
}
