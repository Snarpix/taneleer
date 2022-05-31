use std::sync::Arc;

use tokio::sync::Mutex;

use crate::backends::Backends;
use crate::class::ArtifactClassData;
use crate::storage::Storage;

pub type SharedArtifactManager = Arc<Mutex<ArtifactManager>>;

pub struct ArtifactManager {
    storage: Box<dyn Storage + Send>,
    backends: Backends,
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
        ArtifactManager { storage, backends }
    }

    pub async fn create_class(
        &mut self,
        name: String,
        data: ArtifactClassData,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let backend = self
            .backends
            .get_mut(&data.backend_name)
            .ok_or(ManagerError::BackendNotExists)?;
        self.storage.create_class(&name, &data).await?;
        backend.create_class(&name, &data).await?;
        Ok(())
    }
}
