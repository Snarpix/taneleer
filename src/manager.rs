use std::sync::{Arc, Mutex};

use crate::backends::Backends;
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

    pub fn create_class(
        &mut self,
        name: String,
        backend_name: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let backend = self
            .backends
            .get(&backend_name)
            .ok_or(ManagerError::BackendNotExists)?;
        Ok(())
    }
}
