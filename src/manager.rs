use std::sync::{Arc, Mutex};

use crate::backends::Backends;
use crate::storage::Storage;

pub type SharedArtifactManager = Arc<Mutex<ArtifactManager>>;

pub struct ArtifactManager {
    storage: Box<dyn Storage + Send>,
    backends: Backends,
}

impl ArtifactManager {
    pub fn new(storage: Box<dyn Storage + Send>, backends: Backends) -> ArtifactManager {
        ArtifactManager { storage, backends }
    }
}
