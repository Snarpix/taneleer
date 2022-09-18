use std::collections::HashMap;

use url::Url;
use uuid::Uuid;

use crate::{
    artifact::ArtifactItemInfo,
    backends::{backend_from_config, Backend},
    class::{ArtifactClassData, ArtifactType},
    config::backend::ConfigBackend,
    error::Result,
    manager::ManagerError,
    proxies::proxy_from_config,
    proxies::BackendProxy,
};

pub struct BackendPack {
    backend: Box<dyn Backend + Send + Sync>,
    proxies: HashMap<String, Box<dyn BackendProxy + Send + Sync>>,
}

impl BackendPack {
    pub async fn create_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()> {
        self.backend.create_class(name, data).await
    }

    pub async fn reserve_artifact(
        &mut self,
        proxy: Option<&str>,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        if let Some(proxy) = proxy {
            let p = self
                .proxies
                .get_mut(proxy)
                .ok_or(ManagerError::ProxyNotExists)?;
            p.reserve_artifact(&mut *self.backend, class_name, art_type, uuid)
                .await
        } else {
            self.backend
                .reserve_artifact(class_name, art_type, uuid)
                .await
        }
    }

    pub async fn abort_reserve(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<()> {
        self.backend.abort_reserve(class_name, art_type, uuid).await
    }

    pub async fn commit_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Vec<ArtifactItemInfo>> {
        self.backend
            .commit_artifact(class_name, art_type, uuid)
            .await
    }

    pub async fn get_artifact(
        &mut self,
        proxy: Option<&str>,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        if let Some(proxy) = proxy {
            let p = self
                .proxies
                .get_mut(proxy)
                .ok_or(ManagerError::ProxyNotExists)?;
            p.get_artifact(&mut *self.backend, class_name, art_type, uuid)
                .await
        } else {
            self.backend.get_artifact(class_name, art_type, uuid).await
        }
    }
}

pub type Backends = HashMap<String, BackendPack>;

pub async fn from_config(config: &ConfigBackend) -> Result<BackendPack> {
    let backend = backend_from_config(&config.specific).await?;
    let mut proxies = HashMap::with_capacity(config.common.proxies.len());
    for (p_name, p_config) in &config.common.proxies {
        let proxy = proxy_from_config(p_config).await?;
        proxies.insert(p_name.clone(), proxy);
    }
    Ok(BackendPack { backend, proxies })
}
