use async_trait::async_trait;
use url::Url;
use uuid::Uuid;

use crate::{
    backends::Backend, class::ArtifactType, config::proxy::ConfigRewriteProxy, error::Result,
};

use super::BackendProxy;

pub struct RewriteProxy {
    from: Url,
    to: Url,
}

impl RewriteProxy {
    pub async fn new(cfg: &ConfigRewriteProxy) -> Result<Self> {
        let from = Url::parse(&cfg.from)?;
        let to = Url::parse(&cfg.to)?;
        Ok(Self { from, to })
    }
}

#[async_trait]
impl BackendProxy for RewriteProxy {
    async fn reserve_artifact(
        &mut self,
        backend: &mut (dyn Backend + Send + Sync),
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        match backend.reserve_artifact(class_name, art_type, uuid).await {
            Ok(url) => {
                if let Some(rel) = self.from.make_relative(&url) {
                    Ok(self.to.join(&rel)?)
                } else {
                    Err(RewriteProxyError::RelativeRewriteFailed.into())
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn get_artifact(
        &mut self,
        backend: &mut (dyn Backend + Send + Sync),
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        match backend.get_artifact(class_name, art_type, uuid).await {
            Ok(url) => {
                if let Some(rel) = self.from.make_relative(&url) {
                    Ok(self.to.join(&rel)?)
                } else {
                    Err(RewriteProxyError::RelativeRewriteFailed.into())
                }
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
pub enum RewriteProxyError {
    RelativeRewriteFailed,
}

impl std::fmt::Display for RewriteProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RewriteProxyError {}
