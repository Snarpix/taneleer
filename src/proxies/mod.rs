mod rewrite;

use async_trait::async_trait;
use url::Url;
use uuid::Uuid;

use crate::{
    backends::Backend, class::ArtifactType, config::proxy::ConfigBackendProxy, error::Result,
};
use rewrite::RewriteProxy;

#[async_trait]
pub trait BackendProxy {
    #[must_use]
    async fn reserve_artifact(
        &mut self,
        backend: &mut (dyn Backend + Send + Sync),
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url>;

    #[must_use]
    async fn get_artifact(
        &mut self,
        backend: &mut (dyn Backend + Send + Sync),
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url>;
}

pub async fn proxy_from_config(
    config: &ConfigBackendProxy,
) -> Result<Box<dyn BackendProxy + Send + Sync>> {
    match config {
        ConfigBackendProxy::Rewrite(cfg) => Ok(Box::new(RewriteProxy::new(cfg).await?)),
    }
}
