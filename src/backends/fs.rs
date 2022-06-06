use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use uuid::Uuid;

use super::Backend;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::error::Result;

pub struct FsBackend {
    root_path: std::path::PathBuf,
}

impl FsBackend {
    pub async fn new(root_path: &std::path::Path) -> Result<FsBackend> {
        let meta = tokio::fs::metadata(root_path).await?;
        if !meta.is_dir() {
            return Err(FsError::RootIsNotDir.into());
        }
        let mut new_perm = meta.permissions();
        new_perm.set_mode(0o700);
        tokio::fs::set_permissions(root_path, new_perm).await?;
        Ok(FsBackend {
            root_path: root_path.to_owned(),
        })
    }
}

#[derive(Debug)]
pub enum FsError {
    RootIsNotDir,
    PathNotUtf8,
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FsError {}

#[async_trait]
impl Backend for FsBackend {
    async fn create_class(&mut self, name: &str, _data: &ArtifactClassData) -> Result<()> {
        let mut dir_path = self.root_path.clone();
        dir_path.push(name);
        tokio::fs::create_dir(dir_path).await.map_err(|e| e.into())
    }

    async fn reserve_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<String> {
        let uuid_str = uuid.to_string();
        let mut dir_path = self.root_path.clone();
        dir_path.push(class_name);
        dir_path.push(uuid_str);
        tokio::fs::create_dir(&dir_path).await?;
        match art_type {
            ArtifactType::File => {
                dir_path.push("artifact");
                tokio::fs::File::create(&dir_path).await?;
                dir_path
                    .into_os_string()
                    .into_string()
                    .map_err(|_| FsError::PathNotUtf8.into())
            }
        }
    }
}
