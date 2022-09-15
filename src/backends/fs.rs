use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use log::warn;
use url::Url;
use uuid::Uuid;

use super::Backend;
use crate::artifact::ArtifactItemInfo;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::backend::ConfigFsBackend;
use crate::error::Result;
use crate::source::Hashsum;
use crate::util::hash_file_sha256;

pub struct FsBackend {
    root_path: std::path::PathBuf,
}

impl FsBackend {
    pub async fn new(cfg: &ConfigFsBackend) -> Result<FsBackend> {
        let root_path = &cfg.root_path;
        let meta = tokio::fs::metadata(root_path).await?;
        if !meta.is_dir() {
            return Err(FsError::RootIsNotDir.into());
        }
        let mut new_perm = meta.permissions();
        new_perm.set_mode(0o701);
        tokio::fs::set_permissions(root_path, new_perm).await?;
        Ok(FsBackend {
            root_path: root_path.to_owned(),
        })
    }
}

#[derive(Debug)]
pub enum FsError {
    RootIsNotDir,
    InvalidArtifactType,
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
        tokio::fs::DirBuilder::new()
            .mode(0o701)
            .create(&dir_path)
            .await?;
        Ok(())
    }

    async fn reserve_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        let uuid_str = uuid.to_string();
        let mut dir_path = self.root_path.clone();
        dir_path.push(class_name);
        dir_path.push(uuid_str);
        tokio::fs::DirBuilder::new()
            .mode(0o701)
            .create(&dir_path)
            .await?;
        let res: Result<Url> = async {
            match art_type {
                ArtifactType::File => {
                    let mut file_path = dir_path.clone();
                    file_path.push("artifact");
                    tokio::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o606)
                        .open(&file_path)
                        .await?
                        .set_permissions(PermissionsExt::from_mode(0o606))
                        .await?;
                    let res_path = tokio::fs::canonicalize(&file_path).await;
                    match res_path {
                        Ok(res_path) => {
                            let res = res_path.into_os_string().into_string().unwrap();
                            Ok(Url::parse("file://").unwrap().join(&res).unwrap())
                        }
                        Err(e) => {
                            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                                warn!("Failed to cleanup file: {:?}", e);
                            }
                            Err(e.into())
                        }
                    }
                }
                ArtifactType::DockerContainer => Err(FsError::InvalidArtifactType.into()),
            }
        }
        .await;
        if res.is_err() {
            if let Err(e) = tokio::fs::remove_dir(&dir_path).await {
                warn!("Failed to cleanup dir: {:?}", e);
            }
        }
        res
    }

    async fn commit_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Vec<ArtifactItemInfo>> {
        let uuid_str = uuid.to_string();
        let mut dir_path = self.root_path.clone();
        dir_path.push(class_name);
        dir_path.push(uuid_str);
        tokio::fs::set_permissions(&dir_path, PermissionsExt::from_mode(0o500)).await?;
        match art_type {
            ArtifactType::File => {
                let mut file_path = dir_path.clone();
                file_path.push("artifact");
                tokio::fs::set_permissions(&file_path, PermissionsExt::from_mode(0o400)).await?;
                let meta = tokio::fs::metadata(&file_path).await?;
                let file_size = meta.len();
                let file_hash = hash_file_sha256(&file_path).await?;
                Ok(vec![ArtifactItemInfo {
                    id: "artifact".to_string(),
                    size: file_size,
                    hash: Hashsum::Sha256(file_hash),
                }])
            }
            ArtifactType::DockerContainer => return Err(FsError::InvalidArtifactType.into()),
        }
    }

    async fn get_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        let uuid_str = uuid.to_string();
        let mut dir_path = self.root_path.clone();
        dir_path.push(class_name);
        dir_path.push(uuid_str);
        match art_type {
            ArtifactType::File => {
                let mut file_path = dir_path.clone();
                file_path.push("artifact");
                let res_path = tokio::fs::canonicalize(&file_path).await?;
                tokio::fs::set_permissions(&res_path, PermissionsExt::from_mode(0o404)).await?;
                Ok(Url::parse(&format!(
                    "file://{}",
                    res_path.as_os_str().to_string_lossy()
                ))?)
            }
            ArtifactType::DockerContainer => return Err(FsError::InvalidArtifactType.into()),
        }
    }
}
