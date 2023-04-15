use std::fs::Metadata;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use async_trait::async_trait;
use log::warn;
use tokio::fs::File;
use url::Url;
use uuid::Uuid;

use super::{Backend, Result, BackendError};
use crate::artifact::ArtifactItemInfo;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::backend::ConfigFsBackend;
use crate::manifest::ManifestInfo;
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
            return Err(BackendError::RootIsNotDir);
        }
        let mut new_perm = meta.permissions();
        new_perm.set_mode(0o701);
        tokio::fs::set_permissions(root_path, new_perm).await?;
        Ok(FsBackend {
            root_path: root_path.to_owned(),
        })
    }
}

#[async_trait]
impl Backend for FsBackend {
    async fn create_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()> {
        match data.art_type {
            ArtifactType::File => (),
            ArtifactType::DockerContainer => return Err(BackendError::InvalidArtifactType.into()),
        }
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
                ArtifactType::DockerContainer => Err(BackendError::InvalidArtifactType.into()),
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

    async fn abort_reserve(
        &mut self,
        class_name: &str,
        _art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<()> {
        let uuid_str = uuid.to_string();
        let mut dir_path = self.root_path.clone();
        dir_path.push(class_name);
        dir_path.push(uuid_str);
        tokio::fs::remove_dir_all(dir_path).await?;
        Ok(())
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
            ArtifactType::DockerContainer => return Err(BackendError::InvalidArtifactType.into()),
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
            ArtifactType::DockerContainer => return Err(BackendError::InvalidArtifactType.into()),
        }
    }
    async fn create_upload(
        &mut self,
        class_name: &str,
    ) -> Result<Uuid> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn lock_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
    ) -> Result<File> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn unlock_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
    ) -> Result<()> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn commit_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
        hash: Hashsum,
    ) -> Result<()> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn commit_manifest_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
        hash: Hashsum,
    ) -> Result<()> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn check_upload(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<Metadata> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn get_upload(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<File> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn check_manifest_by_hash(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<ManifestInfo> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn check_manifest_by_uuid(
        &mut self,
        class_name: &str,
        uuid: Uuid,
    ) -> Result<ManifestInfo> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn get_manifest_by_hash(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<(File, ManifestInfo)> {
        return Err(BackendError::NotImplemented.into());
    }
    async fn get_manifest_by_uuid(
        &mut self,
        class_name: &str,
        uuid: Uuid,
    ) -> Result<(File, ManifestInfo)> {
        return Err(BackendError::NotImplemented.into());
    }
}
