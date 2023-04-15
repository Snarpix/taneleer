#![allow(clippy::declare_interior_mutable_const)]

mod storage;

use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::Metadata;
use std::io::{Cursor, ErrorKind};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path as StdPath, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::StreamBody;
use axum::extract::{BodyStream, Path, Query};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::{
    http::header,
    routing::{get, head, patch, post, put},
    Router,
};
use bytes::Buf;
use futures::StreamExt;
use log::{error, info, warn};
use sha2::Digest;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio_util::io::ReaderStream;
use tower::ServiceBuilder;
use url::Url;
use uuid::Uuid;

use crate::api::Hashsum;
use crate::artifact::ArtifactItemInfo;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::backend::ConfigDockerRegistry;
use crate::manifest::ManifestInfo;
pub use storage::StorageError;
use crate::source::Sha256;
use crate::util::{hash_file_sha256, rename_no_replace};

use super::{Backend, Result, BackendError};

pub struct DockerRegistryBackend {
    state: Arc<DockerRegistryBackendState>,
}

struct DockerRegistryBackendState {
    root_path: std::path::PathBuf,
    storage: Box<dyn storage::Storage + Send + Sync>,
}

impl DockerRegistryBackendState {
    fn repo_path(&self, repo_name: &str) -> PathBuf {
        let mut file_path = root_repo_path(&self.root_path);
        file_path.push(repo_name);
        file_path
    }

    fn upload_path(&self, repo_name: &str, upload_uuid: &Uuid) -> PathBuf {
        let mut file_path = self.repo_path(repo_name);
        file_path.push("uploads");
        file_path.push(upload_uuid.to_string());
        file_path
    }

    fn blob_path(&self, repo_name: &str, digest: &Sha256) -> PathBuf {
        let mut blobs_path = self.repo_path(repo_name);
        blobs_path.push("blobs");
        blobs_path.push("sha256");
        blobs_path.push(hex::encode(digest));
        blobs_path
    }

    fn manifest_by_digest_path(&self, repo_name: &str, digest: &Sha256) -> PathBuf {
        let mut mani_path = self.repo_path(repo_name);
        mani_path.push("manifests");
        mani_path.push("digests");
        mani_path.push("sha256");
        mani_path.push(hex::encode(digest));
        mani_path
    }

    fn manifest_by_digest_rel_path(&self, _repo_name: &str, digest: &Sha256) -> PathBuf {
        let mut mani_path = PathBuf::new();
        mani_path.push("..");
        mani_path.push("..");
        mani_path.push("digests");
        mani_path.push("sha256");
        mani_path.push(hex::encode(digest));
        mani_path
    }

    // In a folder, so we can track created reserves
    fn manifest_by_uuid_tag_path(&self, repo_name: &str, tag: &Uuid) -> PathBuf {
        let mut mani_path = self.repo_path(repo_name);
        mani_path.push("manifests");
        mani_path.push("tags");
        mani_path.push(tag.to_string());
        mani_path.push("manifest");
        mani_path
    }

    fn global_blob_path(&self, digest: &Sha256) -> PathBuf {
        let mut blobs_path = blobs_path(&self.root_path);
        blobs_path.push("sha256");
        blobs_path.push(hex::encode(digest));
        blobs_path
    }

    fn global_rel_blob_path(&self, digest: &Sha256) -> PathBuf {
        let mut rel_path = PathBuf::new();
        rel_path.push("..");
        rel_path.push("..");
        rel_path.push("..");
        rel_path.push("..");
        rel_path.push("blobs");
        rel_path.push("sha256");
        rel_path.push(hex::encode(digest));
        rel_path
    }
}

fn blobs_path(root_path: &StdPath) -> PathBuf {
    let mut blobs_path = root_path.to_owned();
    blobs_path.push("blobs");
    blobs_path
}

fn root_repo_path(root_path: &StdPath) -> PathBuf {
    let mut blobs_path = root_path.to_owned();
    blobs_path.push("repositories");
    blobs_path
}

async fn create_dir_if_not_exists(path: &StdPath) -> Result<()> {
    match tokio::fs::DirBuilder::new().mode(0o700).create(&path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
    }
}

impl DockerRegistryBackend {
    pub async fn new(cfg: &ConfigDockerRegistry) -> Result<Self> {
        let root_path = &cfg.root_path;
        let meta = tokio::fs::metadata(root_path).await?;
        if !meta.is_dir() {
            return Err(BackendError::RootIsNotDir.into());
        }

        let new_perm = {
            let mut perm = meta.permissions();
            perm.set_mode(0o700);
            perm
        };
        tokio::fs::set_permissions(root_path, new_perm.clone()).await?;

        let mut blobs_path = blobs_path(root_path);
        create_dir_if_not_exists(&blobs_path).await?;
        tokio::fs::set_permissions(&blobs_path, new_perm.clone()).await?;

        blobs_path.push("sha256");
        create_dir_if_not_exists(&blobs_path).await?;
        tokio::fs::set_permissions(blobs_path, new_perm.clone()).await?;

        let repo_path = root_repo_path(root_path);
        create_dir_if_not_exists(&repo_path).await?;
        tokio::fs::set_permissions(repo_path, new_perm.clone()).await?;

        let mut db_path = root_path.clone();
        db_path.push("docker.db");
        let storage = storage::new_storage(&db_path).await?;

        let state = Arc::new(DockerRegistryBackendState {
            root_path: root_path.to_owned(),
            storage,
        });

        Ok(DockerRegistryBackend {
            state,
        })
    }
}

#[async_trait]
impl Backend for DockerRegistryBackend {
    async fn create_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<()> {
        if !matches!(data.art_type, ArtifactType::DockerContainer) {
            return Err(BackendError::InvalidArtifactType.into());
        }

        let mut dir_path = self.state.repo_path(name);
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.push("uploads");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.pop();
        dir_path.push("blobs");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.push("sha256");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.pop();
        dir_path.pop();
        dir_path.push("manifests");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.push("digests");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.push("sha256");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        dir_path.pop();
        dir_path.pop();
        dir_path.push("tags");
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        self.state.storage.create_class(name).await?;
        Ok(())
    }

    async fn reserve_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(BackendError::InvalidArtifactType.into());
        }

        let mut dir_path = self.state.manifest_by_uuid_tag_path(class_name, &uuid);
        dir_path.pop();
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        Ok(Url::parse(&format!(
            "{}:{}",
            class_name, uuid
        ))
        .unwrap())
    }

    async fn abort_reserve(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<()> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(BackendError::InvalidArtifactType.into());
        }

        self.state.storage.remove_tag_if_exists(uuid).await?;
        let mut dir_path = self.state.manifest_by_uuid_tag_path(class_name, &uuid);
        dir_path.pop();
        tokio::fs::remove_dir_all(dir_path).await?;

        Ok(())
    }

    async fn commit_artifact(
        &mut self,
        _class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Vec<ArtifactItemInfo>> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(BackendError::InvalidArtifactType.into());
        }

        let res = self.state.storage.get_artifact_items(uuid).await?;
        if res.is_empty() {
            Err(BackendError::EmptyCommit.into())
        } else {
            Ok(res)
        }
    }

    async fn get_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(BackendError::InvalidArtifactType.into());
        }

        // TODO: Maybe check existence
        Ok(Url::parse(&format!(
            "{}:{}",
            class_name, uuid
        ))
        .unwrap())
    }
    async fn create_upload(&mut self, repo_name: &str) -> Result<Uuid>{
        let upload_uuid = Uuid::new_v4();
        let upload_uuid_str = upload_uuid.to_string();
        let upload_path = self.state.upload_path(&repo_name, &upload_uuid);
        tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&upload_path)
            .await?
            .flush()
            .await?;

        self.state.storage.create_upload(&repo_name, upload_uuid).await?;
        Ok(upload_uuid)
    }
    async fn lock_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
    ) -> Result<File> {
        let upload_path = self.state.upload_path(&class_name, &upload_uuid);
        self.state.storage.lock_upload(upload_uuid).await?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&upload_path)
            .await?;
        Ok(file)
    }
    async fn unlock_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
    ) -> Result<()> {
        self.state.storage.unlock_upload(upload_uuid).await?;
        Ok(())
    }

    async fn commit_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
        hash: Hashsum,
    ) -> Result<()> {
        let upload_path = self.state.upload_path(&class_name, &upload_uuid);

        let mut locked = true;
        let res = async {
            tokio::fs::set_permissions(&upload_path, PermissionsExt::from_mode(0o400)).await?;

            let file_hash = hash_file_sha256(&upload_path).await?;
            let Hashsum::Sha256(hash) = hash;
            let size = tokio::fs::metadata(&upload_path).await?.len();

            if hash != file_hash {
                return Err(BackendError::InvalidDigest);
            }

            let global_path = self.state.global_blob_path(&hash);
            match rename_no_replace(&upload_path, global_path).await {
                Ok(()) => {
                    self.state
                        .storage
                        .commit_blob(hash, size.try_into().unwrap(), upload_uuid)
                        .await?;
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    info!("Blob already exists: {}", &hex::encode(hash));
                    tokio::fs::remove_file(upload_path).await?;
                    self.state.storage.remove_upload(upload_uuid).await?;
                }
                Err(e) => return Err(e.into()),
            };
            locked = false;

            let local_path = self.state.blob_path(&class_name, &hash);
            let rel_path = self.state.global_rel_blob_path(&hash);
            match tokio::fs::symlink(rel_path, local_path).await {
                Ok(()) => {
                    self.state.storage.link_blob(hash, &class_name).await?;
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    info!("Blob already linked: {}", &hex::encode(hash));
                }
                Err(e) => return Err(e.into()),
            }
            Ok(())
        }
        .await;
        if locked {
            self.state.storage.unlock_upload(upload_uuid).await?;
        }
        res
    }

    async fn commit_manifest_upload(
        &mut self,
        class_name: &str,
        upload_uuid: Uuid,
        hash: Hashsum,
    ) -> Result<()> {
        let Hashsum::Sha256(hash) = hash;
        let upload_path = self.state.upload_path(&class_name, &upload_uuid);
        let target_path = self.state.manifest_by_digest_path(&class_name, &hash);

        // TODO: Error processing
        let parsed_mainfest: serde_json::Value = serde_json::from_str(&body)?;

        let media_type = parsed_mainfest
            .get("mediaType")
            .and_then(|v| v.as_str())
            .ok_or(BackendError::InternalError)?;

        let mut layers = Vec::<Sha256>::new();
        for l in parsed_mainfest
            .get("layers")
            .and_then(|v| v.as_array())
            .map(|v| {
                v.iter().map(|l| {
                    l.get("digest")
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.strip_prefix("sha256:"))
                        .and_then(|v| hex::decode(v).ok())
                })
            })
            .ok_or(BackendError::InternalError)?
        {
            layers.push(
                l.and_then(|v| v.try_into().ok())
                    .ok_or(BackendError::InternalError)?,
            );
        }

        let config_digest: Sha256 = parsed_mainfest
            .get("config")
            .and_then(|v| v.get("digest"))
            .and_then(|v| v.as_str())
            .and_then(|v| v.strip_prefix("sha256:"))
            .and_then(|v| hex::decode(v).ok())
            .and_then(|v| v.try_into().ok())
            .ok_or(BackendError::InternalError)?;
        layers.push(config_digest);
 
        match rename_no_replace(&upload_path, &target_path).await {
            Ok(()) => {
                self.state.storage
                    .commit_manifest(&class_name, hash, body_len, media_type, layers)
                    .await?;
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                info!("Manifest already exists: {}", hex::encode(&hash));
                tokio::fs::remove_file(upload_path).await?;
            }
            Err(e) => return Err(e.into()),
        };
    }

    async fn check_upload(
        &mut self,
        repo_name: &str,
        hash: Hashsum,
    ) -> Result<Metadata> {
        let Hashsum::Sha256(hash) = hash;
        let file_path = self.state.blob_path(&repo_name, &hash);
        return tokio::fs::metadata(&file_path).await.map_err(Into::into);
    }
    async fn get_upload(
        &mut self,
        repo_name: &str,
        hash: Hashsum,
    ) -> Result<File> {
        let Hashsum::Sha256(hash) = hash;
        let file_path = self.state.blob_path(&repo_name, &hash);
        Ok(tokio::fs::OpenOptions::new()
            .read(true)
            .open(&file_path)
            .await?)
    }
    async fn check_manifest_by_hash(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<ManifestInfo> {
        let Hashsum::Sha256(hash_sha) = hash;
        let file_path = self.state.manifest_by_digest_path(class_name, &hash_sha);
    
        let metadata = tokio::fs::metadata(&file_path).await?;
        // let path = tokio::fs::read_link(&file_path).await?;
        // let hash_hex = path.file_name().unwrap().to_str().unwrap();
    
        Ok(ManifestInfo { hash, len: metadata.len(), cnt_type: "application/vnd.docker.distribution.manifest.v2+json".to_owned() })
    }
    async fn check_manifest_by_uuid(
        &mut self,
        class_name: &str,
        uuid: Uuid,
    ) -> Result<ManifestInfo> {
        let file_path = self.state.manifest_by_uuid_tag_path(class_name, &uuid);
    
        let metadata = tokio::fs::metadata(&file_path).await?;
        let path = tokio::fs::read_link(&file_path).await?;
        let hash_hex = path.file_name().unwrap().to_str().unwrap();
        let mut hash: Sha256 = Default::default();
        hex::decode_to_slice(hash_hex, &mut hash).or(Err(BackendError::InternalError))?;
        Ok(ManifestInfo { hash: Hashsum::Sha256(hash), len: metadata.len(), cnt_type: "application/vnd.docker.distribution.manifest.v2+json".to_owned() })
    }   
    async fn get_manifest_by_hash(
        &mut self,
        class_name: &str,
        hash: Hashsum,
    ) -> Result<(File, ManifestInfo)> {
        let Hashsum::Sha256(hash_sha) = hash;
        let file_path = self.state.manifest_by_digest_path(class_name, &hash_sha);
    
        let file = tokio::fs::OpenOptions::new().read(true).open(&file_path).await?;
        let metadata = file.metadata().await?;
    
        Ok((file, ManifestInfo { hash, len: metadata.len(), cnt_type: "application/vnd.docker.distribution.manifest.v2+json".to_owned() }))
    }
    async fn get_manifest_by_uuid(
        &mut self,
        class_name: &str,
        uuid: Uuid,
    ) -> Result<(File, ManifestInfo)> {
        let file_path = self.state.manifest_by_uuid_tag_path(class_name, &uuid);
        let target_path = tokio::fs::read_link(&file_path).await?;
        let hash_hex = target_path.file_name().unwrap().to_str().unwrap();
        let mut hash: Sha256 = Default::default();
        hex::decode_to_slice(hash_hex, &mut hash).or(Err(BackendError::InternalError))?;
    
        let file = tokio::fs::OpenOptions::new().read(true).open(&file_path).await?;
        let metadata = file.metadata().await?;
    
        Ok((file, ManifestInfo { hash: Hashsum::Sha256(hash), len: metadata.len(), cnt_type: "application/vnd.docker.distribution.manifest.v2+json".to_owned() }))
    }
}
