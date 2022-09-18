mod storage;

use std::collections::HashMap;
use std::convert::TryInto;
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
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio_util::io::ReaderStream;
use tower::ServiceBuilder;
use url::Url;
use uuid::Uuid;

use crate::artifact::ArtifactItemInfo;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::backend::ConfigDockerRegistry;
use crate::error::Error;
use crate::source::Sha256;
use crate::util::{hash_file_sha256, rename_no_replace};

use super::Backend;

pub struct DockerRegistryBackend {
    state: Arc<DockerRegistryBackendState>,
    _handle: JoinHandle<()>,
}

struct DockerRegistryBackendState {
    root_path: std::path::PathBuf,
    hostname: String,
    port: u16,
    storage: Box<dyn storage::Storage + Send + Sync>,
}

impl DockerRegistryBackendState {
    fn repo_path(&self, repo_name: &str) -> PathBuf {
        let mut file_path = root_repo_path(&self.root_path);
        file_path.push(&repo_name);
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

const DOCKER_DISTRIBUTION_API_VERSION: header::HeaderName =
    header::HeaderName::from_static("docker-distribution-api-version");
const API_VERSION: header::HeaderValue = header::HeaderValue::from_static("registry/2.0");
const DOCKER_CONTENT_DIGEST: header::HeaderName =
    header::HeaderName::from_static("docker-content-digest");
const DOCKER_UPLOAD_UUID: header::HeaderName =
    header::HeaderName::from_static("docker-upload-uuid");

lazy_static::lazy_static! {
    static ref RANGE_REGEX: regex::Regex = regex::Regex::new("^([0-9]+)-([0-9]+)$").unwrap();
    static ref REPO_NAME_REGEX: regex::Regex = regex::Regex::new("^[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)*$").unwrap();
    static ref REFERENCE_REGEX: regex::Regex = regex::Regex::new("^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$").unwrap();
    static ref DIGEST_REGEX: regex::Regex = regex::Regex::new("^sha256:([A-Fa-f0-9]{64})$").unwrap();
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

async fn create_dir_if_not_exists(path: &StdPath) -> Result<(), Error> {
    match tokio::fs::DirBuilder::new().mode(0o700).create(&path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
    }
}

enum Reference {
    UuidTag(Uuid),
    Digest(Sha256),
}

fn parse_reference(reference: &str) -> Option<Reference> {
    get_digest(reference)
        .map(Reference::Digest)
        .or_else(|| Uuid::from_str(reference).ok().map(Reference::UuidTag))
}

fn get_digest(reference: &str) -> Option<Sha256> {
    if let Some(c) = DIGEST_REGEX.captures(reference) {
        let c = c.get(1).unwrap();
        let mut digest_hash: Sha256 = Default::default();
        hex::decode_to_slice(c.as_str(), &mut digest_hash).unwrap();
        Some(digest_hash)
    } else {
        None
    }
}

struct DockerRegistryError(Error);

impl IntoResponse for DockerRegistryError {
    fn into_response(self) -> Response {
        error!("{}", &self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl From<Error> for DockerRegistryError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}

impl From<std::io::Error> for DockerRegistryError {
    fn from(e: std::io::Error) -> Self {
        Self(Box::new(e))
    }
}

impl From<uuid::Error> for DockerRegistryError {
    fn from(e: uuid::Error) -> Self {
        Self(Box::new(e))
    }
}

impl From<axum::Error> for DockerRegistryError {
    fn from(e: axum::Error) -> Self {
        Self(Box::new(e))
    }
}

impl From<DockerError> for DockerRegistryError {
    fn from(e: DockerError) -> Self {
        Self(Box::new(e))
    }
}

impl From<std::array::TryFromSliceError> for DockerRegistryError {
    fn from(e: std::array::TryFromSliceError) -> Self {
        Self(Box::new(e))
    }
}

impl From<serde_json::Error> for DockerRegistryError {
    fn from(e: serde_json::Error) -> Self {
        Self(Box::new(e))
    }
}

impl DockerRegistryBackend {
    pub async fn new(cfg: &ConfigDockerRegistry) -> Result<Self, Error> {
        let root_path = &cfg.root_path;
        let meta = tokio::fs::metadata(root_path).await?;
        if !meta.is_dir() {
            return Err(DockerError::RootIsNotDir.into());
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
            hostname: cfg.hostname.clone(),
            port: cfg.port,
            storage,
        });

        let addr = SocketAddr::new(cfg.address, cfg.port);
        let app = Router::new()
            .route("/v2/", get(|| async { StatusCode::OK }))
            .route("/v2/:name/blobs/uploads/", post(Self::start_upload))
            .route(
                "/v2/:name/blobs/upload/:upload_uuid",
                patch(Self::upload_chunk),
            )
            .route(
                "/v2/:name/blobs/upload/:upload_uuid",
                put(Self::finalize_upload),
            )
            .route("/v2/:name/blobs/:reference", head(Self::check_blob))
            .route("/v2/:name/blobs/:reference", get(Self::get_blob))
            .route("/v2/:name/manifests/:tag", put(Self::put_manifest))
            .route("/v2/:name/manifests/:tag", head(Self::check_manifest))
            .route("/v2/:name/manifests/:tag", get(Self::get_manifest))
            .layer(
                ServiceBuilder::new()
                    .layer(Extension(state.clone()))
                    .layer(axum::middleware::from_fn(Self::add_docker_version)),
            );
        let handle = tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        Ok(DockerRegistryBackend {
            state,
            _handle: handle,
        })
    }

    async fn add_docker_version<B>(req: Request<B>, next: Next<B>) -> Response {
        let mut res = next.run(req).await;
        res.headers_mut()
            .insert(DOCKER_DISTRIBUTION_API_VERSION, API_VERSION);
        res
    }

    async fn start_upload(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path(repo_name): Path<String>,
    ) -> Result<Response, DockerRegistryError> {
        println!("Post blob: {}", &repo_name);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let upload_uuid = Uuid::new_v4();
        let upload_uuid_str = upload_uuid.to_string();
        let upload_path = state.upload_path(&repo_name, &upload_uuid);
        tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&upload_path)
            .await?
            .flush()
            .await?;

        state.storage.create_upload(&repo_name, upload_uuid).await?;
        Ok((
            StatusCode::ACCEPTED,
            [
                (
                    header::LOCATION,
                    format!("/v2/{}/blobs/upload/{}", repo_name, upload_uuid_str),
                ),
                (header::CONTENT_LENGTH, 0.to_string()),
                (DOCKER_UPLOAD_UUID, upload_uuid_str),
                (header::RANGE, "0-0".to_owned()),
            ],
        )
            .into_response())
    }

    async fn upload_chunk(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, upload_uuid_str)): Path<(String, String)>,
        headers: HeaderMap,
        mut stream: BodyStream,
    ) -> Result<Response, DockerRegistryError> {
        println!("Patch blob: {}", &repo_name);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let upload_uuid = Uuid::from_str(&upload_uuid_str)?;
        let upload_path = state.upload_path(&repo_name, &upload_uuid);

        state.storage.lock_upload(upload_uuid).await?;

        let res = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(&upload_path)
                .await?;

            let uploaded_file_size = file.metadata().await?.len();

            let mut expected_size: Option<u64> = None;
            if uploaded_file_size != 0 {
                if let Some(true) = headers
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| RANGE_REGEX.captures(v))
                    .map(|v| {
                        (
                            v.get(1).unwrap().as_str().parse::<u64>().unwrap(),
                            v.get(2).unwrap().as_str().parse::<u64>().unwrap(),
                        )
                    })
                    .map(|(start, end)| {
                        expected_size = Some(end - start);
                        start != uploaded_file_size + 1
                    })
                {
                } else {
                    return Ok((
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        [
                            (
                                header::LOCATION,
                                format!("/v2/{}/blobs/upload/{}", repo_name, upload_uuid_str),
                            ),
                            (header::CONTENT_LENGTH, 0.to_string()),
                            (header::RANGE, format!("0-{}", uploaded_file_size - 1)),
                            (DOCKER_UPLOAD_UUID, upload_uuid_str),
                        ],
                    )
                        .into_response());
                }
            }

            let mut size = 0usize;
            while let Some(chunk) = stream.next().await {
                let mut chunk = chunk?;
                while chunk.has_remaining() {
                    size += file.write_buf(&mut chunk).await?;
                }
            }
            println!("Patch size {}", &size);
            file.flush().await?;
            Ok((
                StatusCode::ACCEPTED,
                [
                    (
                        header::LOCATION,
                        format!("/v2/{}/blobs/upload/{}", repo_name, upload_uuid_str),
                    ),
                    (header::CONTENT_LENGTH, 0.to_string()),
                    (
                        header::RANGE,
                        format!("0-{}", uploaded_file_size + size as u64 - 1),
                    ),
                    (DOCKER_UPLOAD_UUID, upload_uuid_str),
                ],
            )
                .into_response())
        }
        .await;
        state.storage.unlock_upload(upload_uuid).await?;
        res
    }

    async fn finalize_upload(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, upload_uuid_str)): Path<(String, String)>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Response, DockerRegistryError> {
        println!("Put blob:{}", &repo_name);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let upload_uuid = Uuid::from_str(&upload_uuid_str)?;
        let upload_path = state.upload_path(&repo_name, &upload_uuid);

        let digest = params.get("digest").ok_or(DockerError::NoDigest)?;
        let digest_hash = get_digest(digest).ok_or(DockerError::InvalidDigest)?;

        state.storage.lock_upload(upload_uuid).await?;
        let mut locked = true;
        let res = async {
            tokio::fs::set_permissions(&upload_path, PermissionsExt::from_mode(0o400)).await?;

            let hash = hash_file_sha256(&upload_path).await?;
            let size = tokio::fs::metadata(&upload_path).await?.len();

            if digest_hash != hash {
                return Ok((
                    StatusCode::NOT_ACCEPTABLE,
                    [(header::CONTENT_LENGTH, 0.to_string())],
                )
                    .into_response());
            }

            let global_path = state.global_blob_path(&hash);
            match rename_no_replace(&upload_path, global_path).await {
                Ok(()) => {
                    state
                        .storage
                        .commit_blob(hash, size.try_into().unwrap(), upload_uuid)
                        .await?;
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    info!("Blob already exists: {}", digest);
                    tokio::fs::remove_file(upload_path).await?;
                    state.storage.remove_upload(upload_uuid).await?;
                }
                Err(e) => return Err(e.into()),
            };
            locked = false;

            let local_path = state.blob_path(&repo_name, &hash);
            let rel_path = state.global_rel_blob_path(&hash);
            match tokio::fs::symlink(rel_path, local_path).await {
                Ok(()) => {
                    state.storage.link_blob(hash, &repo_name).await?;
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    info!("Blob already linked: {}", digest);
                }
                Err(e) => return Err(e.into()),
            }

            Ok((
                StatusCode::CREATED,
                [
                    (
                        header::LOCATION,
                        format!("/v2/{}/blobs/upload/{}", repo_name, digest),
                    ),
                    (header::CONTENT_LENGTH, 0.to_string()),
                    (DOCKER_CONTENT_DIGEST, digest.clone()),
                ],
            )
                .into_response())
        }
        .await;
        if locked {
            state.storage.unlock_upload(upload_uuid).await?;
        }
        res
    }

    async fn check_blob(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, digest)): Path<(String, String)>,
    ) -> Result<Response, DockerRegistryError> {
        println!("Head blob: {}, {}", &repo_name, &digest);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let digest_hash = get_digest(&digest).ok_or(DockerError::InvalidDigest)?;
        let file_path = state.blob_path(&repo_name, &digest_hash);
        match tokio::fs::metadata(&file_path).await {
            Ok(m) => Ok((
                StatusCode::OK,
                [
                    (header::CONTENT_LENGTH, m.len().to_string()),
                    (DOCKER_CONTENT_DIGEST, digest.parse().unwrap()),
                ],
            )
                .into_response()),
            Err(_) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }

    async fn get_blob(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, digest)): Path<(String, String)>,
    ) -> Result<Response, DockerRegistryError> {
        println!("Get blob: {}, {}", &repo_name, &digest);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let digest_hash = get_digest(&digest).ok_or(DockerError::InvalidDigest)?;
        let file_path = state.blob_path(&repo_name, &digest_hash);

        match tokio::fs::OpenOptions::new()
            .read(true)
            .open(&file_path)
            .await
        {
            Ok(file) => {
                let size = file.metadata().await?.len();
                let stream = ReaderStream::new(file);
                let body = StreamBody::new(stream);
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_LENGTH, size.to_string()),
                        (DOCKER_CONTENT_DIGEST, digest),
                    ],
                    body,
                )
                    .into_response())
            }
            Err(_) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }

    async fn put_manifest(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, reference_str)): Path<(String, String)>,
        body: String,
    ) -> Result<Response, DockerRegistryError> {
        println!("Put manifest: {}, {}", &repo_name, &reference_str);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let reference = parse_reference(&reference_str).ok_or(DockerError::InvalidDigest)?;

        let mut hasher = sha2::Sha256::new();
        hasher.update(&body);
        let hash: Sha256 = hasher.finalize().into();
        let hex_hash = "sha256:".to_owned() + &hex::encode(hash);

        match &reference {
            Reference::UuidTag(t) => {
                // Early check, later check will be atomic
                let mut tag_path = state.manifest_by_uuid_tag_path(&repo_name, t);
                tag_path.pop();
                match tokio::fs::metadata(tag_path).await {
                    Ok(_) => (),
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        warn!("Tag is not reserved");
                        return Ok((StatusCode::NOT_ACCEPTABLE, [(header::CONTENT_LENGTH, 0)])
                            .into_response());
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Reference::Digest(d) => {
                if &hash != d {
                    return Ok(
                        (StatusCode::NOT_ACCEPTABLE, [(header::CONTENT_LENGTH, 0)]).into_response()
                    );
                }
            }
        }

        // TODO: Error processing
        let parsed_mainfest: serde_json::Value = serde_json::from_str(&body)?;

        let media_type = parsed_mainfest
            .get("mediaType")
            .and_then(|v| v.as_str())
            .ok_or(DockerError::InvalidDigest)?;

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
            .ok_or(DockerError::InvalidDigest)?
        {
            layers.push(
                l.and_then(|v| v.try_into().ok())
                    .ok_or(DockerError::InvalidDigest)?,
            );
        }

        let config_digest: Sha256 = parsed_mainfest
            .get("config")
            .and_then(|v| v.get("digest"))
            .and_then(|v| v.as_str())
            .and_then(|v| v.strip_prefix("sha256:"))
            .and_then(|v| hex::decode(v).ok())
            .and_then(|v| v.try_into().ok())
            .ok_or(DockerError::InvalidDigest)?;
        layers.push(config_digest);

        let upload_uuid = Uuid::new_v4();
        let upload_path = state.upload_path(&repo_name, &upload_uuid);

        let body_len = body.len().try_into().unwrap();
        {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&upload_path)
                .await?;
            let mut cursor = Cursor::new(body);
            while cursor.has_remaining() {
                file.write_buf(&mut cursor).await?;
            }
            file.flush().await?;
            file.set_permissions(PermissionsExt::from_mode(0o400))
                .await?;
        }

        let target_path = state.manifest_by_digest_path(&repo_name, &hash);
        match rename_no_replace(&upload_path, &target_path).await {
            Ok(()) => {
                state
                    .storage
                    .commit_manifest(&repo_name, hash, body_len, media_type, layers)
                    .await?;
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                info!("Manifest already exists: {}", &hex_hash);
                tokio::fs::remove_file(upload_path).await?;
            }
            Err(e) => return Err(e.into()),
        };

        if let Reference::UuidTag(artifact_uuid) = reference {
            let tag_path = state.manifest_by_uuid_tag_path(&repo_name, &artifact_uuid);
            let rel_main_path = state.manifest_by_digest_rel_path(&repo_name, &hash);
            // Tag should be unique
            match tokio::fs::symlink(&rel_main_path, &tag_path).await {
                Ok(()) => {
                    state
                        .storage
                        .commit_tag(artifact_uuid, &repo_name, hash)
                        .await?;
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    info!("Tag already exists: {}", &hex_hash);
                    let link = tokio::fs::read_link(tag_path).await?;
                    if link != rel_main_path {
                        return Ok((StatusCode::NOT_ACCEPTABLE, [(header::CONTENT_LENGTH, 0)])
                            .into_response());
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        println!("Hash: {}", &hex_hash);
        Ok((
            StatusCode::CREATED,
            [
                (
                    header::LOCATION,
                    format!("/v2/{}/manifests/{}", repo_name, reference_str),
                ),
                (header::CONTENT_LENGTH, 0.to_string()),
                (DOCKER_CONTENT_DIGEST, hex_hash),
            ],
        )
            .into_response())
    }

    async fn check_manifest(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, reference_str)): Path<(String, String)>,
    ) -> Result<Response, DockerRegistryError> {
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let reference = parse_reference(&reference_str).ok_or(DockerError::InvalidDigest)?;
        let file_path = match reference {
            Reference::UuidTag(artifact_uuid) => {
                state.manifest_by_uuid_tag_path(&repo_name, &artifact_uuid)
            }
            Reference::Digest(digest) => state.manifest_by_digest_path(&repo_name, &digest),
        };

        match tokio::fs::metadata(&file_path).await {
            Ok(m) => {
                let path = tokio::fs::read_link(&file_path).await?;
                let hash_hex = path.file_name().unwrap().to_str().unwrap();
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_LENGTH, m.len().to_string()),
                        (
                            header::CONTENT_TYPE,
                            "application/vnd.docker.distribution.manifest.v2+json".to_owned(),
                        ),
                        (DOCKER_CONTENT_DIGEST, "sha256:".to_owned() + hash_hex),
                    ],
                )
                    .into_response())
            }
            Err(_) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }

    async fn get_manifest(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((repo_name, reference_str)): Path<(String, String)>,
    ) -> Result<Response, DockerRegistryError> {
        println!("Get manifest: {}, {}", &repo_name, &reference_str);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let reference = parse_reference(&reference_str).ok_or(DockerError::InvalidDigest)?;
        let (file_path, digest) = match reference {
            Reference::UuidTag(artifact_uuid) => {
                let file_path = state.manifest_by_uuid_tag_path(&repo_name, &artifact_uuid);
                let target_path = tokio::fs::read_link(&file_path).await?;
                let digest =
                    "sha256:".to_owned() + target_path.file_name().unwrap().to_str().unwrap();
                (file_path, digest)
            }
            Reference::Digest(digest) => {
                let file_path = state.manifest_by_digest_path(&repo_name, &digest);
                let digest = "sha256:".to_owned() + &hex::encode(&digest);
                (file_path, digest)
            }
        };

        match tokio::fs::OpenOptions::new()
            .read(true)
            .open(&file_path)
            .await
        {
            Ok(file) => {
                let size = file.metadata().await?.len();
                let stream = ReaderStream::new(file);
                let body = StreamBody::new(stream);
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_LENGTH, size.to_string()),
                        (
                            header::CONTENT_TYPE,
                            "application/vnd.docker.distribution.manifest.v2+json".to_owned(),
                        ),
                        (DOCKER_CONTENT_DIGEST, digest),
                    ],
                    body,
                )
                    .into_response())
            }
            Err(_) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }
}

#[derive(Debug)]
pub enum DockerError {
    RootIsNotDir,
    NoDigest,
    InvalidDigest,
    InvalidArtifactType,
    EmptyCommit,
}

impl std::fmt::Display for DockerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for DockerError {}

#[async_trait]
impl Backend for DockerRegistryBackend {
    async fn create_class(&mut self, name: &str, data: &ArtifactClassData) -> Result<(), Error> {
        if !matches!(data.art_type, ArtifactType::DockerContainer) {
            return Err(DockerError::InvalidArtifactType.into());
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
    ) -> Result<Url, Error> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(DockerError::InvalidArtifactType.into());
        }

        let mut dir_path = self.state.manifest_by_uuid_tag_path(class_name, &uuid);
        dir_path.pop();
        tokio::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir_path)
            .await?;

        Ok(Url::parse(&format!(
            "{}:{}/{}:{}",
            self.state.hostname, self.state.port, class_name, uuid
        ))
        .unwrap())
    }

    async fn abort_reserve(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<(), Error> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(DockerError::InvalidArtifactType.into());
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
    ) -> Result<Vec<ArtifactItemInfo>, Error> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(DockerError::InvalidArtifactType.into());
        }

        let res = self.state.storage.get_artifact_items(uuid).await?;
        if res.is_empty() {
            Err(DockerError::EmptyCommit.into())
        } else {
            Ok(res)
        }
    }

    async fn get_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url, Error> {
        if !matches!(art_type, ArtifactType::DockerContainer) {
            return Err(DockerError::InvalidArtifactType.into());
        }

        // TODO: Maybe check existence
        Ok(Url::parse(&format!(
            "{}:{}/{}:{}",
            self.state.hostname, self.state.port, class_name, uuid
        ))
        .unwrap())
    }
}
