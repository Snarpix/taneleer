use std::{net::SocketAddr, sync::Arc};

use axum::{Router, routing::{get, post, patch, put, head}, http::StatusCode, Extension};
use tokio::task::JoinHandle;
use tower::ServiceBuilder;

use crate::{config::frontend::ConfigDockerFrontend, error::Error, manager::{SharedArtifactManager, ManagerError}};

use super::Frontend;

use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Cursor, ErrorKind};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path as StdPath, PathBuf};
use std::str::FromStr;

use async_trait::async_trait;
use axum::body::StreamBody;
use axum::extract::{BodyStream, Path, Query};
use axum::http::{HeaderMap, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{
    http::header,
};
use bytes::Buf;
use futures::StreamExt;
use log::{error, info, warn};
use sha2::Digest;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use url::Url;
use uuid::Uuid;

use crate::artifact::ArtifactItemInfo;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::backend::ConfigDockerRegistry;
use crate::source::{Sha256, Hashsum};
use crate::util::{hash_file_sha256, rename_no_replace};
use crate::backends::BackendError;
use crate::manifest::ManifestInfo;

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

struct DockerRegistryFrontendState {
    manager: SharedArtifactManager,
    hostname: String,
    port: u16,
    default_backend: String,
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

impl From<DockerError> for DockerRegistryError {
    fn from(e: DockerError) -> Self {
        Self(Box::new(e))
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

impl From<ManagerError> for DockerRegistryError {
    fn from(e: ManagerError) -> Self {
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

pub struct DockerRegistry
{
    _handle: JoinHandle<()>,
}

impl DockerRegistry {
    pub async fn new(cfg: &ConfigDockerFrontend, manager: SharedArtifactManager) -> Result<Self, Error> {
        let state = Arc::new(DockerRegistryFrontendState {
            manager,
            hostname: cfg.hostname,
            port: cfg.port,
            default_backend: cfg.default_backend,
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
        Ok(DockerRegistry{_handle: handle})
    }
    async fn add_docker_version<B>(req: Request<B>, next: Next<B>) -> Response {
        let mut res = next.run(req).await;
        res.headers_mut()
            .insert(DOCKER_DISTRIBUTION_API_VERSION, API_VERSION);
        res
    }

    async fn start_upload(
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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

        let upload_uuid = state.manager.lock().await.create_upload(&state.default_backend,  &repo_name).await?;
        let upload_uuid_str = upload_uuid.to_string();

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
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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
        let res = async {
            let mut file = state.manager.lock().await.lock_upload(&state.default_backend, &repo_name, upload_uuid).await?;
    
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
        state.manager.lock().await.unlock_upload(&state.default_backend, &repo_name, upload_uuid).await?;
        res
    }

    async fn finalize_upload(
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
        Path((repo_name, upload_uuid_str)): Path<(String, String)>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<Response, DockerRegistryError> {
        println!("Put blob: {}", &repo_name);
        if !REPO_NAME_REGEX.is_match(&repo_name) {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                [(header::CONTENT_LENGTH, 0)],
            )
                .into_response());
        }
        let upload_uuid = Uuid::from_str(&upload_uuid_str)?;
        let digest = params.get("digest").ok_or(DockerError::NoDigest)?;
        let digest_hash = get_digest(digest).ok_or(DockerError::InvalidDigest)?;
        match state.manager.lock().await.commit_upload(&state.default_backend, &repo_name, upload_uuid, Hashsum::Sha256(digest_hash)).await {
            Ok(()) =>  Ok((
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
                .into_response()),
            Err(ManagerError::BackendError(BackendError::InvalidDigest)) => Ok((
                    StatusCode::NOT_ACCEPTABLE,
                    [(header::CONTENT_LENGTH, 0.to_string())],
                )
                    .into_response()),
            Err(e) => return Err(e.into()),
        }
       
    }

    async fn check_blob(
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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

        match state.manager.lock().await.check_upload(&state.default_backend, &repo_name, Hashsum::Sha256(digest_hash)).await {
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
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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

        match state.manager.lock().await.get_upload(&state.default_backend, &repo_name, Hashsum::Sha256(digest_hash)).await {
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
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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
                match state.manager.lock().await.check_artifact_reserve(*t).await {
                    Ok(_) => (),
                    Err(e) => {
                        warn!("Tag is not reserved");
                        return Ok((StatusCode::NOT_ACCEPTABLE, [(header::CONTENT_LENGTH, 0)])
                            .into_response());
                    }
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

        let upload_uuid = state.manager.lock().await.create_upload(&state.default_backend,  &repo_name).await?;
        let mut file = state.manager.lock().await.lock_upload(&state.default_backend, &repo_name, upload_uuid).await?;

        let body_len = body.len().try_into().unwrap();
        {
            let mut cursor = Cursor::new(body);
            while cursor.has_remaining() {
                file.write_buf(&mut cursor).await?;
            }
            file.flush().await?;
            file.set_permissions(PermissionsExt::from_mode(0o400))
                .await?;
        }

        state.manager.lock().await.unlock_upload(&state.default_backend, &repo_name, upload_uuid).await?;
        state.manager.lock().await.commit_manifest_upload(&state.default_backend, &repo_name, upload_uuid, Hashsum::Sha256(hash)).await?;

        if let Reference::UuidTag(artifact_uuid) = reference {
            let tag_path = self.storage.manifest_by_uuid_tag_path(&repo_name, &artifact_uuid);
            let rel_main_path = self.storage.manifest_by_digest_rel_path(&repo_name, &hash);
            // Tag should be unique
            match tokio::fs::symlink(&rel_main_path, &tag_path).await {
                Ok(()) => {
                    self.storage
                        .db
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
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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
        let manifest_info = match reference {
            Reference::Digest(hash) => 
                state.manager.lock().await.check_manifest_by_hash(&repo_name, Hashsum::Sha256(hash)).await
            ,
            Reference::UuidTag(uuid) => {
                state.manager.lock().await.check_manifest_by_uuid(&repo_name, uuid).await
            }
        };
        match manifest_info {
            Ok(ManifestInfo{hash, len, cnt_type}) => {
                let Hashsum::Sha256(hash) = hash; 
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_LENGTH, len.to_string()),
                        (
                            header::CONTENT_TYPE,
                            cnt_type,
                        ),
                        (DOCKER_CONTENT_DIGEST, "sha256:".to_owned() + &hex::encode(&hash)),
                    ],
                )
                    .into_response())
            }
            Err(_) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }

    async fn get_manifest(
        Extension(state): Extension<Arc<DockerRegistryFrontendState>>,
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
        let result = match reference {
            Reference::UuidTag(artifact_uuid) => {
                state.manager.lock().await.get_manifest_by_uuid( &repo_name, artifact_uuid).await
            }
            Reference::Digest(digest) => {
                state.manager.lock().await.get_manifest_by_hash( &repo_name, Hashsum::Sha256(digest)).await
            }
        };
    
        match result {
            Ok((file, ManifestInfo{hash, len, cnt_type})) => {
                let Hashsum::Sha256(hash) = hash;
                let stream = ReaderStream::new(file);
                let body = StreamBody::new(stream);
                Ok((
                    StatusCode::OK,
                    [
                        (header::CONTENT_LENGTH, len.to_string()),
                        (
                            header::CONTENT_TYPE,
                            cnt_type,
                        ),
                        (DOCKER_CONTENT_DIGEST, "sha256:".to_owned() + &hex::encode(&hash)),
                    ],
                    body,
                )
                    .into_response())
            }
            Err(_) => Ok(StatusCode::NOT_FOUND.into_response()),
        }
    }    

}

impl Frontend for DockerRegistry{}