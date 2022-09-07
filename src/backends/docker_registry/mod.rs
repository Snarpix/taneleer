mod storage;

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{BodyStream, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::{
    http::header,
    routing::{get, head, patch, post, put},
    Router,
};
use bytes::Buf;
use futures::StreamExt;
use sha2::Digest;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

use crate::artifact::ArtifactItemInfo;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::backend::ConfigDockerRegistry;
use crate::error::Result;
use crate::source::Sha256;
use crate::util::hash_file_sha256;

use super::Backend;

pub struct DockerRegistryBackend {
    state: Arc<DockerRegistryBackendState>,
    _handle: JoinHandle<()>,
}

struct DockerRegistryBackendState {
    root_path: std::path::PathBuf,
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
}

impl DockerRegistryBackend {
    pub async fn new(cfg: &ConfigDockerRegistry) -> Result<Self> {
        let root_path = &cfg.root_path;
        let meta = tokio::fs::metadata(root_path).await?;
        if !meta.is_dir() {
            return Err(DockerError::RootIsNotDir.into());
        }
        let mut new_perm = meta.permissions();
        new_perm.set_mode(0o701);
        tokio::fs::set_permissions(root_path, new_perm).await?;

        let state = Arc::new(DockerRegistryBackendState {
            root_path: root_path.to_owned(),
        });

        let addr = SocketAddr::new(cfg.address, cfg.port);
        let app = Router::new()
            .route(
                "/v2/",
                get(|| async {
                    (
                        StatusCode::OK,
                        [(DOCKER_DISTRIBUTION_API_VERSION, API_VERSION)],
                    )
                }),
            )
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
            .route("/v2/:name/manifests/:tag", put(Self::put_manifest))
            .layer(Extension(state.clone()));
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

    async fn start_upload(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path(name): Path<String>,
    ) -> Response {
        println!("Post blob: {}", &name);
        match Self::start_upload_impl(state, name).await {
            Ok(r) => r.into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", e),
            )
                .into_response(),
        }
    }

    async fn start_upload_impl(
        state: Arc<DockerRegistryBackendState>,
        name: String,
    ) -> Result<impl IntoResponse> {
        let upload_uuid = Uuid::new_v4();
        let upload_uuid_str = upload_uuid.to_string();
        let mut file_path = state.root_path.clone();
        file_path.push(&name);
        file_path.push("upload");
        file_path.push(&upload_uuid_str);
        tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o606)
            .open(&file_path)
            .await?
            .set_permissions(PermissionsExt::from_mode(0o606))
            .await?;
        Ok((
            StatusCode::ACCEPTED,
            [
                (
                    DOCKER_DISTRIBUTION_API_VERSION,
                    API_VERSION.to_str().unwrap().to_owned(),
                ),
                (
                    header::LOCATION,
                    format!("/v2/{}/blobs/upload/{}", name, upload_uuid_str),
                ),
                (header::CONTENT_LENGTH, 0.to_string()),
                (DOCKER_UPLOAD_UUID, upload_uuid_str),
                (header::RANGE, "0-0".to_owned()),
            ],
        ))
    }

    async fn upload_chunk(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((name, upload_uuid_str)): Path<(String, String)>,
        headers: HeaderMap,
        stream: BodyStream,
    ) -> Response {
        println!("Patch blob: {}", &name);
        match Self::upload_chunk_impl(state, name, upload_uuid_str, headers, stream).await {
            Ok(r) => r.into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", e),
            )
                .into_response(),
        }
    }

    async fn upload_chunk_impl(
        state: Arc<DockerRegistryBackendState>,
        name: String,
        upload_uuid_str: String,
        headers: HeaderMap,
        mut stream: BodyStream,
    ) -> Result<impl IntoResponse> {
        let _upload_uuid = Uuid::from_str(&upload_uuid_str)?;

        let mut file_path = state.root_path.clone();
        file_path.push(&name);
        file_path.push("upload");
        file_path.push(&upload_uuid_str);

        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)
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
                            DOCKER_DISTRIBUTION_API_VERSION,
                            API_VERSION.to_str().unwrap().to_owned(),
                        ),
                        (
                            header::LOCATION,
                            format!("/v2/{}/blobs/upload/{}", name, upload_uuid_str),
                        ),
                        (header::CONTENT_LENGTH, 0.to_string()),
                        (header::RANGE, format!("0-{}", uploaded_file_size - 1)),
                        (DOCKER_UPLOAD_UUID, upload_uuid_str),
                    ],
                ));
            }
        }

        let mut size = 0usize;
        while let Some(chunk) = stream.next().await {
            let mut chunk = chunk?;
            while chunk.has_remaining() {
                size += file.write_buf(&mut chunk).await?;
            }
        }
        println!("patch size {}", &size);
        println!("0-{}", size - 1);
        Ok((
            StatusCode::ACCEPTED,
            [
                (
                    DOCKER_DISTRIBUTION_API_VERSION,
                    API_VERSION.to_str().unwrap().to_owned(),
                ),
                (
                    header::LOCATION,
                    format!("/v2/{}/blobs/upload/{}", name, upload_uuid_str),
                ),
                (header::CONTENT_LENGTH, 0.to_string()),
                (
                    header::RANGE,
                    format!("0-{}", uploaded_file_size + size as u64 - 1),
                ),
                (DOCKER_UPLOAD_UUID, upload_uuid_str),
            ],
        ))
    }

    async fn finalize_upload(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((name, upload_uuid_str)): Path<(String, String)>,
        Query(params): Query<HashMap<String, String>>,
    ) -> Response {
        println!("Put blob:{}", &name);

        match Self::finalize_upload_impl(state, name, upload_uuid_str, params).await {
            Ok(r) => r,
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", e),
            )
                .into_response(),
        }
    }

    async fn finalize_upload_impl(
        state: Arc<DockerRegistryBackendState>,
        name: String,
        upload_uuid_str: String,
        params: HashMap<String, String>,
    ) -> Result<Response> {
        let digest = params.get("digest").ok_or(DockerError::NoDigest)?;
        let mut digest_hash: Sha256 = Default::default();
        if let Some(hash) = digest.strip_prefix("sha256:") {
            hex::decode_to_slice(hash, &mut digest_hash)?;
        } else {
            return Err(DockerError::InvalidDigest.into());
        }

        let mut file_path = state.root_path.clone();
        file_path.push(&name);
        file_path.push("upload");
        file_path.push(&upload_uuid_str);
        tokio::fs::set_permissions(&file_path, PermissionsExt::from_mode(0o400)).await?;

        let hash = hash_file_sha256(&file_path).await?;

        if digest_hash == hash {
            let mut target_path = state.root_path.clone();
            target_path.push(&name);
            target_path.push("blobs");
            target_path.push(digest);
            tokio::fs::rename(file_path, target_path).await?;
            Ok((
                StatusCode::CREATED,
                [
                    (
                        DOCKER_DISTRIBUTION_API_VERSION,
                        API_VERSION.to_str().unwrap().to_owned(),
                    ),
                    (
                        header::LOCATION,
                        format!("/v2/{}/blobs/upload/{}", name, digest),
                    ),
                    (header::CONTENT_LENGTH, 0.to_string()),
                    (DOCKER_CONTENT_DIGEST, digest.clone()),
                ],
            )
                .into_response())
        } else {
            Ok((
                StatusCode::NOT_ACCEPTABLE,
                [
                    (
                        DOCKER_DISTRIBUTION_API_VERSION,
                        API_VERSION.to_str().unwrap().to_owned(),
                    ),
                    (header::CONTENT_LENGTH, 0.to_string()),
                ],
            )
                .into_response())
        }
    }

    async fn check_blob(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((name, reference)): Path<(String, String)>,
    ) -> Response {
        println!("Head blob: {}, {}", &name, &reference);
        match Self::check_blob_impl(state, name, reference).await {
            Ok(r) => r,
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", e),
            )
                .into_response(),
        }
    }

    async fn check_blob_impl(
        state: Arc<DockerRegistryBackendState>,
        name: String,
        reference: String,
    ) -> Result<Response> {
        let mut file_path = state.root_path.clone();
        file_path.push(&name);
        file_path.push("blobs");
        file_path.push(&reference);
        match tokio::fs::metadata(&file_path).await {
            Ok(m) => {
                return Ok((
                    StatusCode::OK,
                    [
                        (
                            DOCKER_DISTRIBUTION_API_VERSION,
                            API_VERSION.to_str().unwrap().to_owned(),
                        ),
                        (header::CONTENT_LENGTH, m.len().to_string()),
                        (DOCKER_CONTENT_DIGEST, reference.parse().unwrap()),
                    ],
                )
                    .into_response());
            }
            Err(_) => {
                return Ok((
                    StatusCode::NOT_FOUND,
                    [(DOCKER_DISTRIBUTION_API_VERSION, API_VERSION)],
                )
                    .into_response());
            }
        }
    }

    async fn put_manifest(
        Extension(state): Extension<Arc<DockerRegistryBackendState>>,
        Path((name, tag)): Path<(String, String)>,
        body: String,
    ) -> impl IntoResponse {
        println!("Put manifest: {}, {}", &name, &tag);
        let mut hasher = sha2::Sha256::new();
        hasher.update(&body);
        let hash = hasher.finalize();
        std::fs::File::create("manifest")
            .unwrap()
            .write_all(body.as_bytes())
            .unwrap();
        let hex_hash = "sha256:".to_owned() + &hex::encode(hash);
        println!("Hash: {}", &hex_hash);
        (
            StatusCode::CREATED,
            [
                (
                    DOCKER_DISTRIBUTION_API_VERSION,
                    API_VERSION.to_str().unwrap().to_owned(),
                ),
                (
                    header::LOCATION,
                    format!("/v2/{}/manifests/{}", name, hex_hash),
                ),
                (header::CONTENT_LENGTH, 0.to_string()),
                (DOCKER_CONTENT_DIGEST, hex_hash),
            ],
        )
    }
}

#[derive(Debug)]
pub enum DockerError {
    RootIsNotDir,
    NoDigest,
    InvalidDigest,
}

impl std::fmt::Display for DockerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for DockerError {}

#[async_trait]
impl Backend for DockerRegistryBackend {
    async fn create_class(&mut self, name: &str, _data: &ArtifactClassData) -> Result<()> {
        let mut dir_path = self.state.root_path.clone();
        dir_path.push(name);
        tokio::fs::DirBuilder::new()
            .mode(0o701)
            .create(&dir_path)
            .await?;
        dir_path.push("upload");
        tokio::fs::DirBuilder::new()
            .mode(0o701)
            .create(&dir_path)
            .await?;
        dir_path.pop();
        dir_path.push("blobs");
        tokio::fs::DirBuilder::new()
            .mode(0o701)
            .create(&dir_path)
            .await?;
        dir_path.pop();
        dir_path.push("manifests");
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
        todo!()
    }

    async fn commit_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Vec<ArtifactItemInfo>> {
        todo!()
    }

    async fn get_artifact(
        &mut self,
        class_name: &str,
        art_type: ArtifactType,
        uuid: Uuid,
    ) -> Result<Url> {
        todo!()
    }
}
