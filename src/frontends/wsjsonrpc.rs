use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::{Future, FutureExt, SinkExt, StreamExt};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::class::{ArtifactClassData, ArtifactType};
use crate::config::frontend::ConfigWSJsonRPC;
use crate::error::Error;
use crate::manager::SharedArtifactManager;
use crate::source::Source;
use crate::tag::Tag;

use super::Frontend;

pub struct WSFrontend {
    _handle: JoinHandle<()>,
}

#[derive(Serialize, Deserialize, Debug)]
enum RpcVersion {
    #[serde(rename = "2.0")]
    V20,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum RpcId {
    Number(i64),
    String(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct RpcRequest {
    jsonrpc: RpcVersion,
    method: String,
    #[serde(default)]
    params: Params,
    id: Option<RpcId>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum RpcResult {
    Result(serde_json::Value),
    Error {
        code: i64,
        message: Option<String>,
        data: Option<serde_json::Value>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct RpcResponce {
    jsonrpc: RpcVersion,
    #[serde(flatten)]
    res: RpcResult,
    id: Option<RpcId>,
}

impl RpcResponce {
    fn new(res: RpcResult, id: Option<RpcId>) -> Self {
        Self {
            jsonrpc: RpcVersion::V20,
            res,
            id,
        }
    }

    fn new_no_id(res: RpcResult) -> Self {
        Self {
            jsonrpc: RpcVersion::V20,
            res,
            id: None,
        }
    }
}

#[repr(i64)]
enum ErrorCode {
    UnknownError = -1,
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    #[allow(dead_code)]
    InternalError = -32603,
    #[allow(dead_code)]
    ServerError = -32000,
}

type Params = serde_json::Value;
type MethodHandlerFutureBox = Pin<Box<dyn Future<Output = RpcResult> + Send>>;
type MethodHandler =
    Box<dyn Fn(Params, SharedArtifactManager) -> MethodHandlerFutureBox + Send + Sync>;

struct Handlers {
    handlers: HashMap<&'static str, MethodHandler>,
}

impl Handlers {
    fn new() -> Self {
        Handlers {
            handlers: HashMap::new(),
        }
    }

    fn insert<Args, Fut, Fun>(&mut self, name: &'static str, handler: Fun)
    where
        Args: for<'de> serde::Deserialize<'de>,
        Fun: Fn(Args, SharedArtifactManager) -> Fut,
        Fun: Send + Sync + 'static,
        Fut: Future<Output = RpcResult> + Send + 'static,
    {
        self.handlers.insert(
            name,
            Box::new(
                move |params: serde_json::Value, manager: SharedArtifactManager| {
                    let args = match serde_json::from_value(params) {
                        Ok(args) => args,
                        Err(_) => {
                            return async {
                                RpcResult::Error {
                                    code: ErrorCode::InvalidParams as i64,
                                    message: Some("Invalid params".to_owned()),
                                    data: None,
                                }
                            }
                            .boxed()
                        }
                    };
                    handler(args, manager).boxed()
                },
            ),
        );
    }

    fn get(&self, name: &str) -> Option<&MethodHandler> {
        self.handlers.get(name)
    }
}

impl WSFrontend {
    pub async fn new(cfg: &ConfigWSJsonRPC, manager: SharedArtifactManager) -> Result<Self, Error> {
        let mut handlers = Handlers::new();
        handlers.insert("create_artifact_class", create_artifact_class);
        handlers.insert("get_classes", get_classes);
        handlers.insert("get_artifacts", get_artifacts);
        handlers.insert("get_sources", get_sources);
        handlers.insert("get_items", get_items);
        handlers.insert("get_tags", get_tags);
        handlers.insert("get_usages", get_usages);
        handlers.insert("reserve_artifact", reserve_artifact);
        handlers.insert("commit_artifact", commit_artifact);
        handlers.insert("abort_reserve", abort_reserve);
        handlers.insert("use_artifact", use_artifact);

        let handlers = Arc::new(handlers);
        let addr = SocketAddr::new(cfg.address, cfg.port);
        let listener = TcpListener::bind(&addr).await?;
        let handle = tokio::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                tokio::spawn(Self::handle_connection(
                    handlers.clone(),
                    manager.clone(),
                    stream,
                    addr,
                ));
            }
        });
        Ok(Self { _handle: handle })
    }

    async fn handle_connection(
        handlers: Arc<Handlers>,
        manager: SharedArtifactManager,
        raw_stream: TcpStream,
        addr: SocketAddr,
    ) {
        info!("Incoming TCP connection from: {}", addr);

        let ws_stream = match tokio_tungstenite::accept_async(raw_stream).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Connection failed: {:?}", e);
                return;
            }
        };
        info!("WebSocket connection established: {}", addr);

        let (mut outgoing, mut incoming) = ws_stream.split();

        while let Some(msg) = incoming.next().await {
            match msg {
                Ok(msg) => {
                    let resp = Self::handle_message(msg, &*handlers, &manager).await;
                    let resp = serde_json::to_string(&resp).unwrap();
                    if let Err(e) = outgoing.send(Message::Text(resp)).await {
                        warn!("Failed to send responce: {:?}", e);
                        return;
                    }
                }
                Err(e) => {
                    warn!("Error recieving message: {:?}", e);
                    return;
                }
            }
        }

        info!("{} disconnected", &addr);
    }

    async fn handle_message(
        msg: Message,
        handlers: &Handlers,
        manager: &SharedArtifactManager,
    ) -> RpcResponce {
        let text: serde_json::Value = match msg
            .to_text()
            .map_err(|e| <Error>::from(e))
            .and_then(|s| serde_json::from_str(s).map_err(|e| e.into()))
        {
            Ok(r) => r,
            Err(_) => {
                return RpcResponce::new_no_id(RpcResult::Error {
                    code: ErrorCode::ParseError as i64,
                    message: Some("Parse error".to_owned()),
                    data: None,
                });
            }
        };
        let request: RpcRequest = match serde_json::from_value(text) {
            Ok(r) => r,
            Err(_) => {
                return RpcResponce::new_no_id(RpcResult::Error {
                    code: ErrorCode::InvalidRequest as i64,
                    message: Some("Invalid request".to_owned()),
                    data: None,
                });
            }
        };

        match handlers.get(request.method.as_str()) {
            Some(h) => RpcResponce::new(h(request.params, manager.clone()).await, request.id),
            None => RpcResponce::new(
                RpcResult::Error {
                    code: ErrorCode::MethodNotFound as i64,
                    message: Some("Method not found".to_owned()),
                    data: None,
                },
                request.id,
            ),
        }
    }
}

#[derive(Deserialize)]
struct CreateArtifactClassArgs {
    name: String,
    backend_name: String,
    art_type: ArtifactType,
}

async fn create_artifact_class(
    CreateArtifactClassArgs {
        name,
        backend_name,
        art_type,
    }: CreateArtifactClassArgs,
    manager: SharedArtifactManager,
) -> RpcResult {
    if manager
        .lock()
        .await
        .create_class(
            name,
            ArtifactClassData {
                backend_name,
                art_type,
            },
        )
        .await
        .is_err()
    {
        RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        }
    } else {
        RpcResult::Result(serde_json::Value::Null)
    }
}

async fn get_classes((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_artifact_classes().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

async fn get_artifacts((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_artifacts_info().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

async fn get_sources((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_sources().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

async fn get_items((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_items().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

async fn get_tags((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_tags().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

async fn get_usages((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_usages().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

#[derive(Deserialize)]
struct ReserveArtifact {
    class_name: String,
    proxy: Option<String>,
    sources: Vec<Source>,
    tags: Vec<Tag>,
}

#[derive(Serialize)]
struct ReserveArtifactRes {
    uuid: Uuid,
    url: String,
}

async fn reserve_artifact(
    ReserveArtifact {
        class_name,
        proxy,
        sources,
        tags,
    }: ReserveArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager
        .lock()
        .await
        .reserve_artifact(class_name, sources, tags, proxy)
        .await
    {
        Ok((uuid, url)) => RpcResult::Result(
            serde_json::to_value(ReserveArtifactRes {
                uuid,
                url: url.to_string(),
            })
            .unwrap(),
        ),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

#[derive(Deserialize)]
struct CommitArtifact {
    uuid: Uuid,
    tags: Vec<Tag>,
}

async fn commit_artifact(
    CommitArtifact { uuid, tags }: CommitArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager
        .lock()
        .await
        .commit_artifact_reserve(uuid, tags)
        .await
    {
        Ok(()) => RpcResult::Result(serde_json::Value::Null),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

#[derive(Deserialize)]
struct AbortReserve {
    uuid: Uuid,
}

async fn abort_reserve(
    AbortReserve { uuid }: AbortReserve,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager.lock().await.abort_artifact_reserve(uuid).await {
        Ok(()) => RpcResult::Result(serde_json::Value::Null),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

#[derive(Deserialize)]
struct GetArtifact {
    uuid: Uuid,
    proxy: Option<String>,
}

#[derive(Serialize)]
struct UseArtifactRes {
    uuid: Uuid,
    url: String,
}

async fn use_artifact(
    GetArtifact { uuid, proxy }: GetArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager.lock().await.use_artifact(uuid, proxy).await {
        Ok((uuid, url)) => RpcResult::Result(
            serde_json::to_value(UseArtifactRes {
                uuid,
                url: url.to_string(),
            })
            .unwrap(),
        ),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

impl Frontend for WSFrontend {}
