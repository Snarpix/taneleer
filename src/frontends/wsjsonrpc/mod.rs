pub mod api;
mod handlers;
pub mod jsonrpc;
mod methods;

use std::{net::SocketAddr, sync::Arc};

use futures::{SinkExt, StreamExt};
use log::{trace, warn};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

use crate::{config::frontend::ConfigWSJsonRPC, error::Error, manager::SharedArtifactManager};

use self::{
    handlers::Handlers,
    jsonrpc::{ErrorCode, RpcRequest, RpcResponce, RpcResult},
    methods::*,
};

use super::Frontend;

pub struct WSFrontend {
    _handle: JoinHandle<()>,
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
        handlers.insert("get_artifact", get_artifact);
        handlers.insert("find_last_artifact", find_last_artifact);
        handlers.insert("reserve_artifact", reserve_artifact);
        handlers.insert("commit_artifact", commit_artifact);
        handlers.insert("abort_reserve", abort_reserve);
        handlers.insert("use_artifact", use_artifact);
        handlers.insert("use_last_artifact", use_last_artifact);

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
        trace!("Incoming TCP connection from: {}", addr);

        let ws_stream = match tokio_tungstenite::accept_async(raw_stream).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Connection failed: {:?}", e);
                return;
            }
        };
        trace!("WebSocket connection established: {}", addr);

        let (mut outgoing, mut incoming) = ws_stream.split();

        while let Some(msg) = incoming.next().await {
            match msg {
                Ok(Message::Text(msg)) => {
                    let resp = Self::handle_message(msg, &*handlers, &manager).await;
                    let resp = serde_json::to_string(&resp).unwrap();
                    if let Err(e) = outgoing.send(Message::Text(resp)).await {
                        warn!("Failed to send responce: {:?}", e);
                        return;
                    }
                }
                Ok(_) => (),
                Err(e) => {
                    warn!("Error recieving message: {:?}", e);
                    return;
                }
            }
        }

        trace!("{} disconnected", &addr);
    }

    async fn handle_message(
        text: String,
        handlers: &Handlers,
        manager: &SharedArtifactManager,
    ) -> RpcResponce {
        let text = match serde_json::from_str(&text) {
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

impl Frontend for WSFrontend {}
