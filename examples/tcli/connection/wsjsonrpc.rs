use std::net::TcpStream;

use tungstenite::{
    connect,
    protocol::{frame::coding::CloseCode, CloseFrame},
    stream::MaybeTlsStream,
    Error, Message, WebSocket,
};

use taneleer::{api::*, jsonrpc::*};
use url::Url;
use uuid::Uuid;

use super::MethodCall;

pub struct WSJsonRPC {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl WSJsonRPC {
    pub fn new() -> Self {
        let (socket, _) = connect(Url::parse("ws://localhost:5002").unwrap()).unwrap();
        Self { socket }
    }

    fn method_call<Res, Args>(&mut self, method_name: &str, args: Args) -> Res
    where
        Res: for<'de> serde::Deserialize<'de>,
        Args: serde::Serialize,
    {
        let (req, id) = RpcRequest::new_call(method_name, serde_json::to_value(args).unwrap());
        self.socket
            .write_message(Message::Text(serde_json::to_string(&req).unwrap()))
            .unwrap();
        let resp = loop {
            match self.socket.read_message().unwrap() {
                Message::Text(m) => {
                    let res: RpcResponce = serde_json::from_str(&m).unwrap();
                    if let Some(RpcId::Number(res_id)) = res.id {
                        if res_id == id {
                            break res.res;
                        }
                    }
                }
                Message::Binary(_) => panic!("Binary message"),
                Message::Close(_) => panic!("Close message"),
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => (),
            }
        };
        if let RpcResult::Result(r) = resp {
            serde_json::from_value(r).unwrap()
        } else {
            panic!("Error");
        }
    }
}

impl MethodCall for WSJsonRPC {
    fn create_artifact_class(&mut self, args: CreateArtifactClass) {
        self.method_call("create_artifact_class", args)
    }

    fn get_classes(&mut self) -> Vec<ArtifactClass> {
        self.method_call("get_classes", ())
    }

    fn get_artifacts(&mut self) -> Vec<Artifact> {
        self.method_call("get_artifacts", ())
    }

    fn get_sources(&mut self) -> Vec<(Uuid, Source)> {
        self.method_call("get_sources", ())
    }

    fn get_items(&mut self) -> Vec<ArtifactItem> {
        self.method_call("get_items", ())
    }

    fn get_tags(&mut self) -> Vec<ArtifactTag> {
        self.method_call("get_tags", ())
    }

    fn get_usages(&mut self) -> Vec<ArtifactUsage> {
        self.method_call("get_usages", ())
    }

    fn get_artifact(&mut self, args: GetArtifact) -> GetArtifactRes {
        self.method_call("get_artifact", args)
    }

    fn reserve_artifact(&mut self, args: ReserveArtifact) -> ReserveArtifactRes {
        self.method_call("reserve_artifact", args)
    }

    fn commit_artifact(&mut self, args: CommitArtifact) {
        self.method_call("commit_artifact", args)
    }

    fn abort_reserve(&mut self, args: AbortReserve) {
        self.method_call("abort_reserve", args)
    }

    fn use_artifact(&mut self, args: UseArtifact) -> UseArtifactRes {
        self.method_call("use_artifact", args)
    }

    fn find_last_artifact(&mut self, args: FindLastArtifact) -> FindLastArtifactRes {
        self.method_call("find_last_artifact", args)
    }

    fn use_last_artifact(&mut self, args: UseLastArtifact) -> UseLastArtifactRes {
        self.method_call("use_last_artifact", args)
    }

    fn close(&mut self) {
        self.socket
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            }))
            .unwrap();
        loop {
            self.socket.write_pending().unwrap();
            match self.socket.read_message() {
                Ok(_) => (),
                Err(Error::ConnectionClosed) => break,
                Err(e) => panic!("{:?}", e),
            }
        }
    }
}
