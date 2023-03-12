use std::{collections::HashMap, pin::Pin};

use futures::{Future, FutureExt};

use crate::manager::SharedArtifactManager;

use super::jsonrpc::{ErrorCode, Params, RpcResult};

type MethodHandlerFutureBox = Pin<Box<dyn Future<Output = RpcResult> + Send>>;
type MethodHandler =
    Box<dyn Fn(Params, SharedArtifactManager) -> MethodHandlerFutureBox + Send + Sync>;

pub struct Handlers {
    handlers: HashMap<&'static str, MethodHandler>,
}

impl Handlers {
    pub fn new() -> Self {
        Handlers {
            handlers: HashMap::new(),
        }
    }

    pub fn insert<Args, Fut, Fun>(&mut self, name: &'static str, handler: Fun)
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

    pub fn get(&self, name: &str) -> Option<&MethodHandler> {
        self.handlers.get(name)
    }
}
