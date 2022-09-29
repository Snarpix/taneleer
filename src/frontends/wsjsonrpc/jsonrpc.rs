use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum RpcVersion {
    #[serde(rename = "2.0")]
    V20,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum RpcId {
    Number(i64),
    String(String),
}

pub type Params = serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcRequest {
    pub jsonrpc: RpcVersion,
    pub method: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: Params,
    pub id: Option<RpcId>,
}

impl RpcRequest {
    #[allow(dead_code)]
    pub fn new(method: String, params: Params, id: Option<RpcId>) -> Self {
        Self {
            jsonrpc: RpcVersion::V20,
            method,
            params,
            id,
        }
    }

    #[allow(dead_code)]
    pub fn new_call(method: &str, params: Params) -> (Self, i64) {
        let id = rand::random();
        (
            Self {
                jsonrpc: RpcVersion::V20,
                method: method.to_owned(),
                params,
                id: Some(RpcId::Number(id)),
            },
            id,
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum RpcResult {
    Result(serde_json::Value),
    Error {
        code: i64,
        message: Option<String>,
        data: Option<serde_json::Value>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcResponce {
    pub jsonrpc: RpcVersion,
    #[serde(flatten)]
    pub res: RpcResult,
    pub id: Option<RpcId>,
}

impl RpcResponce {
    pub fn new(res: RpcResult, id: Option<RpcId>) -> Self {
        Self {
            jsonrpc: RpcVersion::V20,
            res,
            id,
        }
    }

    pub fn new_no_id(res: RpcResult) -> Self {
        Self {
            jsonrpc: RpcVersion::V20,
            res,
            id: None,
        }
    }
}

#[repr(i64)]
pub enum ErrorCode {
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
