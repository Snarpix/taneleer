// TODO: Split codebase so not whole binary is included, when only api is needed
#![allow(dead_code)]

pub mod api;
mod artifact;
mod backend_pack;
mod backends;
mod class;
mod config;
mod error;
mod manager;
mod proxies;
mod rpc;
mod source;
mod storage;
mod tag;
mod usage;
mod util;

pub use rpc::wsjsonrpc::jsonrpc;
