// TODO: Split codebase so not whole binary is included, when only api is needed
#![allow(dead_code)]

pub mod api;
mod artifact;
mod backend_pack;
mod backends;
mod class;
mod config;
mod error;
mod frontends;
mod manager;
mod proxies;
mod source;
mod storage;
mod tag;
mod usage;
mod util;

pub use frontends::wsjsonrpc::jsonrpc;
