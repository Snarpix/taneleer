use crate::source::{Sha256, Hashsum};

pub struct ManifestInfo {
    pub hash: Hashsum,
    pub len: u64,
    pub cnt_type: String,
}